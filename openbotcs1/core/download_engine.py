# openbot\core\download_engine.py
"""
【core 下载引擎】全项目唯一的下载执行层（插件只依赖本模块，不互相依赖）。

边界：
  - 本模块只做"怎么下载"：网络状态管理、单文件原子下载（断点/去重/暂停中断）、
    批量调度（分批 + 全局限流 6 并发 + 失败自动重试）、停机清理。
  - "任务编排"（run_job 的 扫描→下载 流程、saved/watch 特殊分支、启动恢复、
    命令/监听）是插件业务，留在 features/downloader/（它从本模块 import 引擎能力）。
  - 所有数据库读写走 core.database；所有消息遍历底层走 core.media_scanner。

热重载保护：模块级可变状态（job_runtime/semaphore/active_*/network_event）
用 globals() 检查保留。core 模块不会被热重载，状态天然跨插件重载存活。
"""
import asyncio
import logging
import os
import re
import time

from telethon import types

from core.database import (
    DOWNLOAD_DIR, JOB_CANCELLED, JOB_DONE, JOB_PAUSED, JOB_WAIT_DL,
    TASK_DONE, TASK_DOWNLOADING, TASK_FAILED, TASK_PENDING, TASK_SKIPPED, _db_conn,
    _extract_media_id, _persist_progress, get_job, get_job_stats,
    get_pending_tasks, update_job_status, update_task_document_id,
    update_task_status,
)

logger = logging.getLogger(__name__)

# ===================== 引擎运行时状态（热重载保留） =====================
# 实时速度统计（内存，不持久化）
if "job_runtime" not in globals():
    job_runtime = {}  # jid -> {"speed": float, "current_file": str, "downloaded": int, "start_time": float}

# 下载被暂停的自定义异常（用于中断正在下载的文件）
class DownloadPausedError(Exception):
    pass

# 全局下载并发限制：同时最多 6 个下载
if "task_semaphore" not in globals():
    task_semaphore = asyncio.Semaphore(6)
    # 正在运行的下载任务集合（用于停机时取消）
    active_download_tasks = set()
    # 正在等待的恢复任务集合（用于停机时取消）
    active_resume_tasks = set()
    # 正在运行的 run_job 任务集合（用于停机时取消）
    active_run_jobs = set()

# Windows 文件名非法字符
INVALID_FILENAME_CHARS = r'[\\/:*?"<>|\r\n\t]'

# 全局网络状态事件：set=正常，clear=断开
if "network_event" not in globals():
    network_event = asyncio.Event()
    network_event.set()  # 默认网络正常
    _network_monitor_started = False
    _network_monitor_task = None  # 网络监控任务引用，用于停机时取消

# 收藏夹监听用户 ID（跨插件共享状态）：
# 由 mtproto 登录插件在 /mtlogin 成功后刷新，downloader 插件的收藏夹监听读取。
# 放 core 避免插件互相 import（插件只依赖 core）。
if "SAVED_USER_ID" not in globals():
    SAVED_USER_ID = None


# ===================== 网络状态管理 =====================
def _is_network_error(e: Exception) -> bool:
    """判断异常是否为网络错误（断网、超时、连接断开等）"""
    # 常见网络异常类型
    if isinstance(e, (ConnectionError, TimeoutError, asyncio.TimeoutError, OSError)):
        return True
    # Telethon 相关错误：检查异常类名或消息
    err_name = type(e).__name__
    err_msg = str(e).lower()
    network_keywords = [
        "connection", "timeout", "timed out", "network", "disconnect",
        "closed the connection", "getfile", "readexactly", "floodwait",
        "0 bytes read", "winerror", "10054", "10053",
    ]
    if any(kw in err_name.lower() for kw in network_keywords):
        return True
    if any(kw in err_msg for kw in network_keywords):
        return True
    return False


def on_network_error():
    """标记网络断开（由下载失败时调用）"""
    if network_event.is_set():
        network_event.clear()
        logger.warning("⚠️ 网络断开，下载等待恢复...")


async def _wait_for_network(jid: int) -> bool:
    """等待网络恢复，同时检查任务暂停状态。返回 True=网络恢复，False=任务被暂停"""
    if network_event.is_set():
        return True
    logger.info(f"⏳ 任务 #{jid} 等待网络恢复...")
    while not network_event.is_set():
        # 检查暂停/取消（不和暂停继续冲突）
        current_job = get_job(jid)
        if not current_job or current_job["status"] in (JOB_PAUSED, JOB_CANCELLED):
            logger.info(f"⏸ 任务 #{jid} 等待网络恢复期间被暂停，退出")
            return False
        await asyncio.sleep(3)
    logger.info(f"✅ 网络已恢复，任务 #{jid} 继续下载")
    return True


async def _network_monitor(manager):
    """后台网络监控：每5秒主动检测 MTProto 连通性，标记断网/恢复"""
    global network_event
    while True:
        try:
            client = manager.mtproto_client.client
            if client:
                await client.get_me()
                # 连通成功
                if not network_event.is_set():
                    network_event.set()
                    logger.info("✅ 网络已恢复，MTProto 已连接")
            else:
                # MTProto 客户端不存在，标记断网
                if network_event.is_set():
                    network_event.clear()
                    logger.warning("⚠️ 网络断开（MTProto 未就绪），下载等待恢复...")
        except Exception:
            # 连通失败，标记断网
            if network_event.is_set():
                network_event.clear()
                logger.warning("⚠️ 网络断开，下载等待恢复...")


# ===================== 核心下载原子操作 =====================
def _is_video_document(document):
    if not document: return False
    mime = getattr(document, 'mime_type', '')
    if mime and mime.startswith('video/'): return True
    for attr in getattr(document, 'attributes', []):
        if isinstance(attr, types.DocumentAttributeVideo): return True
    return False


async def _download_single_file(client, jid, tid, msg_id, chat_id, chat_name):
    """下载单个文件，更新数据库状态和速度统计（全局并发限制6个）"""
    # 追踪当前任务到全局集合（用于停机时取消）
    current_task = asyncio.current_task()
    if current_task:
        active_download_tasks.add(current_task)
    try:
        async with task_semaphore:
            # 获取信号量后再次检查暂停/取消（防止等待期间被暂停）
            current_job = get_job(jid)
            if not current_job or current_job["status"] in (JOB_PAUSED, JOB_CANCELLED):
                return False

            while True:  # 网络错误重试循环：断网时等待恢复后重试，不标记失败
                try:
                    m = await client.get_messages(chat_id, ids=msg_id)
                    if not m or not m.media:
                        # v1.2：源消息已删除/无媒体（空壳）→ 标记"已删除"而非"完成"
                        update_task_status(tid, TASK_SKIPPED)
                        return True
                    # 提取媒体全局唯一ID（document.id / photo.id，用于下载成功后写库做全局去重）
                    media_id = _extract_media_id(m.media)

                    # 构造保存路径
                    source_id = str(chat_id)
                    safe_chat_name = re.sub(INVALID_FILENAME_CHARS, '_', str(chat_name))
                    save_dir = os.path.join(DOWNLOAD_DIR, source_id, safe_chat_name)
                    os.makedirs(save_dir, exist_ok=True)

                    # 文件名（过滤非法字符）
                    fname = f"{msg_id}"
                    if hasattr(m.media, 'document') and m.media.document:
                        for attr in m.media.document.attributes:
                            if isinstance(attr, types.DocumentAttributeFilename):
                                fname = attr.file_name
                    fname = re.sub(INVALID_FILENAME_CHARS, '_', fname)
                    # 限制文件名长度（Windows 路径总长限制）
                    if len(fname) > 200:
                        name, ext = os.path.splitext(fname)
                        fname = name[:190] + ext
                    if "." not in fname:
                        if hasattr(m.media, 'document') and _is_video_document(m.media.document):
                            fname += ".mp4"
                        elif hasattr(m.media, 'photo') and m.media.photo:
                            fname += ".jpg"
                        else:
                            fname += ".file"

                    fpath = os.path.join(save_dir, fname)

                    # 获取预期文件大小（用于同名文件去重判断）
                    expected_size = 0
                    if hasattr(m.media, 'document') and m.media.document:
                        expected_size = m.media.document.size or 0

                    # 已存在则检查大小：同名同大小跳过，同名不同大小重命名
                    if os.path.exists(fpath):
                        existing_size = os.path.getsize(fpath)
                        if existing_size == expected_size and expected_size > 0:
                            # 同名同大小，是同一个文件，跳过
                            update_task_document_id(tid, media_id)  # 记录媒体ID（供全局去重）
                            update_task_status(tid, TASK_DONE)
                            return True
                        else:
                            # 同名不同大小，是不同文件，重命名保存
                            base, ext = os.path.splitext(fname)
                            counter = 1
                            while os.path.exists(fpath):
                                fname = f"{base}({counter}){ext}"
                                fpath = os.path.join(save_dir, fname)
                                counter += 1

                    # 速度统计初始化
                    if jid not in job_runtime:
                        job_runtime[jid] = {"speed": 0, "current_file": fname, "downloaded": 0, "start_time": time.time(), "last_bytes": 0, "last_time": time.time()}
                    job_runtime[jid]["current_file"] = fname
                    job_runtime[jid]["downloaded"] = 0
                    job_runtime[jid]["start_time"] = time.time()
                    job_runtime[jid]["last_bytes"] = 0
                    job_runtime[jid]["last_time"] = time.time()

                    update_task_status(tid, TASK_DOWNLOADING)

                    # 下载回调：更新速度统计
                    # 【性能修复】DB 落库频率从 0.5s 降到 3s，且写入提交到线程池执行，
                    # 彻底避免大文件下载时 sqlite 同步 IO 阻塞事件循环导致命令无响应
                    last_db_write = 0.0
                    def progress_cb(current, total):
                        nonlocal last_db_write
                        now = time.time()
                        if now - last_db_write >= 3.0:
                            last_db_write = now
                            # 【优先】检查暂停/取消（单条 SELECT 很快，保持同步以能中断下载）
                            current_job = get_job(jid)
                            if not current_job or current_job["status"] in (JOB_PAUSED, JOB_CANCELLED):
                                raise DownloadPausedError("下载被暂停/取消")
                            # 进度落库：提交到线程池执行，不阻塞事件循环
                            try:
                                asyncio.get_running_loop().run_in_executor(
                                    None, _persist_progress, tid, current)
                            except Exception:
                                pass

                        # 速度统计（纯内存，不碰 DB）
                        if jid not in job_runtime: return
                        rt = job_runtime[jid]
                        elapsed = now - rt["last_time"]
                        if elapsed >= 1.0:
                            rt["speed"] = (current - rt["last_bytes"]) / elapsed if elapsed > 0 else 0
                            rt["last_bytes"] = current
                            rt["last_time"] = now
                        rt["downloaded"] = current

                    # 原子下载
                    temp_path = fpath + ".temp"
                    await client.download_media(m, file=temp_path, progress_callback=progress_cb)

                    if os.path.exists(temp_path):
                        os.rename(temp_path, fpath)
                        update_task_document_id(tid, media_id)  # 记录媒体ID（供全局去重）
                        update_task_status(tid, TASK_DONE)
                        return True
                    else:
                        update_task_status(tid, TASK_FAILED)
                        return False

                except DownloadPausedError:
                    # 被暂停/取消中断，重置为待下载，下次继续
                    logger.debug(f"⏸ 下载被暂停 tid={tid}，重置为待下载")
                    update_task_status(tid, TASK_PENDING)
                    return False
                except Exception as e:
                    if _is_network_error(e):
                        # 网络错误：标记断网，等待恢复后重试，不标记失败
                        on_network_error()
                        logger.info(f"⏳ 下载遇网络错误 tid={tid}，等待网络恢复...")
                        if not await _wait_for_network(jid):
                            # 等待期间被暂停/取消，重置为待下载，不标记失败
                            update_task_status(tid, TASK_PENDING)
                            return False
                        # 网络恢复，重试下载（continue while 循环）
                        continue
                    else:
                        # 非网络错误，标记失败
                        logger.error(f"下载失败 tid={tid}: {e}")
                        update_task_status(tid, TASK_FAILED)
                        return False
    finally:
        if current_task:
            active_download_tasks.discard(current_task)


async def _download_pending_tasks(manager, jid):
    """下载任务中所有待下载的文件（失败自动重试1次）
    注意：本函数不负责设置任务状态为下载中，由调用者负责。
    下载完成后：普通任务→JOB_DONE，收藏夹任务→保持JOB_DOWNLOADING（监听模式）
    """
    job = get_job(jid)
    if not job: return

    # 开头就检查：如果已经被暂停/取消，直接返回，不开始任何下载
    if job["status"] in (JOB_PAUSED, JOB_CANCELLED):
        logger.info(f"⏸ 任务 #{jid} 已暂停/取消，跳过下载")
        return

    client = manager.mtproto_client.client
    bot = manager.bot_app.bot
    user_chat_id = job["user_chat_id"]
    is_saved = (job["type"] in ("saved", "watch"))

    async def _dl_one(task):
        # 每次下载前检查暂停/取消（双保险）
        current_job = get_job(jid)
        if not current_job or current_job["status"] in (JOB_PAUSED, JOB_CANCELLED):
            return
        await _download_single_file(client, jid, task["tid"], task["msg_id"], task["chat_id"], task["chat_name"])

    async def _dl_batches(items, batch_size=50):
        """分批调度下载：避免海量待下载文件一次性创建上万个协程（配合全局限流6并发），
        每批结束后检查暂停/取消，避免无谓调度剩余批次"""
        for i in range(0, len(items), batch_size):
            await asyncio.gather(*(_dl_one(t) for t in items[i:i + batch_size]))
            cur = get_job(jid)
            if not cur or cur["status"] in (JOB_PAUSED, JOB_CANCELLED):
                return

    # 第一轮下载
    pending = get_pending_tasks(jid)
    if not pending:
        if not is_saved:
            update_job_status(jid, JOB_DONE)
            await bot.send_message(user_chat_id, f"✅ 任务 #{jid} 已完成（无待下载文件）")
        else:
            logger.info(f"📡 任务 #{jid} 无待下载文件，进入持续监听模式")
        return

    stats = get_job_stats(jid)
    logger.info(f"📥 任务 #{jid} 开始下载 | 总计 {stats['total']} | 已完成 {stats['done']} | 待下载 {stats['pending']} | 失败 {stats['failed']}")
    await _dl_batches(pending)

    # 检查是否被暂停/取消
    current_job = get_job(jid)
    if not current_job or current_job["status"] in (JOB_PAUSED, JOB_CANCELLED):
        logger.info(f"⏸ 任务 #{jid} 已暂停，停止下载")
        return

    # 失败自动重试1次：把失败的文件重置为待下载
    conn = _db_conn()
    failed_count = conn.execute("SELECT COUNT(*) FROM tasks WHERE jid=? AND status=?", (jid, TASK_FAILED)).fetchone()[0]
    if failed_count > 0:
        logger.info(f"📥 任务 #{jid} 第一轮下载完成，{failed_count} 个失败，自动重试1次")
        conn.execute("UPDATE tasks SET status=? WHERE jid=? AND status=?", (TASK_PENDING, jid, TASK_FAILED))
        conn.commit()
    conn.close()

    # 第二轮下载（重试失败的）
    if failed_count > 0:
        retry_pending = get_pending_tasks(jid)
        if retry_pending:
            await _dl_batches(retry_pending)

    # 最终检查
    final_job = get_job(jid)
    if final_job and final_job["status"] not in (JOB_PAUSED, JOB_CANCELLED):
        remaining = get_pending_tasks(jid)
        stats = get_job_stats(jid)
        if not remaining:
            if is_saved:
                # 收藏夹任务：下载完成但保持下载中状态（持续监听新消息）
                logger.info(f"📡 任务 #{jid} 历史文件下载完成，进入持续监听模式")
            else:
                update_job_status(jid, JOB_DONE)
                await bot.send_message(user_chat_id, f"✅ 任务 #{jid} 下载完成\n总计: {stats['total']} | 成功: {stats['done']} | 失败: {stats['failed']}")
        else:
            update_job_status(jid, JOB_WAIT_DL)
            await bot.send_message(user_chat_id, f"⚠️ 任务 #{jid} 下载完成但有失败\n总计: {stats['total']} | 成功: {stats['done']} | 失败: {stats['failed']}\n用 /dl_continue #{jid} 重新下载失败的文件")


async def shutdown_downloads():
    """停机时取消所有下载任务、恢复任务、run_job任务和网络监控，等待它们结束（最多2秒）"""
    global _network_monitor_task
    tasks_to_cancel = list(active_download_tasks) + list(active_resume_tasks) + list(active_run_jobs)
    if _network_monitor_task:
        tasks_to_cancel.append(_network_monitor_task)
        _network_monitor_task = None

    if not tasks_to_cancel:
        return
    logger.info(f"🛑 取消 {len(tasks_to_cancel)} 个后台任务（下载+恢复+run_job+网络监控）...")
    for task in tasks_to_cancel:
        task.cancel()
    try:
        await asyncio.wait_for(
            asyncio.gather(*tasks_to_cancel, return_exceptions=True),
            timeout=2.0
        )
        logger.info("✅ 所有后台任务已取消")
    except asyncio.TimeoutError:
        logger.warning("⚠️ 取消超时，强制退出")
