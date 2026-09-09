# openbot\features\look\group_browse.py
"""
群文件查询 + Web 请求中转（配合 7777 Web 查看器的"群文件查询"视图）。

架构（插件自包含，只依赖 core，不依赖其它插件）：
  web（独立进程，无 MTProto）不直接连 Telegram，通过 web_requests 表向 bot 发请求：
    - type='scan'    : look 插件自己扫描指定群（服务器媒体过滤 + 提取文件清单写 group_files 表）
    - type='download': 只负责把下载请求"推"进 web_requests 表，
                       由 downloader 插件（features/downloader/mt_downloader.py 的消费 worker）执行下载
  扫描串行化：同一时刻只允许一个扫描在跑（_scan_busy），
  多个扫描并发会共用同一个 Telethon client 连接 → readexactly 冲突/静默空结果。

底层全部来自 core（全项目唯一实现）：
  core.database       DB_PATH / _db_conn / _extract_media_id
  core.media_scanner  消息遍历 + 媒体提取（iter_media_messages / extract_media_info / FILTER_FILES / FILTER_PHOTOS）

表：
  group_files  : 群文件索引缓存（chat_id+msg_id 唯一，扫描后复用，避免重复拉取）
  web_requests : web → bot 请求队列（0待处理 1处理中 2完成 3失败）
"""
import asyncio
import logging
import sqlite3
from datetime import datetime

from features.look.db import _db_conn
from core import media_cache
from core.database import _extract_media_id
from core.media_scanner import (
    FILTER_FILES, FILTER_PHOTOS, count_media_by_category, count_media_messages,
    extract_media_info, iter_media_messages,
)
from features.look.dialog_sync import sync_dialogs

logger = logging.getLogger(__name__)

# web_requests 状态
REQ_PENDING = 0
REQ_RUNNING = 1
REQ_DONE = 2
REQ_FAILED = 3

# 扫描串行化：同一时刻只允许一个 iter_messages 扫描在跑，
# 多个扫描并发会共用同一个 Telethon client 连接 → readexactly 冲突/静默空结果
_scan_busy = False
_RUNNING_TIMEOUT_SEC = 600  # RUNNING 超过 10 分钟视为卡死（bot 重启/崩溃残留），自动转失败


def _now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _ensure_tables():
    """确保 web_requests 表存在（幂等）。group_files 已迁移到 core/media_cache 共享缓存。"""
    conn = _db_conn()
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS web_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT,
            chat_id INTEGER,
            chat_name TEXT,
            msg_id INTEGER,
            file_name TEXT,
            document_id TEXT,
            file_size INTEGER DEFAULT 0,
            status INTEGER DEFAULT 0,
            result TEXT,
            created_at TEXT,
            updated_at TEXT
        )""")
        conn.commit()
    finally:
        conn.close()


# ===================== 请求写入（web 侧调用） =====================
def add_scan_request(chat_id, chat_name, mode="full"):
    """写入群扫描请求；同一群已有待处理/处理中请求则复用，不重复排队。
    mode: 'full'（重新扫描，type='scan'）/ 'incremental'（增量扫描，type='scan_inc'）"""
    _ensure_tables()
    req_type = "scan_inc" if mode == "incremental" else "scan"
    conn = _db_conn()
    try:
        row = conn.execute(
            f"SELECT id, status FROM web_requests WHERE type='{req_type}' AND chat_id=? AND status IN (0,1) ORDER BY id DESC LIMIT 1",
            (chat_id,),
        ).fetchone()
        if row:
            return row[0], row[1]
        now = _now()
        cur = conn.execute(
            "INSERT INTO web_requests (type, chat_id, chat_name, status, created_at, updated_at) VALUES (?, ?, ?, 0, ?, ?)",
            (req_type, chat_id, chat_name or "", now, now),
        )
        conn.commit()
        return cur.lastrowid, REQ_PENDING
    finally:
        conn.close()


def add_sync_request():
    """写入对话列表同步请求（7777 点"刷新频道列表"时调用，bot 消费执行 sync_dialogs）。
    已有待处理/处理中的同步请求则复用，不重复排队。"""
    _ensure_tables()
    conn = _db_conn()
    try:
        row = conn.execute(
            "SELECT id, status FROM web_requests WHERE type='sync_dialogs' AND status IN (0,1) ORDER BY id DESC LIMIT 1",
        ).fetchone()
        if row:
            return row[0], row[1]
        now = _now()
        cur = conn.execute(
            "INSERT INTO web_requests (type, chat_id, chat_name, status, created_at, updated_at) VALUES ('sync_dialogs', 0, '对话列表同步', 0, ?, ?)",
            (now, now),
        )
        conn.commit()
        return cur.lastrowid, REQ_PENDING
    finally:
        conn.close()


async def _run_sync(manager, req_id):
    """执行对话列表同步（bot 消费），完成后清空旧统计并重新获取所有群的媒体统计"""
    try:
        n = await sync_dialogs(manager)
        # 清空旧统计：强制所有群重新获取 count 聚合值（core 共享库，删 download/ 可全清）
        media_cache.clear_group_stats()
        # 为所有对话排队 get_count（bot 逐个消费，用户无需手动点）
        queued = _queue_missing_stats()
        update_request(req_id, REQ_DONE, f"已同步 {n} 个对话，已排队 {queued} 个群统计获取")
        logger.info(f"✅ 对话列表同步完成：{n} 个，排队统计 {queued} 个")
    except Exception as e:
        update_request(req_id, REQ_FAILED, f"同步失败: {e}")
        logger.error(f"❌ 对话列表同步失败: {e}")


def _queue_missing_stats():
    """为所有没有 group_stats 的对话排队 get_count 请求，返回排队数量"""
    conn = _db_conn()
    try:
        # 查出所有对话（group_stats 在 core 的 media_cache.db，不在 look.db）
        rows = conn.execute("SELECT chat_id, chat_name FROM dialogs").fetchall()
    finally:
        conn.close()
    have_stats = set(media_cache.get_all_group_stats().keys())
    rows = [r for r in rows if r[0] not in have_stats]
    queued = 0
    now = _now()
    conn = _db_conn()
    try:
        for chat_id, chat_name in rows:
            # 跳过已有待处理/处理中的请求
            existing = conn.execute(
                "SELECT id FROM web_requests WHERE type='get_count' AND chat_id=? AND status IN (0,1) LIMIT 1",
                (chat_id,),
            ).fetchone()
            if existing:
                continue
            conn.execute(
                "INSERT INTO web_requests (type, chat_id, chat_name, status, created_at, updated_at) VALUES ('get_count', ?, ?, 0, ?, ?)",
                (chat_id, chat_name or "", now, now),
            )
            queued += 1
        conn.commit()
    finally:
        conn.close()
    return queued


def add_count_request(chat_id, chat_name):
    """写入群媒体分类统计请求（bot 调用 Telegram count 接口，扫描前即可显示数量）"""
    _ensure_tables()
    conn = _db_conn()
    try:
        # 已有待处理/处理中的同群请求 → 复用
        row = conn.execute(
            "SELECT id, status FROM web_requests WHERE type='get_count' AND chat_id=? AND status IN (0,1) ORDER BY id DESC LIMIT 1",
            (chat_id,),
        ).fetchone()
        if row:
            return row[0], row[1]
        now = _now()
        cur = conn.execute(
            "INSERT INTO web_requests (type, chat_id, chat_name, status, created_at, updated_at) VALUES ('get_count', ?, ?, 0, ?, ?)",
            (chat_id, chat_name or "", now, now),
        )
        conn.commit()
        return cur.lastrowid, REQ_PENDING
    finally:
        conn.close()


async def _run_count(manager, req_id, chat_id, chat_name):
    """执行群媒体分类统计（4次 count 请求，不遍历消息），结果写入 group_stats 表"""
    try:
        if not manager or not manager.mtproto_client or not manager.mtproto_client.client:
            update_request(req_id, REQ_FAILED, "MTProto 未就绪")
            return
        if not await manager.mtproto_client.ensure_ready():
            update_request(req_id, REQ_FAILED, "MTProto 未就绪（未登录或连接失败），请先 /mtlogin")
            return
        client = manager.mtproto_client.client
        entity = await client.get_entity(chat_id)
        stats = await count_media_by_category(client, entity)
        # 写入 core 共享统计库（media_cache.db 的 group_stats）
        media_cache.upsert_group_stats(chat_id, stats["photo"], stats["video"], stats["audio"], stats["file"])
        total = stats["photo"] + stats["video"] + stats["audio"] + stats["file"]
        update_request(req_id, REQ_DONE, f"图片{stats['photo']} 视频{stats['video']} 音频{stats['audio']} 文件{stats['file']}（共{total}）")
        logger.info(f"📊 群统计完成 chat_id={chat_id}: 图{stats['photo']} 视{stats['video']} 音{stats['audio']} 文{stats['file']}")
    except Exception as e:
        update_request(req_id, REQ_FAILED, f"统计失败: {e}")
        logger.error(f"❌ 群统计失败 chat_id={chat_id}: {e}")


def add_download_request(chat_id, chat_name, msg_id, file_name, document_id, file_size):
    """写入文件下载请求"""
    _ensure_tables()
    conn = _db_conn()
    try:
        # 同一文件已在下载队列 → 复用
        row = conn.execute(
            "SELECT id, status FROM web_requests WHERE type='download' AND chat_id=? AND msg_id=? AND status IN (0,1) ORDER BY id DESC LIMIT 1",
            (chat_id, msg_id),
        ).fetchone()
        if row:
            return row[0], row[1]
        now = _now()
        cur = conn.execute(
            "INSERT INTO web_requests (type, chat_id, chat_name, msg_id, file_name, document_id, file_size, status, created_at, updated_at) VALUES ('download', ?, ?, ?, ?, ?, ?, 0, ?, ?)",
            (chat_id, chat_name or "", msg_id, file_name or "", document_id or "", file_size or 0, now, now),
        )
        conn.commit()
        return cur.lastrowid, REQ_PENDING
    finally:
        conn.close()


def update_request(req_id, status, result=""):
    conn = _db_conn()
    try:
        conn.execute(
            "UPDATE web_requests SET status=?, result=?, updated_at=? WHERE id=?",
            (status, result, _now(), req_id),
        )
        conn.commit()
    finally:
        conn.close()


def get_request(req_id):
    conn = _db_conn()
    conn.row_factory = sqlite3.Row
    try:
        return conn.execute("SELECT * FROM web_requests WHERE id=?", (req_id,)).fetchone()
    finally:
        conn.close()


# ===================== 群文件扫描（bot 侧执行，调用 core 扫描引擎） =====================
async def scan_group_files(manager, chat_id, chat_name, req_id=None, mode="full"):
    """扫描群消息，提取文件清单写入 core/media_cache 共享缓存（返回文件数）。

    mode: "full"=清空旧缓存全量重扫；"incremental"=从上次最大msg_id继续。
    实际扫描逻辑在 core/media_scanner.scan_chat_to_cache，look 只负责进度回调。
    """
    if not manager or not manager.mtproto_client or not manager.mtproto_client.client:
        raise RuntimeError("MTProto 未就绪")
    if not await manager.mtproto_client.ensure_ready():
        raise RuntimeError("MTProto 未就绪（未登录或连接失败），请先 /mtlogin")
    client = manager.mtproto_client.client

    # 进度回调：每 50 条更新 web_requests.result（web 端实时显示）
    def _on_progress(scanned, total, identified):
        if not req_id:
            return
        try:
            conn = _db_conn()
            try:
                # 统一统计：进度按“已处理”算（拉到的消息要么识别、要么空壳跳过），
                # 识别到 + 不可用 = 服务器总数，进度 100% 收尾，数字全部对得上。
                processed = min(scanned, total) if total else scanned      # 显示分子（封顶服务器总数）
                pct = round(processed * 100 / total) if total else 0       # 百分比
                unavailable = max(0, scanned - identified)                 # 空壳不可用条数
                if total:
                    result = f"已处理 {processed} / {total} 个文件 ({pct}%)，识别到 {identified} 个文件，不可用 {unavailable} 条"
                else:
                    result = f"已识别 {identified} 个文件"
                conn.execute(
                    "UPDATE web_requests SET result=?, updated_at=? WHERE id=?",
                    (result, _now(), req_id),
                )
                conn.commit()
            finally:
                conn.close()
        except Exception:
            pass

    from core.media_scanner import scan_chat_to_cache
    return await scan_chat_to_cache(
        client, chat_id, chat_name,
        mode=mode, source="群文件查询",
        # 💡 分遍扫描（物理完整）：无过滤全遍历对超大对话会漏消息（Telegram 搜索索引不全），
        #    改为按 图片/视频/GIF/音频/语音/圆视频/文档 分类分遍扫，主键去重，一条不漏。
        scan_style="filtered",
        progress_callback=_on_progress,
    )


# ===================== 文件下载 =====================
# 💡 look 插件不执行下载：/api/download 只把请求写入 web_requests（type='download'），
#    由 downloader 插件（features/downloader/mt_downloader.py 的消费 worker）执行。


# ===================== 请求处理循环（bot 侧 JobQueue 每 5 秒，look 只处理 scan） =====================
def _recover_stale_requests(force=False):
    """RUNNING 超过超时阈值（10 分钟）的请求视为卡死（bot 重启/崩溃残留），转失败。
    force=True：无条件清理所有 RUNNING（bot 刚启动时调用——进程重启后残留的
    RUNNING 请求必然已中断，立即转失败，避免同群新扫描请求被去重逻辑死锁）。"""
    try:
        from datetime import timedelta
        if force:
            cutoff = None
        else:
            cutoff = (datetime.now() - timedelta(seconds=_RUNNING_TIMEOUT_SEC)).strftime("%Y-%m-%d %H:%M:%S")
        conn = _db_conn()
        try:
            if cutoff is None:
                conn.execute(
                    "UPDATE web_requests SET status=?, result=?, updated_at=? "
                    "WHERE status=?",
                    (REQ_FAILED, "扫描中断（进程重启残留），请重新发起",
                     _now(), REQ_RUNNING),
                )
            else:
                conn.execute(
                    "UPDATE web_requests SET status=?, result=?, updated_at=? "
                    "WHERE status=? AND updated_at < ?",
                    (REQ_FAILED, "超时（处理超过 10 分钟，可能因 Bot 重启中断），请重新发起",
                     _now(), REQ_RUNNING, cutoff),
                )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        pass


async def process_web_requests(manager):
    """轮询 web_requests，处理待处理请求（look 只处理 scan）。
    串行约束：扫描（iter_messages 流式拉取）同一时刻只允许一个在跑，
    多个扫描并发会共用同一个 Telethon client 连接导致冲突/静默空结果。
    💡 type='download' 请求跳过不处理——下载由 downloader 插件消费执行。"""
    global _scan_busy
    try:
        _ensure_tables()
        _recover_stale_requests()
        conn = _db_conn()
        try:
            rows = conn.execute(
                "SELECT * FROM web_requests WHERE status=? ORDER BY id ASC LIMIT 10",
                (REQ_PENDING,),
            ).fetchall()
        finally:
            conn.close()

        for r in rows:
            req_id, rtype = r[0], r[1]
            # 下载请求：留给 downloader 插件消费（look 只做 web 交互 + 扫描）
            if rtype == "download":
                continue
            # 扫描忙：不领取新的扫描请求（留在 PENDING，下轮再试）
            if rtype in ("scan", "scan_inc") and _scan_busy:
                continue
            update_request(req_id, REQ_RUNNING, "处理中")
            if rtype == "scan":
                _scan_busy = True
                asyncio.create_task(_run_scan(manager, req_id, r[2], r[3], mode="full"))
            elif rtype == "scan_inc":
                _scan_busy = True
                asyncio.create_task(_run_scan(manager, req_id, r[2], r[3], mode="incremental"))
            elif rtype == "sync_dialogs":
                asyncio.create_task(_run_sync(manager, req_id))
            elif rtype == "get_count":
                # 统计请求不占扫描锁（只是4次 count RPC，非流式遍历）
                asyncio.create_task(_run_count(manager, req_id, r[2], r[3]))
            else:
                update_request(req_id, REQ_FAILED, f"未知请求类型: {rtype}")
        # 定期清理：只删 3 天前已结束(完成/失败)的历史请求，每 type 保留最近 20 条
        # 进行中(0/1)永不删；避免 web_requests 只增不清导致表无限膨胀
        try:
            _cleanup_old_requests()
        except Exception as e:
            logger.warning(f"⚠️ web_requests 清理失败: {e}")
    except Exception as e:
        logger.error(f"❌ Web 请求处理循环异常: {e}")


def _cleanup_old_requests():
    """定期清理 web_requests：只删 3 天前已结束(2完成/3失败)的历史，每 type 保留最近 20 条"""
    conn = _db_conn()
    try:
        types = [r[0] for r in conn.execute(
            "SELECT DISTINCT type FROM web_requests WHERE status IN (2,3)")]
        for t in types:
            keep = [r[0] for r in conn.execute(
                "SELECT id FROM web_requests WHERE type=? AND status IN (2,3) ORDER BY id DESC LIMIT 20",
                (t,))]
            if not keep:
                continue
            ph = ",".join("?" * len(keep))
            conn.execute(
                "DELETE FROM web_requests WHERE type=? AND status IN (2,3) "
                "AND updated_at < datetime('now','-3 day') AND id NOT IN (" + ph + ")",
                (t,) + tuple(keep))
        conn.commit()
    finally:
        conn.close()


async def _run_scan(manager, req_id, chat_id, chat_name, mode="full"):
    global _scan_busy
    mode_label = "增量扫描" if mode == "incremental" else "重新扫描"
    try:
        # 无硬超时：大群（10万+文件）可一直扫到完。
        # 每500条分批commit，bot被强杀也不丢数据，下次增量续扫。
        count = await scan_group_files(manager, chat_id, chat_name, req_id=req_id, mode=mode)
        if count == 0:
            update_request(req_id, REQ_DONE, f"{mode_label}完成，共 0 个文件（群内可能没有新文件）")
        else:
            # 对账显示：完成文件数 + 失效数（无法显示该频道的空壳，无媒体内容）
            # v1.2 统一口径：以 group_files.invalid 为准（与文件清单同源，页面显示天然一致）
            unavailable = 0
            try:
                unavailable = media_cache.get_invalid_count(chat_id)
            except Exception:
                pass
            if unavailable:
                update_request(req_id, REQ_DONE, f"{mode_label}完成，共 {count} 个文件（其中 {unavailable} 条源频道不可用）")
            else:
                update_request(req_id, REQ_DONE, f"{mode_label}完成，共 {count} 个文件")
    except Exception as e:
        update_request(req_id, REQ_FAILED, f"{mode_label}失败: {e}")
        logger.error(f"❌ 群扫描失败 chat_id={chat_id} mode={mode}: {e}")
    finally:
        _scan_busy = False


def register_worker(manager):
    """挂载请求处理循环：每 5 秒轮询 web_requests（PTB JobQueue / APScheduler）"""
    try:
        job_queue = manager.bot_app.job_queue
        if job_queue is None:
            logger.warning("⚠️ JobQueue 不可用，Web 下载/扫描请求将无人处理")
            return
        _ensure_tables()
        # 💡 启动即清：进程重启后，残留的 RUNNING 请求必然已中断，
        #    立即转失败（不等 10 分钟超时），避免同群新扫描请求被去重逻辑死锁
        _recover_stale_requests(force=True)
        async def _loop(context):
            m = context.bot_data.get("manager")
            if m:
                await process_web_requests(m)
        job_queue.run_repeating(_loop, interval=5.0, first=5.0)
        logger.info("🔁 Web 请求处理循环已挂载（每 5 秒轮询扫描请求；下载请求由 downloader 插件消费）")
    except Exception as e:
        logger.error(f"❌ Web 请求处理循环挂载失败: {e}")
