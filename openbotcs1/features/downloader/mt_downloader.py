#openbot\features\downloader\mt_downloader.py
"""
统一下载插件（业务层）
- 命令：/dl /dl_saved /dl_watch /dls /dl_stop /dl_continue /dl_no /dl_clear
- 监听：收藏夹 / 群 watch /（at_downloader 转发监听）
- 编排：run_job（扫描→下载）、resume_jobs、扫描函数（scan_saved/scan_watch）

底层一律来自 core（插件只依赖 core，不依赖其它插件）：
  core.database        数据库连接/主表/去重/format_size
  core.download_engine 网络管理/单文件下载/批量调度/停机清理
  core.media_scanner   消息遍历 + 媒体提取（iter_media_messages / extract_media_info）
"""
import logging
import os
import sqlite3
import time
import asyncio
import re
import html
from datetime import datetime
from telegram import Update
from telegram.ext import ContextTypes, CommandHandler
from core.command_registry import register_handler
from core.utils import is_admin
from telethon import types

# ---- 底层：core（全项目唯一实现） ----
from core.database import (
    DB_PATH, JOB_CANCELLED, JOB_DONE, JOB_DOWNLOADING, JOB_PAUSED,
    JOB_PENDING, JOB_SCANNING, JOB_WAIT_DL, TASK_DONE, TASK_DOWNLOADING,
    TASK_FAILED, TASK_PENDING, _db_conn, _extract_media_id, _now, add_task,
    create_job, delete_job, format_size, get_job, get_job_stats,
    get_pending_tasks, init_db, is_media_downloaded, list_jobs,
    update_job_progress, update_job_status, update_task_document_id,
    update_task_status,
)
from core import download_engine as _engine

# ---- look 插件数据库（web_requests 下载请求队列）----
# 插件独立原则：不 import look 模块，直接连 download/look.db 消费下载请求
# （look.db 与任务库同目录，重置数据删 download/ 目录即全清；未装 look 插件时文件不存在则静默跳过）
# 环境变量 LOOK_DB_PATH 与 look/db.py 共用，Docker 部署指定持久化路径时两边保持一致
_LOOK_DB_PATH = os.getenv(
    "LOOK_DB_PATH",
    os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        "download", "look.db",
    ),
)

def _look_db_conn():
    """连接 look 插件数据库（WAL + 30s timeout）；文件不存在时返回 None"""
    if not os.path.exists(_LOOK_DB_PATH):
        return None
    conn = sqlite3.connect(_LOOK_DB_PATH, timeout=30)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
    except Exception:
        pass
    return conn
from core.download_engine import (
    INVALID_FILENAME_CHARS, _download_pending_tasks, _download_single_file,
    _network_monitor, _network_monitor_started, _network_monitor_task,
    active_resume_tasks, active_run_jobs, job_runtime, shutdown_downloads,
)
from core.media_scanner import iter_media_messages

logger = logging.getLogger(__name__)

__MODULE_NAME__ = "统一下载插件(命令+监听)"



# 实时速度统计（内存，不持久化）
# 【S3热重载保护】以下所有模块级可变状态在 reload 时必须保留，
# 否则热重载(reload_plugins/add_plugin)会清空运行中的下载/监听任务状态



# 收藏夹监听任务（全局，只有一个监听实例）
# 群/频道监听任务（支持多个同时监听，chat_id -> jid 映射）
# 【S3热重载保护】reload 时保留，避免监听关系丢失
if "SAVED_MONITOR_JID" not in globals():
    SAVED_MONITOR_JID = None  # 收藏夹监听任务的 jid
    WATCH_MONITORS = {}  # 群/频道监听任务：chat_id -> jid 映射






# ===================== 3. 筛选参数解析 =====================
def parse_filters(args):
    """
    解析筛选参数，返回 iter_messages 的 kwargs、描述 和 to_date
    支持：all、关键字、latest:N、from:日期、to:日期、min_id:N、max_id:N
    可组合使用
    """
    if not args:
        return {}, "全部", None
    
    kwargs = {}
    desc_parts = []
    search_terms = []
    to_date = None
    
    for arg in args:
        arg_lower = arg.lower()
        if arg_lower == "all":
            desc_parts.append("全部")
        elif arg_lower.startswith("latest:"):
            try:
                n = int(arg.split(":")[1])
                kwargs["limit"] = n
                desc_parts.append(f"最新{n}条")
            except: pass
        elif arg_lower.startswith("from:"):
            date_str = arg.split(":", 1)[1]
            try:
                from datetime import datetime as dt
                offset_date = dt.strptime(date_str, "%Y-%m-%d")
                kwargs["offset_date"] = offset_date
                desc_parts.append(f"从{date_str}")
            except: pass
        elif arg_lower.startswith("to:"):
            # Bug 6 修复：真正解析 to 日期，供 run_job 扫描循环过滤（此前仅显示不生效）
            date_str = arg.split(":", 1)[1]
            try:
                from datetime import datetime as dt
                to_date = dt.strptime(date_str, "%Y-%m-%d")
                desc_parts.append(f"到{date_str}")
            except: pass
        elif arg_lower.startswith("min_id:"):
            try:
                kwargs["min_id"] = int(arg.split(":")[1])
                desc_parts.append(f"min_id={kwargs['min_id']}")
            except: pass
        elif arg_lower.startswith("max_id:"):
            try:
                kwargs["max_id"] = int(arg.split(":")[1])
                desc_parts.append(f"max_id={kwargs['max_id']}")
            except: pass
        else:
            search_terms.append(arg)
    
    if search_terms:
        kwargs["search"] = " ".join(search_terms)
        desc_parts.append(f"关键字:{kwargs['search']}")
    
    desc = ", ".join(desc_parts) if desc_parts else "全部"
    return kwargs, desc, to_date


# ===================== 5. 任务执行引擎 =====================
def _start_run_job(manager, jid):
    """启动 run_job 任务并追踪，用于停机时取消"""
    async def _wrapper():
        try:
            await run_job(manager, jid)
        finally:
            active_run_jobs.discard(asyncio.current_task())
    task = asyncio.create_task(_wrapper())
    active_run_jobs.add(task)
    return task

async def run_job(manager, jid):
    """执行任务：扫描 → 下载，支持暂停检查"""
    job = get_job(jid)
    if not job: return
    
    # 重置上次中断时停留在"下载中"状态的文件为"待下载"
    # 程序重启意味着之前的下载都中断了，需要重新下载
    conn = _db_conn()
    conn.execute("UPDATE tasks SET status = ? WHERE jid = ? AND status = ?", (TASK_PENDING, jid, TASK_DOWNLOADING))
    conn.commit()
    conn.close()
    
    client = manager.mtproto_client.client
    bot = manager.bot_app.bot
    user_chat_id = job["user_chat_id"]
    
    # 确定 entity
    if job["type"] == "saved":
        # 收藏夹任务：用专门的 scan_saved_messages（有增量扫描+停止阈值）
        update_job_status(jid, JOB_SCANNING)
        added = await scan_saved_messages(manager, jid)
        # 扫描完成后检查是否被暂停（扫描过程中用户可能执行了 /dl_stop）
        current_job = get_job(jid)
        if not current_job or current_job["status"] in (JOB_PAUSED, JOB_CANCELLED):
            logger.info(f"⏸ 任务 #{jid} 扫描后检测到暂停/取消，不进入下载")
            return
        # 进入下载阶段
        update_job_status(jid, JOB_DOWNLOADING)
        await _download_pending_tasks(manager, jid)
        return
    elif job["type"] == "watch":
        # 群/频道监听任务：复用收藏夹的增量扫描逻辑（scan_watch_messages）
        update_job_status(jid, JOB_SCANNING)
        added = await scan_watch_messages(manager, jid)
        # 扫描完成后检查是否被暂停
        current_job = get_job(jid)
        if not current_job or current_job["status"] in (JOB_PAUSED, JOB_CANCELLED):
            logger.info(f"⏸ 任务 #{jid} 扫描后检测到暂停/取消，不进入下载")
            return
        # 进入下载阶段
        update_job_status(jid, JOB_DOWNLOADING)
        await _download_pending_tasks(manager, jid)
        return
    elif job["type"] == "auto" or job["type"] == "web":
        # 自动下载 / Web下载：文件已预先登记（带 document_id），不需要扫描，直接下载
        update_job_status(jid, JOB_DOWNLOADING)
        await _download_pending_tasks(manager, jid)
        return
    else:
        entity_key = parse_link(job["source"])
    
    # 解析筛选参数
    filter_args = job["tag"].split() if job["tag"] and job["tag"] != "all" else []
    iter_kwargs, filter_desc, to_date = parse_filters(filter_args)
    
    # 收藏夹增量扫描：如果有 last_scanned_id 且没有明确筛选，只扫新增的
    if job["type"] == "saved" and job["last_scanned_id"] and job["last_scanned_id"] > 0 and "search" not in iter_kwargs and "limit" not in iter_kwargs:
        iter_kwargs["min_id"] = job["last_scanned_id"]
        logger.info(f"任务 #{jid} 收藏夹增量扫描，min_id={job['last_scanned_id']}")
    
    # 断点续扫：如果有 last_msg_id，从断点继续
    if job["last_msg_id"] and job["last_msg_id"] > 0:
        iter_kwargs["offset_id"] = job["last_msg_id"]
    
    update_job_status(jid, JOB_SCANNING)
    
    try:
        ent = await client.get_entity(entity_key)
    except Exception as e:
        await bot.send_message(user_chat_id, f"❌ 任务 #{jid} 获取实体失败: {e}")
        update_job_status(jid, JOB_PAUSED)
        return
    
    chat_name = getattr(ent, 'title', None) or getattr(ent, 'first_name', None) or '收藏夹'
    chat_id = int(ent.id)
    
    # ===== 扫描阶段：走 core 共享扫描缓存（look 和 downloader 共用，不重复扫） =====
    # 1. 增量扫描（已有缓存只扫新消息，没有就全量），结果写 core/media_cache
    from core.media_scanner import scan_chat_to_cache
    from core import media_cache
    try:
        total_in_cache = await scan_chat_to_cache(
            client, chat_id, chat_name,
            mode="incremental", source="/dl命令",
        )
    except Exception as e:
        logger.error(f"任务 #{jid} core扫描异常: {e}")
        await bot.send_message(user_chat_id, f"❌ 任务 #{jid} 扫描失败: {e}")
        update_job_status(jid, JOB_PAUSED)
        return
    
    # 2. 从共享缓存读出该群全部文件
    cached_files = media_cache.get_files(chat_id)
    
    # 3. 按 tag 筛选（video/photo/all）
    media_type = job["tag"] if job["tag"] in ("video", "photo") else None
    _VID_EXT = {".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv", ".webm", ".m4v", ".mpg", ".mpeg", ".3gp", ".ts", ".rmvb"}
    _IMG_EXT = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".svg", ".heic", ".ico", ".tiff"}
    added = 0
    for f in cached_files:
        fn = (f.get("file_name") or "").lower()
        dot = fn.rfind(".")
        ext = fn[dot:] if dot >= 0 else ""
        if media_type == "video" and ext not in _VID_EXT:
            continue
        if media_type == "photo" and ext not in _IMG_EXT:
            continue
        add_task(
            jid, f["msg_id"], chat_id, chat_name,
            f.get("file_name") or str(f["msg_id"]),
            f.get("file_size") or 0,
            f.get("document_id") or "",
            str(chat_id),
        )
        added += 1
    
    # 扫描完成通知
    update_job_status(jid, JOB_WAIT_DL)
    await bot.send_message(
        user_chat_id,
        f"🔍 任务 #{jid} 扫描完成\n"
        f"缓存共 {total_in_cache} 个文件，符合条件 {added} 个，开始下载..."
    )
    
    # 下载阶段（设置状态为下载中，然后下载）
    update_job_status(jid, JOB_DOWNLOADING)
    await _download_pending_tasks(manager, jid)


# ===================== 6. 启动恢复 =====================
async def resume_jobs(manager):
    """启动时恢复未完成的任务"""
    global SAVED_MONITOR_JID
    if not manager.mtproto_client or not manager.mtproto_client.client:
        logger.warning("⚠️ MTProto 未就绪，跳过下载任务恢复")
        return
    if not await manager.mtproto_client.ensure_ready():
        logger.error("❌ 恢复任务时 MTProto 未就绪（未登录或连接失败），请先 /mtlogin")
        return
    
    # 恢复进行中的任务（待扫描、扫描中、待下载、下载中）
    for status in [JOB_PENDING, JOB_SCANNING, JOB_WAIT_DL, JOB_DOWNLOADING]:
        jobs = list_jobs(status_filter=status)
        for job in jobs:
            if job["type"] == "saved":
                # 收藏夹监听任务：恢复 SAVED_MONITOR_JID + 统一走 run_job
                SAVED_MONITOR_JID = job["jid"]
                logger.info(f"📡 恢复收藏夹监听任务 #{job['jid']}")
            elif job["type"] == "watch":
                # 群监听任务：延迟解析 entity 并注册到 WATCH_MONITORS
                async def _restore_watch(jid=job["jid"]):
                    await asyncio.sleep(6)  # 等连接稳定 + run_job 启动
                    await _register_watch_monitor(manager, jid)
                asyncio.create_task(_restore_watch())
                logger.info(f"📡 恢复群监听任务 #{job['jid']}")
            else:
                logger.info(f"🔄 恢复任务 #{job['jid']} (type={job['type']}, status={job['status']})")
            
            # 所有任务统一走 run_job（扫描→下载→收藏夹保持监听）
            async def _resume_job(jid=job["jid"]):
                try:
                    await asyncio.sleep(5)  # 等连接稳定
                    await run_job(manager, jid)
                finally:
                    active_resume_tasks.discard(asyncio.current_task())
            task = asyncio.create_task(_resume_job())
            active_resume_tasks.add(task)
    
    # 暂停的任务不自动恢复，等用户手动 /dl_continue

# ===================== 7. 工具函数 =====================
# 单次 /dl 指定ID模式的数量上限，防止超大区间撑爆内存 (Bug 3 修复)
MAX_IDS = 5000

def parse_msg_ids(arg):
    """
    解析消息ID参数，支持：
    - 单个: 12345
    - 范围: 12345-12350
    - 多个: 12345,12346,12347
    - 混合: 12345,12350-12355,12360
    返回去重排序后的ID列表
    """
    if not arg: return []
    ids = set()
    for part in arg.split(','):
        part = part.strip()
        if not part: continue
        if '-' in part and not part.startswith('-'):
            try:
                start, end = part.split('-', 1)
                start, end = int(start.strip()), int(end.strip())
                r = range(min(start, end), max(start, end) + 1)
                # 安全加固：限制单次指定ID总数，防止超大区间(如 1-999999999)撑爆内存(DoS)
                if len(ids) + len(r) > MAX_IDS:
                    raise ValueError(f"❌ 指定ID数量超过上限 {MAX_IDS} 条，请分批下载")
                ids.update(r)
            except ValueError:
                raise
            except: pass
        else:
            try:
                # 逗号分隔的单个 ID 同样受 MAX_IDS 限制，防止绕过范围模式上限
                if len(ids) >= MAX_IDS:
                    raise ValueError(f"❌ 指定ID数量超过上限 {MAX_IDS} 条，请分批下载")
                ids.add(int(part))
            except ValueError:
                raise
            except: pass
    return sorted(ids)

def parse_link(link):
    if "/+" in link or "joinchat" in link: return link
    parts = link.rstrip('/').split('/')
    if 't.me/c/' in link:
        for p in parts:
            if p.isdigit() and len(p) > 5: return int("-100" + p)
    return parts[-1]

def status_text(status):
    return {
        JOB_PENDING: "⏳ 待扫描",
        JOB_SCANNING: "🔍 扫描中",
        JOB_WAIT_DL: "📥 待下载",
        JOB_DOWNLOADING: "⬇️ 下载中",
        JOB_DONE: "✅ 已完成",
        JOB_PAUSED: "⏸ 已暂停",
        JOB_CANCELLED: "❌ 已取消",
    }.get(status, "未知")

def type_text(job_type):
    return {"auto": "自动下载", "channel": "频道下载", "saved": "收藏夹", "watch": "群监听"}.get(job_type, job_type)

def format_speed(bytes_per_sec):
    if bytes_per_sec <= 0: return "0 B/s"
    for unit in ['B/s', 'KB/s', 'MB/s', 'GB/s']:
        if bytes_per_sec < 1024: return f"{bytes_per_sec:.1f} {unit}"
        bytes_per_sec /= 1024
    return f"{bytes_per_sec:.1f} TB/s"


# ===================== 8. 命令处理器 =====================
async def handle_dl_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/dl [链接] [筛选变量/消息ID] - 新建频道下载任务
    支持搜刮模式和指定ID模式：
    - 搜刮: /dl 链接 latest:50 视频
    - 指定ID: /dl 链接 12345 | 12345-12350 | 12345,12346,12347
    """
    manager = getattr(handle_dl_command, "manager", None) or context.bot_data.get('manager')
    if not manager or not is_admin(update.effective_user.id, manager.config):
        return
    if not manager or not manager.mtproto_client:
        return await update.message.reply_text("❌ MTProto 未就绪")
    if not context.args:
        return await update.message.reply_text(
            "💡 <b>用法:</b> /dl [链接] [筛选变量/消息ID]\n"
            "━━━━━━━━━━━━━━━\n"
            "<b>搜刮模式:</b>\n"
            "  筛选变量: all / 关键字 / latest:N / from:日期 / to:日期 / min_id:N / max_id:N\n"
            "  可组合使用，例如: /dl 链接 latest:50 视频\n"
            "━━━━━━━━━━━━━━━\n"
            "<b>指定消息ID模式:</b>\n"
            "  单个: /dl 链接 12345\n"
            "  范围: /dl 链接 12345-12350\n"
            "  多个: /dl 链接 12345,12346,12347\n"
            "  混合: /dl 链接 12345,12350-12355,12360",
            parse_mode="HTML"
        )
    
    link = context.args[0].strip()
    second_arg = context.args[1] if len(context.args) > 1 else ""
    
    # 判断是不是消息ID（纯数字、范围、逗号分隔，不含字母）
    msg_ids = []
    if second_arg and re.match(r'^[\d,\-\s]+$', second_arg):
        try:
            msg_ids = parse_msg_ids(second_arg)
        except ValueError as e:
            return await update.message.reply_text(str(e))
    
    if msg_ids:
        # ===== 指定ID下载模式 =====
        jid = create_job("channel", link, f"ids:{second_arg}", update.effective_chat.id)
        
        await update.message.reply_html(
            f"📥 <b>已创建指定ID下载任务</b>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"📋 任务编号: <code>#{jid}</code>\n"
            f"📁 来源: {link}\n"
            f"🔢 消息ID: {second_arg}\n"
            f"📊 共 {len(msg_ids)} 条消息\n"
            f"━━━━━━━━━━━━━━━\n"
            f"用 /dls #{jid} 查看详情和下载速度"
        )
        
        if not await manager.mtproto_client.ensure_ready():
            return await update.message.reply_text("❌ MTProto 未就绪（未登录或连接失败），请先 /mtlogin")
        
        asyncio.create_task(download_by_ids(manager, jid, link, msg_ids))
    else:
        # ===== 搜刮模式 =====
        filter_args = context.args[1:]
        tag = " ".join(filter_args) if filter_args else "all"
        
        jid = create_job("channel", link, tag, update.effective_chat.id)
        
        _, filter_desc, _ = parse_filters(filter_args)
        await update.message.reply_html(
            f"📥 <b>已创建搜刮下载任务</b>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"📋 任务编号: <code>#{jid}</code>\n"
            f"📁 来源: {link}\n"
            f"🔍 筛选: {filter_desc}\n"
            f"━━━━━━━━━━━━━━━\n"
            f"用 /dls #{jid} 查看详情和下载速度"
        )
        
        if not await manager.mtproto_client.ensure_ready():
            return await update.message.reply_text("❌ MTProto 未就绪（未登录或连接失败），请先 /mtlogin")
        
        _start_run_job(manager, jid)

async def handle_dl_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/dl_video [链接] - 只下载视频"""
    manager = getattr(handle_dl_video, "manager", None) or context.bot_data.get('manager')
    if not manager or not is_admin(update.effective_user.id, manager.config):
        return
    if not manager or not manager.mtproto_client:
        return await update.message.reply_text("❌ MTProto 未就绪")
    if not context.args:
        return await update.message.reply_text("💡 用法: /dl_video [链接]")
    
    link = context.args[0].strip()
    jid = create_job("channel", link, "video", update.effective_chat.id)
    
    await update.message.reply_html(
        f"📥 <b>已创建视频下载任务</b>\n"
        f"━━━━━━━━━━━━━━━\n"
        f"📋 任务编号: <code>#{jid}</code>\n"
        f"📁 来源: {link}\n"
        f"🎬 类型: 只下载视频\n"
        f"━━━━━━━━━━━━━━━\n"
        f"用 /dls #{jid} 查看详情和下载速度"
    )
    
    if not await manager.mtproto_client.ensure_ready():
        return await update.message.reply_text("❌ MTProto 未就绪（未登录或连接失败），请先 /mtlogin")
    _start_run_job(manager, jid)

async def handle_dl_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/dl_photo [链接] - 只下载图片"""
    manager = getattr(handle_dl_photo, "manager", None) or context.bot_data.get('manager')
    if not manager or not is_admin(update.effective_user.id, manager.config):
        return
    if not manager or not manager.mtproto_client:
        return await update.message.reply_text("❌ MTProto 未就绪")
    if not context.args:
        return await update.message.reply_text("💡 用法: /dl_photo [链接]")
    
    link = context.args[0].strip()
    jid = create_job("channel", link, "photo", update.effective_chat.id)
    
    await update.message.reply_html(
        f"📥 <b>已创建图片下载任务</b>\n"
        f"━━━━━━━━━━━━━━━\n"
        f"📋 任务编号: <code>#{jid}</code>\n"
        f"📁 来源: {link}\n"
        f"🖼️ 类型: 只下载图片\n"
        f"━━━━━━━━━━━━━━━\n"
        f"用 /dls #{jid} 查看详情和下载速度"
    )
    
    if not await manager.mtproto_client.ensure_ready():
        return await update.message.reply_text("❌ MTProto 未就绪（未登录或连接失败），请先 /mtlogin")
    _start_run_job(manager, jid)

async def download_by_ids(manager, jid, link, msg_ids):
    """指定ID下载：直接获取消息并添加到任务，然后下载"""
    job = get_job(jid)
    if not job: return
    
    client = manager.mtproto_client.client
    bot = manager.bot_app.bot
    user_chat_id = job["user_chat_id"]
    
    # 重置中断的下载中文件
    conn = _db_conn()
    conn.execute("UPDATE tasks SET status = ? WHERE jid = ? AND status = ?", (TASK_PENDING, jid, TASK_DOWNLOADING))
    conn.commit()
    conn.close()
    
    update_job_status(jid, JOB_SCANNING)
    
    try:
        entity_key = parse_link(link)
        ent = await client.get_entity(entity_key)
        chat_name = getattr(ent, 'title', None) or getattr(ent, 'first_name', None) or '未知'
        
        # 批量获取消息（Telethon 支持 ids 传列表）
        messages = await client.get_messages(ent, ids=msg_ids)
        
        found = 0
        for m in messages:
            if not m or not m.media: continue
            found += 1
            
            # 提取文件名和大小
            file_name = f"{m.id}"
            file_size = 0
            if hasattr(m.media, 'document') and m.media.document:
                file_size = m.media.document.size or 0
                for attr in m.media.document.attributes:
                    if isinstance(attr, types.DocumentAttributeFilename):
                        file_name = attr.file_name
            elif hasattr(m.media, 'photo') and m.media.photo:
                file_name = f"photo_{m.id}.jpg"
            
            add_task(jid, m.id, ent.id, chat_name, file_name, file_size, _extract_media_id(m.media), str(ent.id))
        
        update_job_status(jid, JOB_WAIT_DL)
        await bot.send_message(
            user_chat_id,
            f"🔍 任务 #{jid} 消息获取完成\n"
            f"指定 {len(msg_ids)} 条ID，找到 {found} 个媒体文件，开始下载..."
        )
        
    except Exception as e:
        logger.error(f"任务 #{jid} 获取指定ID消息失败: {e}")
        await bot.send_message(user_chat_id, f"❌ 任务 #{jid} 获取消息失败: {e}")
        update_job_status(jid, JOB_PAUSED)
        return
    
    # 下载阶段
    await _download_pending_tasks(manager, jid)

async def scan_saved_messages(manager, jid, limit=None, stop_consecutive=50):
    """
    增量扫描收藏夹中未下载的历史消息（带限速+断点续扫）
    - 从最新消息往前扫（或从断点继续）
    - 遇到连续 stop_consecutive 条已下载的就停止（增量）
    - 最多扫描 limit 条
    - 每扫描 100 条暂停 2 秒（限速），并更新扫描断点
    - 重启后从断点继续扫描，不从头来
    返回新增的文件数
    """
    client = manager.mtproto_client.client
    if not await manager.mtproto_client.ensure_ready():
        return 0
    
    # 读取扫描断点和进度（支持扫描中途关闭后续扫）
    conn = _db_conn()
    row = conn.execute("SELECT last_scanned_id, scan_progress_id, scan_start_id FROM jobs WHERE jid=?", (jid,)).fetchone()
    conn.close()
    last_scanned_id = row[0] if row and row[0] else None
    scan_progress_id = row[1] if row and row[1] else None
    scan_start_id = row[2] if row and row[2] else None
    
    # 读取 job 的媒体类型过滤（tag=video/photo）
    job_row = get_job(jid)
    media_type = job_row["tag"] if job_row and job_row["tag"] in ("video", "photo") else None
    
    added = 0
    consecutive_no_new = 0  # 连续无新增文件的消息数（每扫描一条就+1，发现新增就重置）
    scanned = 0  # 已扫描的消息数（用于限速）

    # 收藏夹用户 ID：函数开头获取一次，避免循环内对每条消息重复 get_me 网络请求
    me_id = _engine.SAVED_USER_ID
    if not me_id:
        try:
            me_id = (await client.get_me()).id
        except Exception:
            me_id = None
    
    # 如果有扫描进度（上次没扫完），从进度位置继续往旧扫
    # scan_start_id 是本次扫描开始时的最新消息ID，扫完后更新为断点
    if scan_progress_id and scan_start_id:
        first_msg_id = scan_start_id
        is_resuming = True
        logger.info(f"🔍 恢复收藏夹扫描：从消息ID={scan_progress_id} 继续往旧扫（起点={scan_start_id}）")
        messages_iter = client.iter_messages('me', limit=limit, offset_id=scan_progress_id)
    else:
        first_msg_id = None
        is_resuming = False
        # 从最新消息往前扫，靠停止阈值（连续无新增）来停止
        messages_iter = client.iter_messages('me', limit=limit)
    
    try:
        async for msg in messages_iter:
            # 新扫描：记录第一条消息的ID（最新的），并保存 scan_start_id
            if first_msg_id is None:
                first_msg_id = msg.id
                conn = _db_conn()
                conn.execute("UPDATE jobs SET scan_start_id=? WHERE jid=?", (first_msg_id, jid))
                conn.commit()
                conn.close()
            
            # 如果扫到了上次的断点，说明没有新增消息了，停止
            if last_scanned_id and msg.id <= last_scanned_id:
                logger.info(f"🔍 收藏夹扫描完成：已扫到上次断点 {last_scanned_id}，停止扫描")
                break
            
            # 检查暂停/取消
            current_job = get_job(jid)
            if not current_job or current_job["status"] in (JOB_PAUSED, JOB_CANCELLED):
                logger.info(f"任务 #{jid} 被暂停/取消，停止收藏夹扫描")
                return added
            
            scanned += 1
            is_new_file = False  # 本条消息是否是新增的待下载文件
            
            # 限速：每20条停0.2秒，每100条停2秒，并更新扫描进度
            if scanned % 100 == 0:
                logger.info(f"🔍 收藏夹扫描进度：已扫描 {scanned} 条，发现 {added} 个未下载文件，当前ID={msg.id}")
                # 更新扫描进度（中途关闭后可从这里继续）
                conn = _db_conn()
                conn.execute("UPDATE jobs SET scan_progress_id=? WHERE jid=?", (msg.id, jid))
                conn.commit()
                conn.close()
                await asyncio.sleep(2)
            elif scanned % 20 == 0:
                await asyncio.sleep(0.2)
            
            if not msg.media:
                continue  # 无媒体，直接跳过，不计入连续无新增
            if not isinstance(msg.media, (types.MessageMediaDocument, types.MessageMediaPhoto)):
                continue  # 非文档/图片，直接跳过，不计入连续无新增
            
            # 媒体类型过滤（tag=video/photo 时只下载指定类型）
            # 不匹配的直接跳过，不计入连续无新增（避免视频扫描时被图片/文本打断）
            if media_type == "video":
                if not isinstance(msg.media, types.MessageMediaDocument):
                    continue
                if not msg.media.document or "video" not in (msg.media.document.mime_type or ""):
                    continue
            elif media_type == "photo":
                if isinstance(msg.media, types.MessageMediaPhoto):
                    pass
                elif isinstance(msg.media, types.MessageMediaDocument):
                    if not msg.media.document or "image" not in (msg.media.document.mime_type or ""):
                        continue
                else:
                    continue
            
            # 到这里说明是匹配媒体类型的消息，检查 tasks 表
            conn = _db_conn()
            row = conn.execute("SELECT tid, status FROM tasks WHERE jid=? AND msg_id=?", (jid, msg.id)).fetchone()
            conn.close()
            
            if row:
                tid, status = row
                if status == TASK_DONE:
                    # 已完成的匹配媒体，计入连续无新增
                    consecutive_no_new += 1
                    if consecutive_no_new >= stop_consecutive:
                        logger.info(f"🔍 收藏夹扫描完成：连续 {stop_consecutive} 条已下载的匹配媒体，停止扫描")
                        break
                    continue
                else:
                    # 失败/待下载的，重置为待下载，算新增（需要重新下载）
                    conn = _db_conn()
                    conn.execute("UPDATE tasks SET status=? WHERE tid=?", (TASK_PENDING, tid))
                    conn.commit()
                    conn.close()
                    consecutive_no_new = 0
                    added += 1
                    continue
            
            # 新增的匹配媒体文件
            consecutive_no_new = 0
            
            # 🚨 全局去重：该媒体（document.id/photo.id）在任意任务中已下载/下载中 → 跳过不下载
            _media_id = _extract_media_id(msg.media)
            if _media_id and is_media_downloaded(_media_id):
                consecutive_no_new += 1
                if consecutive_no_new >= stop_consecutive:
                    logger.info(f"🔍 扫描完成：连续 {stop_consecutive} 条已下载的媒体，停止扫描")
                    break
                continue
            
            # 提取文件名和大小
            file_name = f"{msg.id}"
            file_size = 0
            if hasattr(msg.media, 'document') and msg.media.document:
                file_size = msg.media.document.size or 0
                for attr in msg.media.document.attributes:
                    if isinstance(attr, types.DocumentAttributeFilename):
                        file_name = attr.file_name
            elif hasattr(msg.media, 'photo') and msg.media.photo:
                file_name = f"photo_{msg.id}.jpg"
            
            # 扫描阶段去重：检查本地是否已有同名同大小的文件，有就跳过不下载
            if me_id:
                try:
                    _safe_name = re.sub(INVALID_FILENAME_CHARS, '_', "收藏夹")
                    _save_dir = os.path.join(DOWNLOAD_DIR, str(me_id), _safe_name)
                    _fname = re.sub(INVALID_FILENAME_CHARS, '_', file_name)
                    if len(_fname) > 200:
                        _name, _ext = os.path.splitext(_fname)
                        _fname = _name[:190] + _ext
                    _fpath = os.path.join(_save_dir, _fname)
                    if os.path.exists(_fpath) and os.path.getsize(_fpath) == file_size and file_size > 0:
                        # 同名同大小，已存在，跳过不添加任务
                        continue
                except Exception:
                    pass  # 检查失败就正常添加任务
            
            # 只添加到任务表，扫描完毕后统一下载
            add_task(jid, msg.id, _engine.SAVED_USER_ID, "收藏夹", file_name, file_size, _extract_media_id(msg.media), str(_engine.SAVED_USER_ID))
            added += 1
    except Exception as e:
        logger.error(f"扫描收藏夹历史消息失败: {e}")
    
    # 扫描完成，更新断点为本次扫描到的最新消息ID（下次只扫比这个更新的）
    # 同时清除扫描进度（scan_progress_id 和 scan_start_id）
    if first_msg_id:
        conn = _db_conn()
        conn.execute("UPDATE jobs SET last_scanned_id=?, scan_progress_id=NULL, scan_start_id=NULL WHERE jid=?", (first_msg_id, jid))
        conn.commit()
        conn.close()
    
    logger.info(f"🔍 收藏夹扫描结束：共扫描 {scanned} 条，新增 {added} 个未下载文件，断点={first_msg_id}")
    return added

async def _register_watch_monitor(manager, jid):
    """解析 watch 任务的群/频道 entity，将 chat_id -> jid 注册到 WATCH_MONITORS（用于新消息事件匹配）"""
    job = get_job(jid)
    if not job or job["type"] != "watch":
        return
    client = manager.mtproto_client.client
    if not client:
        return
    try:
        entity_key = parse_link(job["source"])
        ent = await client.get_entity(entity_key)
        WATCH_MONITORS[ent.id] = jid
        _ensure_watch_handler(manager, client)
        logger.info(f"📡 注册群监听: chat_id={ent.id} -> jid={jid}")
    except Exception as e:
        logger.warning(f"⚠️ 注册群监听失败 jid={jid}: {e}")

async def scan_watch_messages(manager, jid, limit=None, stop_consecutive=50):
    """
    增量扫描群/频道中未下载的历史消息（带限速+断点续扫）
    逻辑和 scan_saved_messages 完全一致，只是目标从收藏夹('me')换成指定群/频道 entity
    - 从最新消息往前扫（或从断点继续）
    - 遇到连续 stop_consecutive 条已下载的就停止（增量）
    - 重启后从断点继续扫描，不从头来
    返回新增的文件数
    """
    client = manager.mtproto_client.client
    if not await manager.mtproto_client.ensure_ready():
        return 0

    job = get_job(jid)
    if not job:
        return 0

    # 解析群/频道 entity
    entity_key = parse_link(job["source"])
    try:
        entity = await client.get_entity(entity_key)
    except Exception as e:
        logger.error(f"任务 #{jid} 获取群/频道实体失败: {e}")
        return 0

    chat_id = entity.id
    chat_name = getattr(entity, 'title', None) or getattr(entity, 'first_name', None) or str(chat_id)

    # 读取扫描断点和进度（支持扫描中途关闭后续扫）
    conn = _db_conn()
    row = conn.execute("SELECT last_scanned_id, scan_progress_id, scan_start_id FROM jobs WHERE jid=?", (jid,)).fetchone()
    conn.close()
    last_scanned_id = row[0] if row and row[0] else None
    scan_progress_id = row[1] if row and row[1] else None
    scan_start_id = row[2] if row and row[2] else None

    # 读取 job 的媒体类型过滤（tag=video/photo）
    media_type = job["tag"] if job["tag"] in ("video", "photo") else None

    added = 0
    consecutive_no_new = 0  # 连续无新增文件的消息数
    scanned = 0

    # 如果有扫描进度（上次没扫完），从进度位置继续往旧扫
    if scan_progress_id and scan_start_id:
        first_msg_id = scan_start_id
        logger.info(f"🔍 恢复群监听扫描：从消息ID={scan_progress_id} 继续往旧扫（起点={scan_start_id}）")
        messages_iter = client.iter_messages(entity, limit=limit, offset_id=scan_progress_id)
    else:
        first_msg_id = None
        # 从最新消息往前扫，靠停止阈值（连续无新增）来停止
        messages_iter = client.iter_messages(entity, limit=limit)

    try:
        async for msg in messages_iter:
            # 新扫描：记录第一条消息的ID（最新的），并保存 scan_start_id
            if first_msg_id is None:
                first_msg_id = msg.id
                conn = _db_conn()
                conn.execute("UPDATE jobs SET scan_start_id=? WHERE jid=?", (first_msg_id, jid))
                conn.commit()
                conn.close()

            # 如果扫到了上次的断点，说明没有新增消息了，停止
            if last_scanned_id and msg.id <= last_scanned_id:
                logger.info(f"🔍 群监听扫描完成：已扫到上次断点 {last_scanned_id}，停止扫描")
                break

            # 检查暂停/取消
            current_job = get_job(jid)
            if not current_job or current_job["status"] in (JOB_PAUSED, JOB_CANCELLED):
                logger.info(f"任务 #{jid} 被暂停/取消，停止群监听扫描")
                return added

            scanned += 1

            # 限速：每20条停0.2秒，每100条停2秒，并更新扫描进度
            if scanned % 100 == 0:
                logger.info(f"🔍 群监听扫描进度：已扫描 {scanned} 条，发现 {added} 个未下载文件，当前ID={msg.id}")
                conn = _db_conn()
                conn.execute("UPDATE jobs SET scan_progress_id=? WHERE jid=?", (msg.id, jid))
                conn.commit()
                conn.close()
                await asyncio.sleep(2)
            elif scanned % 20 == 0:
                await asyncio.sleep(0.2)

            if not msg.media:
                continue  # 无媒体，直接跳过
            if not isinstance(msg.media, (types.MessageMediaDocument, types.MessageMediaPhoto)):
                continue  # 非文档/图片，直接跳过

            # 媒体类型过滤（tag=video/photo 时只下载指定类型）
            if media_type == "video":
                if not isinstance(msg.media, types.MessageMediaDocument):
                    continue
                if not msg.media.document or "video" not in (msg.media.document.mime_type or ""):
                    continue
            elif media_type == "photo":
                if isinstance(msg.media, types.MessageMediaPhoto):
                    pass
                elif isinstance(msg.media, types.MessageMediaDocument):
                    if not msg.media.document or "image" not in (msg.media.document.mime_type or ""):
                        continue
                else:
                    continue

            # 到这里说明是匹配媒体类型的消息，检查 tasks 表
            conn = _db_conn()
            row = conn.execute("SELECT tid, status FROM tasks WHERE jid=? AND msg_id=?", (jid, msg.id)).fetchone()
            conn.close()

            if row:
                tid, status = row
                if status == TASK_DONE:
                    # 已完成的匹配媒体，计入连续无新增
                    consecutive_no_new += 1
                    if consecutive_no_new >= stop_consecutive:
                        logger.info(f"🔍 群监听扫描完成：连续 {stop_consecutive} 条已下载的匹配媒体，停止扫描")
                        break
                    continue
                else:
                    # 失败/待下载的，重置为待下载，算新增（需要重新下载）
                    conn = _db_conn()
                    conn.execute("UPDATE tasks SET status=? WHERE tid=?", (TASK_PENDING, tid))
                    conn.commit()
                    conn.close()
                    consecutive_no_new = 0
                    added += 1
                    continue

            # 新增的匹配媒体文件
            consecutive_no_new = 0

            # 提取文件名和大小
            file_name = f"{msg.id}"
            file_size = 0
            if hasattr(msg.media, 'document') and msg.media.document:
                file_size = msg.media.document.size or 0
                for attr in msg.media.document.attributes:
                    if isinstance(attr, types.DocumentAttributeFilename):
                        file_name = attr.file_name
            elif hasattr(msg.media, 'photo') and msg.media.photo:
                file_name = f"photo_{msg.id}.jpg"

            # 扫描阶段去重：检查本地是否已有同名同大小的文件，有就跳过不下载
            try:
                _safe_name = re.sub(INVALID_FILENAME_CHARS, '_', str(chat_name))
                _save_dir = os.path.join(DOWNLOAD_DIR, str(chat_id), _safe_name)
                _fname = re.sub(INVALID_FILENAME_CHARS, '_', file_name)
                if len(_fname) > 200:
                    _name, _ext = os.path.splitext(_fname)
                    _fname = _name[:190] + _ext
                _fpath = os.path.join(_save_dir, _fname)
                if os.path.exists(_fpath) and os.path.getsize(_fpath) == file_size and file_size > 0:
                    continue
            except Exception:
                pass

            # 只添加到任务表，扫描完毕后统一下载
            add_task(jid, msg.id, chat_id, chat_name, file_name, file_size, _extract_media_id(msg.media), str(chat_id))
            added += 1
    except Exception as e:
        logger.error(f"扫描群/频道历史消息失败: {e}")

    # 扫描完成，更新断点为本次扫描到的最新消息ID（下次只扫比这个更新的）
    if first_msg_id:
        conn = _db_conn()
        conn.execute("UPDATE jobs SET last_scanned_id=?, scan_progress_id=NULL, scan_start_id=NULL WHERE jid=?", (first_msg_id, jid))
        conn.commit()
        conn.close()

    logger.info(f"🔍 群监听扫描结束：共扫描 {scanned} 条，新增 {added} 个未下载文件，断点={first_msg_id}")
    return added

async def handle_saved_message(event):
    """收藏夹新消息事件处理：有新媒体就自动下载"""
    global SAVED_MONITOR_JID

    # S5兜底：_engine.SAVED_USER_ID 为空时动态获取并缓存（即使启动时未登录，登录后无需重启也能自愈）
    if not _engine.SAVED_USER_ID:
        try:
            me = await event.client.get_me()
            _engine.SAVED_USER_ID = me.id
            logger.info(f"👤 收藏夹监听用户ID(动态刷新): {_engine.SAVED_USER_ID}")
        except Exception:
            pass

    logger.info(f"📨 收藏夹监听触发: chat_id={event.chat_id}, _engine.SAVED_USER_ID={_engine.SAVED_USER_ID}, has_media={bool(event.message.media)}")
    
    # 只处理收藏夹的消息（chat_id == 用户自己的ID）
    if not _engine.SAVED_USER_ID or event.chat_id != _engine.SAVED_USER_ID:
        logger.info(f"❌ 收藏夹监听跳过: chat_id不匹配或SAVED_USER_ID为空")
        return
    if not event.message.media:
        logger.info(f"❌ 收藏夹监听跳过: 无媒体")
        return
    # 只处理真正的文档和图片，过滤链接预览等
    if not isinstance(event.message.media, (types.MessageMediaDocument, types.MessageMediaPhoto)):
        logger.info(f"❌ 收藏夹监听跳过: 媒体类型不匹配: {type(event.message.media)}")
        return
    
    # 检查监听任务是否存在且未暂停/取消
    if not SAVED_MONITOR_JID:
        logger.info(f"❌ 收藏夹监听跳过: SAVED_MONITOR_JID为空（未启动/dl_saved）")
        return
    job = get_job(SAVED_MONITOR_JID)
    if not job or job["status"] in (JOB_PAUSED, JOB_CANCELLED, JOB_DONE):
        return
    
    # 媒体类型过滤（tag=video/photo 时只下载指定类型）
    media_type = job["tag"] if job["tag"] in ("video", "photo") else None
    if media_type == "video":
        if not isinstance(event.message.media, types.MessageMediaDocument): return
        if not event.message.media.document or "video" not in (event.message.media.document.mime_type or ""): return
    elif media_type == "photo":
        if isinstance(event.message.media, types.MessageMediaPhoto):
            pass
        elif isinstance(event.message.media, types.MessageMediaDocument):
            if not event.message.media.document or "image" not in (event.message.media.document.mime_type or ""): return
        else:
            return
    
    jid = SAVED_MONITOR_JID
    msg = event.message
    client = event.client
    chat_name = "收藏夹"
    manager = getattr(handle_saved_message, "manager", None)
    
    # 提取文件名和大小
    file_name = f"{msg.id}"
    file_size = 0
    if hasattr(msg.media, 'document') and msg.media.document:
        file_size = msg.media.document.size or 0
        for attr in msg.media.document.attributes:
            if isinstance(attr, types.DocumentAttributeFilename):
                file_name = attr.file_name
    elif hasattr(msg.media, 'photo') and msg.media.photo:
        file_name = f"photo_{msg.id}.jpg"
    
    # 🚨 全局去重：该媒体（document.id/photo.id）已下载/下载中 → 跳过不重复下载
    _media_id = _extract_media_id(msg.media)
    if _media_id and is_media_downloaded(_media_id):
        logger.info("⏭️ 收藏夹监听去重跳过：该媒体已下载过")
        return

    # 添加到任务表（创建时即写入媒体ID）
    tid = add_task(jid, msg.id, _engine.SAVED_USER_ID, chat_name, file_name, file_size, _media_id, str(_engine.SAVED_USER_ID))
    
    # 通知用户收藏夹有新文件正在下载
    try:
        await manager.bot_app.bot.send_message(
            job["user_chat_id"],
            f"⭐ <b>收藏夹新文件</b>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"📋 任务: <code>#{jid}</code>\n"
            f"📄 文件名: {html.escape(file_name[:50])}\n"
            f"📦 大小: {format_size(file_size)}\n"
            f"📨 消息ID: <code>{msg.id}</code>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"⬇️ 正在自动下载...",
            parse_mode="HTML"
        )
    except: pass
    
    # 异步下载
    asyncio.create_task(_download_single_file(client, jid, tid, msg.id, _engine.SAVED_USER_ID, chat_name))

async def handle_watch_message(event):
    """群/频道新消息事件处理：全局监听所有消息，按 chat_id 匹配 WATCH_MONITORS 后自动下载"""
    chat_id = event.chat_id

    # 查是否在监听列表中（不在直接返回，不影响性能）
    if chat_id not in WATCH_MONITORS:
        return
    jid = WATCH_MONITORS[chat_id]

    if not event.message.media:
        return
    # 只处理真正的文档和图片，过滤链接预览等
    if not isinstance(event.message.media, (types.MessageMediaDocument, types.MessageMediaPhoto)):
        return

    # 检查监听任务是否存在且未暂停/取消
    job = get_job(jid)
    if not job or job["status"] in (JOB_PAUSED, JOB_CANCELLED, JOB_DONE):
        return

    # 媒体类型过滤（tag=video/photo 时只下载指定类型）
    media_type = job["tag"] if job["tag"] in ("video", "photo") else None
    if media_type == "video":
        if not isinstance(event.message.media, types.MessageMediaDocument): return
        if not event.message.media.document or "video" not in (event.message.media.document.mime_type or ""): return
    elif media_type == "photo":
        if isinstance(event.message.media, types.MessageMediaPhoto):
            pass
        elif isinstance(event.message.media, types.MessageMediaDocument):
            if not event.message.media.document or "image" not in (event.message.media.document.mime_type or ""): return
        else:
            return

    msg = event.message
    client = event.client

    # 获取群名（优先从 event.chat，失败则查 entity）
    chat_name = "群监听"
    try:
        if event.chat:
            chat_name = getattr(event.chat, 'title', None) or getattr(event.chat, 'first_name', None) or str(chat_id)
        else:
            ent = await client.get_entity(chat_id)
            chat_name = getattr(ent, 'title', None) or getattr(ent, 'first_name', None) or str(chat_id)
    except:
        pass

    # 提取文件名和大小
    file_name = f"{msg.id}"
    file_size = 0
    if hasattr(msg.media, 'document') and msg.media.document:
        file_size = msg.media.document.size or 0
        for attr in msg.media.document.attributes:
            if isinstance(attr, types.DocumentAttributeFilename):
                file_name = attr.file_name
    elif hasattr(msg.media, 'photo') and msg.media.photo:
        file_name = f"photo_{msg.id}.jpg"

    # 🚨 全局去重：该媒体（document.id/photo.id）已下载/下载中 → 跳过不重复下载
    _media_id = _extract_media_id(msg.media)
    if _media_id and is_media_downloaded(_media_id):
        logger.info(f"⏭️ 群监听去重跳过：该媒体已下载过 chat_id={chat_id}")
        return

    # 添加到任务表（创建时即写入媒体ID）
    tid = add_task(jid, msg.id, chat_id, chat_name, file_name, file_size, _media_id, str(chat_id))

    # 通知用户有新文件正在下载
    try:
        manager = getattr(handle_watch_message, "manager", None)
        if manager:
            await manager.bot_app.bot.send_message(
                job["user_chat_id"],
                f"👁️ <b>群监听新文件</b>\n"
                f"━━━━━━━━━━━━━━━\n"
                f"📋 任务: <code>#{jid}</code>\n"
                f"📁 来源: <b>{html.escape(chat_name)}</b>\n"
                f"📄 文件名: {html.escape(file_name[:50])}\n"
                f"📦 大小: {format_size(file_size)}\n"
                f"📨 消息ID: <code>{msg.id}</code>\n"
                f"━━━━━━━━━━━━━━━\n"
                f"⬇️ 正在自动下载...",
                parse_mode="HTML"
            )
    except: pass

    # 异步下载
    asyncio.create_task(_download_single_file(client, jid, tid, msg.id, chat_id, chat_name))

# ============ 群/频道监听：按需注册（避免无 watch 任务时全局监听每个消息的开销） ============
def _ensure_watch_handler(manager, client):
    """有 watch 任务时才挂载全局 NewMessage 监听（先移除防重，再添加）"""
    if getattr(manager, "_watch_handler_active", False):
        return
    from telethon import events as tg_events
    client.remove_event_handler(handle_watch_message)
    client.add_event_handler(handle_watch_message, tg_events.NewMessage(incoming=True, outgoing=True))
    manager._watch_handler_active = True
    logger.info("📡 群/频道监听已注册（按需）")

def _maybe_remove_watch_handler(manager, client):
    """最后一个 watch 任务删除后注销全局监听，避免无谓的消息回调开销"""
    if not getattr(manager, "_watch_handler_active", False):
        return
    if WATCH_MONITORS:
        return
    client.remove_event_handler(handle_watch_message)
    manager._watch_handler_active = False
    logger.info("📡 群/频道监听已注销（无活动监听任务）")

async def handle_dl_saved(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/dl_saved - 启动收藏夹监听，有新消息自动下载
    统一走 run_job：扫描历史 → 下载历史待下载 → 保持下载中状态（持续监听新消息）
    """
    global SAVED_MONITOR_JID
    manager = getattr(handle_dl_saved, "manager", None) or context.bot_data.get('manager')
    if not manager or not is_admin(update.effective_user.id, manager.config):
        return
    if not manager or not manager.mtproto_client:
        return await update.message.reply_text("❌ MTProto 未就绪，请先执行 /mtlogin 登录")
    
    # 如果已经有监听任务在运行
    if SAVED_MONITOR_JID:
        job = get_job(SAVED_MONITOR_JID)
        if job and job["status"] not in (JOB_CANCELLED, JOB_DONE):
            status = status_text(job["status"])
            return await update.message.reply_html(
                f"⚠️ 收藏夹监听已在运行\n"
                f"任务编号: <code>#{SAVED_MONITOR_JID}</code>\n"
                f"状态: {status}\n"
                f"用 /dl_stop #{SAVED_MONITOR_JID} 暂停，/dl_no #{SAVED_MONITOR_JID} 取消"
            )
    
    # 创建收藏夹监听任务（tag=all 表示下载所有媒体类型）
    jid = create_job("saved", "saved_messages", "all", update.effective_chat.id)
    SAVED_MONITOR_JID = jid
    
    await update.message.reply_html(
        f"⭐ <b>收藏夹监听已启动</b>\n"
        f"━━━━━━━━━━━━━━━\n"
        f"📋 任务编号: <code>#{jid}</code>\n"
        f"🔍 正在扫描收藏夹中未下载的历史消息...\n"
        f"━━━━━━━━━━━━━━━\n"
        f"💡 扫描+下载完成后自动进入持续监听模式\n"
        f"用 /dls #{jid} 查看详情，/dl_stop #{jid} 暂停"
    )
    
    # 统一走 run_job：扫描历史 → 下载 → 保持下载中状态（监听模式）
    if not await manager.mtproto_client.ensure_ready():
        return await update.message.reply_text("❌ MTProto 未就绪（未登录或连接失败），请先 /mtlogin")
    _start_run_job(manager, jid)

async def handle_dl_saved_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/dl_saved_video - 收藏夹只下载视频"""
    global SAVED_MONITOR_JID
    manager = getattr(handle_dl_saved_video, "manager", None) or context.bot_data.get('manager')
    if not manager or not is_admin(update.effective_user.id, manager.config):
        return
    if not manager or not manager.mtproto_client:
        return await update.message.reply_text("❌ MTProto 未就绪，请先执行 /mtlogin 登录")
    
    if SAVED_MONITOR_JID:
        job = get_job(SAVED_MONITOR_JID)
        if job and job["status"] not in (JOB_CANCELLED, JOB_DONE):
            return await update.message.reply_html(
                f"⚠️ 收藏夹监听已在运行\n任务编号: <code>#{SAVED_MONITOR_JID}</code>\n用 /dl_stop #{SAVED_MONITOR_JID} 暂停，/dl_no #{SAVED_MONITOR_JID} 取消"
            )
    
    jid = create_job("saved", "saved_messages", "video", update.effective_chat.id)
    SAVED_MONITOR_JID = jid
    
    await update.message.reply_html(
        f"⭐ <b>收藏夹视频监听已启动</b>\n"
        f"━━━━━━━━━━━━━━━\n"
        f"📋 任务编号: <code>#{jid}</code>\n"
        f"🎬 类型: 只下载视频\n"
        f"🔍 正在扫描收藏夹中未下载的视频...\n"
        f"━━━━━━━━━━━━━━━\n"
        f"💡 扫描+下载完成后自动进入持续监听模式\n"
        f"用 /dls #{jid} 查看详情，/dl_stop #{jid} 暂停"
    )
    
    # 统一走 run_job：扫描历史 → 下载 → 保持下载中状态（监听模式）
    if not await manager.mtproto_client.ensure_ready():
        return await update.message.reply_text("❌ MTProto 未就绪（未登录或连接失败），请先 /mtlogin")
    _start_run_job(manager, jid)

async def handle_dl_saved_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/dl_saved_photo - 收藏夹只下载图片"""
    global SAVED_MONITOR_JID
    manager = getattr(handle_dl_saved_photo, "manager", None) or context.bot_data.get('manager')
    if not manager or not is_admin(update.effective_user.id, manager.config):
        return
    if not manager or not manager.mtproto_client:
        return await update.message.reply_text("❌ MTProto 未就绪，请先执行 /mtlogin 登录")
    
    if SAVED_MONITOR_JID:
        job = get_job(SAVED_MONITOR_JID)
        if job and job["status"] not in (JOB_CANCELLED, JOB_DONE):
            return await update.message.reply_html(
                f"⚠️ 收藏夹监听已在运行\n任务编号: <code>#{SAVED_MONITOR_JID}</code>\n用 /dl_stop #{SAVED_MONITOR_JID} 暂停，/dl_no #{SAVED_MONITOR_JID} 取消"
            )
    
    jid = create_job("saved", "saved_messages", "photo", update.effective_chat.id)
    SAVED_MONITOR_JID = jid
    
    await update.message.reply_html(
        f"⭐ <b>收藏夹图片监听已启动</b>\n"
        f"━━━━━━━━━━━━━━━\n"
        f"📋 任务编号: <code>#{jid}</code>\n"
        f"🖼️ 类型: 只下载图片\n"
        f"🔍 正在扫描收藏夹中未下载的图片...\n"
        f"━━━━━━━━━━━━━━━\n"
        f"💡 扫描+下载完成后自动进入持续监听模式\n"
        f"用 /dls #{jid} 查看详情，/dl_stop #{jid} 暂停"
    )
    
    # 统一走 run_job：扫描历史 → 下载 → 保持下载中状态（监听模式）
    if not await manager.mtproto_client.ensure_ready():
        return await update.message.reply_text("❌ MTProto 未就绪（未登录或连接失败），请先 /mtlogin")
    _start_run_job(manager, jid)

async def _create_watch_job(update, context, media_tag, type_label):
    """创建群/频道监听任务（三个 /dl_watch* 命令共享此逻辑）"""
    manager = getattr(_create_watch_job, "manager", None) or context.bot_data.get('manager')
    if not manager or not is_admin(update.effective_user.id, manager.config):
        return
    if not manager or not manager.mtproto_client:
        return await update.message.reply_text("❌ MTProto 未就绪，请先执行 /mtlogin 登录")
    if not context.args:
        return await update.message.reply_text("💡 用法: /dl_watch [群/频道链接]")

    link = context.args[0].strip()

    # 先解析 entity，拿到 chat_id 和群名（用于查重和显示）
    client = manager.mtproto_client.client
    try:
        entity_key = parse_link(link)
        ent = await client.get_entity(entity_key)
        chat_id = ent.id
        chat_name = getattr(ent, 'title', None) or getattr(ent, 'first_name', None) or str(chat_id)
    except Exception as e:
        return await update.message.reply_text(f"❌ 解析群/频道失败: {e}")

    # 检查同一个群是否已在监听
    if chat_id in WATCH_MONITORS:
        existing_jid = WATCH_MONITORS[chat_id]
        job = get_job(existing_jid)
        if job and job["status"] not in (JOB_CANCELLED, JOB_DONE):
            status = status_text(job["status"])
            return await update.message.reply_html(
                f"⚠️ 该群/频道已在监听\n"
                f"任务编号: <code>#{existing_jid}</code>\n"
                f"状态: {status}\n"
                f"用 /dl_stop #{existing_jid} 暂停，/dl_no #{existing_jid} 取消"
            )

    # 创建监听任务并注册到 WATCH_MONITORS
    jid = create_job("watch", link, media_tag, update.effective_chat.id)
    WATCH_MONITORS[chat_id] = jid
    _ensure_watch_handler(manager, client)

    type_icon = "👁️" if media_tag == "all" else ("🎬" if media_tag == "video" else "🖼️")
    scan_desc = "所有媒体" if media_tag == "all" else type_label

    await update.message.reply_html(
        f"{type_icon} <b>群/频道监听已启动</b>\n"
        f"━━━━━━━━━━━━━━━\n"
        f"📋 任务编号: <code>#{jid}</code>\n"
        f"📁 群/频道: <b>{chat_name}</b>\n"
        f"🎯 类型: {scan_desc}\n"
        f"🔍 正在扫描历史消息中未下载的文件...\n"
        f"━━━━━━━━━━━━━━━\n"
        f"💡 扫描+下载完成后自动进入持续监听模式\n"
        f"用 /dls #{jid} 查看详情，/dl_stop #{jid} 暂停"
    )

    # 统一走 run_job：扫描历史 → 下载 → 保持下载中状态（监听模式）
    if not await manager.mtproto_client.ensure_ready():
        return await update.message.reply_text("❌ MTProto 未就绪（未登录或连接失败），请先 /mtlogin")
    _start_run_job(manager, jid)

async def handle_dl_watch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/dl_watch [链接] - 持续监听群/频道（所有类型），新消息自动下载"""
    await _create_watch_job(update, context, "all", "所有类型")

async def handle_dl_watch_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/dl_watch_video [链接] - 持续监听群/频道，只下载视频"""
    await _create_watch_job(update, context, "video", "只下载视频")

async def handle_dl_watch_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/dl_watch_photo [链接] - 持续监听群/频道，只下载图片"""
    await _create_watch_job(update, context, "photo", "只下载图片")

async def handle_dls(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/dls [#xxx] - 列出所有任务，或查看指定任务详情"""
    manager = getattr(handle_dls, "manager", None) or context.bot_data.get('manager')
    if not manager or not is_admin(update.effective_user.id, manager.config):
        return
    # 查看指定任务详情
    if context.args and context.args[0].startswith("#"):
        try:
            jid = int(context.args[0].lstrip("#"))
        except:
            return await update.message.reply_text("⚠️ 任务编号格式错误，用法: /dls #123")
        
        job = get_job(jid)
        if not job:
            return await update.message.reply_text(f"⚠️ 任务 #{jid} 不存在")
        
        stats = get_job_stats(jid)
        rt = job_runtime.get(jid, {})
        
        lines = [
            f"📋 <b>任务 #{jid} 详情</b>",
            f"━━━━━━━━━━━━━━━",
            f"类型: {type_text(job['type'])}",
            f"来源: {job['source']}",
            f"筛选: {job['tag']}",
            f"状态: {status_text(job['status'])}",
            f"创建时间: {job['created_at']}",
            f"━━━━━━━━━━━━━━━",
            f"📊 <b>文件统计:</b>",
            f"  总计: {stats['total']}",
            f"  ✅ 已完成: {stats['done']}",
            f"  ⏳ 待下载: {stats['pending']}",
            f"  ❌ 失败: {stats['failed']}",
            f"  ⚠️ 已删除: {stats.get('skipped', 0)}",
        ]
        
        # 下载速度信息（仅下载中）
        if job["status"] == JOB_DOWNLOADING and rt:
            speed = rt.get("speed", 0)
            current_file = rt.get("current_file", "")
            downloaded = rt.get("downloaded", 0)
            lines.extend([
                f"━━━━━━━━━━━━━━━",
                f"⚡ <b>实时下载:</b>",
                f"  当前文件: {current_file[:40]}",
                f"  下载速度: {format_speed(speed)}",
                f"  已下载: {format_size(downloaded)}",
            ])
        
        # 最近10个文件
        conn = _db_conn()
        recent = conn.execute("SELECT file_name, status FROM tasks WHERE jid = ? ORDER BY tid DESC LIMIT 10", (jid,)).fetchall()
        conn.close()
        if recent:
            lines.append(f"━━━━━━━━━━━━━━━")
            lines.append(f"📁 <b>最近文件:</b>")
            for fname, fstatus in recent:
                icon = {TASK_PENDING: "⏳", TASK_DOWNLOADING: "⬇️", TASK_DONE: "✅", TASK_FAILED: "❌"}.get(fstatus, "?")
                lines.append(f"  {icon} {fname[:50]}")
        
        return await update.message.reply_text("\n".join(lines), parse_mode="HTML")
    
    # 列出所有任务
    jobs = list_jobs()
    if not jobs:
        return await update.message.reply_text("📭 当前没有下载任务")
    
    lines = ["📑 <b>所有下载任务</b>\n━━━━━━━━━━━━━━━"]
    for job in jobs:
        stats = get_job_stats(job["jid"])
        rt = job_runtime.get(job["jid"], {})
        speed_info = f" | {format_speed(rt.get('speed',0))}" if job["status"] == JOB_DOWNLOADING and rt.get("speed",0) > 0 else ""
        lines.append(
            f"<code>#{job['jid']}</code> | {type_text(job['type'])} | {status_text(job['status'])} | "
            f"{stats['done']}/{stats['total']}{speed_info}\n"
            f"  └ {job['source'][:50]} | 筛选: {job['tag'][:30]}"
        )
    
    lines.append("\n💡 用 /dls #编号 查看任务详情和下载速度")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")

async def handle_dl_control(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """统一处理 /dl_stop /dl_continue /dl_no，基于任务编号"""
    global SAVED_MONITOR_JID
    manager = getattr(handle_dl_control, "manager", None) or context.bot_data.get('manager')
    if not manager or not is_admin(update.effective_user.id, manager.config):
        return
    try:
        logger.info(f"📥 控制命令被调用: {update.effective_message.text}, args={context.args}")
        
        if not context.args:
            return await update.message.reply_text("💡 用法: /dl_stop #编号 | /dl_continue #编号 | /dl_no #编号")
        
        jid_str = context.args[0].lstrip("#")
        try:
            jid = int(jid_str)
        except:
            return await update.message.reply_text("⚠️ 任务编号格式错误")
        
        job = get_job(jid)
        if not job:
            return await update.message.reply_text(f"⚠️ 任务 #{jid} 不存在")
        
        manager = getattr(handle_dl_control, "manager", None) or context.bot_data.get('manager')
        cmd = update.effective_message.text.lower()
        is_saved_monitor = (job["type"] == "saved")
        
        if "stop" in cmd:
            update_job_status(jid, JOB_PAUSED)
            job_runtime.pop(jid, None)
            logger.info(f"⏸ 任务 #{jid} 已停止（用户命令）")
            msg = "⏸ 收藏夹监听已暂停，新消息不会自动下载" if is_saved_monitor else f"⏸ 任务 #{jid} 已停止"
            await update.message.reply_text(f"{msg}\n用 /dl_continue #{jid} 继续")
        
        elif "continue" in cmd:
            if job["status"] == JOB_DONE:
                return await update.message.reply_text(f"⚠️ 任务 #{jid} 已完成，无需继续")
            if not manager or not manager.mtproto_client:
                return await update.message.reply_text("❌ MTProto 未就绪")
            if not await manager.mtproto_client.ensure_ready():
                return await update.message.reply_text("❌ MTProto 未就绪（未登录或连接失败），请先 /mtlogin")
            
            # 统一逻辑：所有任务类型都走 run_job（扫描→下载→收藏夹/群监听保持监听）
            if is_saved_monitor:
                SAVED_MONITOR_JID = jid  # 恢复收藏夹监听引用
            if job["type"] == "watch":
                # 群监听：重新解析 entity 并注册到 WATCH_MONITORS
                asyncio.create_task(_register_watch_monitor(manager, jid))
            
            update_job_status(jid, JOB_PENDING)  # 设为待扫描，run_job 会从断点继续
            logger.info(f"▶️ 任务 #{jid} 已恢复，开始继续（用户命令）")
            await update.message.reply_text(f"▶️ 任务 #{jid} 已恢复，开始继续扫描和下载...")
            _start_run_job(manager, jid)
        
        elif "no" in cmd:
            if is_saved_monitor and SAVED_MONITOR_JID == jid:
                SAVED_MONITOR_JID = None
            # 群监听：从 WATCH_MONITORS 移除
            if job["type"] == "watch":
                for cid, j in list(WATCH_MONITORS.items()):
                    if j == jid:
                        del WATCH_MONITORS[cid]
                        logger.info(f"📡 群监听已注销: chat_id={cid}")
                # 若已无任何监听任务，注销全局监听（避免无谓回调开销）
                if manager and manager.mtproto_client and manager.mtproto_client.client:
                    _maybe_remove_watch_handler(manager, manager.mtproto_client.client)
            delete_job(jid)
            logger.info(f"⏹ 任务 #{jid} 已取消并删除（用户命令）")
            await update.message.reply_text(f"⏹ 任务 #{jid} 已取消并删除")
    
    except Exception as e:
        logger.error(f"❌ 控制命令处理失败: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ 命令处理失败: {e}")
        except:
            pass

async def handle_dl_clear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/dl_clear - 清理所有已完成的任务记录"""
    manager = getattr(handle_dl_clear, "manager", None) or context.bot_data.get('manager')
    if not manager or not is_admin(update.effective_user.id, manager.config):
        return
    
    # 统计已完成的任务
    conn = _db_conn()
    done_jobs = conn.execute("SELECT jid FROM jobs WHERE status = ?", (JOB_DONE,)).fetchall()
    cancelled_jobs = conn.execute("SELECT jid FROM jobs WHERE status = ?", (JOB_CANCELLED,)).fetchall()
    conn.close()
    
    to_delete = [j[0] for j in done_jobs] + [j[0] for j in cancelled_jobs]
    
    if not to_delete:
        return await update.message.reply_text("📭 没有已完成或已取消的任务需要清理")
    
    # 删除任务及其文件记录
    for jid in to_delete:
        delete_job(jid)
    
    await update.message.reply_text(
        f"🧹 <b>已清理任务记录</b>\n"
        f"━━━━━━━━━━━━━━━\n"
        f"✅ 已完成: {len(done_jobs)} 个\n"
        f"❌ 已取消: {len(cancelled_jobs)} 个\n"
        f"━━━━━━━━━━━━━━━\n"
        f"共删除 {len(to_delete)} 个任务记录",
        parse_mode="HTML"
    )

# ===================== 9. 自动下载接口（供 at_downloader 调用） =====================
def create_auto_job(user_chat_id, source_name="转发自动下载"):
    """创建自动下载任务，返回 jid"""
    return create_job("auto", source_name, "auto", user_chat_id)

def add_auto_task(jid, msg_id, chat_id, chat_name, file_name="", file_size=0, document_id=None, source_id=None):
    """添加自动下载文件记录（创建时即写入媒体ID）"""
    return add_task(jid, msg_id, chat_id, chat_name, file_name, file_size, document_id, source_id)

async def run_auto_job(manager, jid):
    """执行自动下载任务"""
    _start_run_job(manager, jid)


# ===================== 9.5 Web 下载请求消费（look 插件推送的下载请求） =====================
# 💡 look 插件只把下载请求写进 web_requests 表（type='download'），不执行下载；
#    本插件（downloader）每 5 秒消费并加入统一引擎（与 /dl 同一条链路）。
#    表不存在（未安装 look 插件）时静默跳过——插件可单独安装。
REQ_PENDING = 0
REQ_RUNNING = 1
REQ_DONE = 2
REQ_FAILED = 3


def _web_requests_table_exists():
    """web_requests 表在 download/look.db 里（look 插件所有），本插件只读不建（尊重插件独立性）"""
    conn = _look_db_conn()
    if conn is None:
        return False
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='web_requests'"
        ).fetchone()
        return row is not None
    finally:
        conn.close()


async def _consume_web_download_requests(manager):
    """消费 download/look.db 中 web_requests 表 type='download' 的待处理请求，加入统一引擎下载"""
    if not _web_requests_table_exists():
        return
    try:
        conn = _look_db_conn()
        if conn is None:
            return
        try:
            row = conn.execute(
                "SELECT id, chat_id, chat_name, msg_id, file_name, document_id, file_size "
                "FROM web_requests WHERE type='download' AND status=? ORDER BY id ASC LIMIT 1",
                (REQ_PENDING,),
            ).fetchone()
        finally:
            conn.close()
        if not row:
            return
        req_id, chat_id, chat_name, msg_id, file_name, document_id, file_size = row

        # 标记处理中（防止重复消费）
        conn = _look_db_conn()
        if conn is None:
            return
        try:
            conn.execute(
                "UPDATE web_requests SET status=?, updated_at=? WHERE id=?",
                (REQ_RUNNING, _now(), req_id),
            )
            conn.commit()
        finally:
            conn.close()

        try:
            # 复用统一引擎：建任务 → 登记文件 → 启动执行（下载任务仍写 download_tasks.db）
            # source=chat_id（实体标识，run_job 用它找群）；tag=群名（备注）；user_chat_id=管理员（通知对象）
            from core.download_engine import SAVED_USER_ID
            jid = create_job("web", str(chat_id), chat_name or f"群{chat_id}", SAVED_USER_ID)
            add_task(
                jid, msg_id, chat_id, chat_name or f"群{chat_id}",
                file_name or str(msg_id), file_size or 0, document_id, str(chat_id),
            )
            _start_run_job(manager, jid)
            conn = _look_db_conn()
            if conn:
                try:
                    conn.execute(
                        "UPDATE web_requests SET status=?, result=?, updated_at=? WHERE id=?",
                        (REQ_DONE, f"已加入下载队列 (任务 #{jid})", _now(), req_id),
                    )
                    conn.commit()
                finally:
                    conn.close()
            logger.info(f"🌐 Web 下载请求 #{req_id} 已入队 → 任务 #{jid} (群 {chat_id} msg {msg_id})")
        except Exception as e:
            logger.error(f"❌ Web 下载请求处理失败 req_id={req_id}: {e}")
            conn = _look_db_conn()
            if conn:
                try:
                    conn.execute(
                        "UPDATE web_requests SET status=?, result=?, updated_at=? WHERE id=?",
                        (REQ_FAILED, f"下载失败: {e}", _now(), req_id),
                    )
                    conn.commit()
                finally:
                    conn.close()
    except Exception as e:
        logger.error(f"❌ Web 下载请求消费循环异常: {e}")


# ===================== 10. 注册入口 =====================
def register(manager):
    init_db()
    
    # 注册停机回调（取消下载任务）【S3热重载幂等：防止重复累积】
    if shutdown_downloads not in manager.shutdown_callbacks:
        manager.shutdown_callbacks.append(shutdown_downloads)
    
    # 绑定 manager
    handle_dl_command.manager = manager
    handle_dl_video.manager = manager
    handle_dl_photo.manager = manager
    handle_dl_saved.manager = manager
    handle_dl_saved_video.manager = manager
    handle_dl_saved_photo.manager = manager
    handle_dl_watch.manager = manager
    handle_dl_watch_video.manager = manager
    handle_dl_watch_photo.manager = manager
    _create_watch_job.manager = manager
    handle_dls.manager = manager
    handle_dl_control.manager = manager
    handle_dl_clear.manager = manager
    handle_saved_message.manager = manager
    handle_watch_message.manager = manager
    
    # 注册命令
    register_handler(CommandHandler("dl", handle_dl_command), __name__)
    register_handler(CommandHandler("dl_video", handle_dl_video), __name__)
    register_handler(CommandHandler("dl_photo", handle_dl_photo), __name__)
    register_handler(CommandHandler("dl_saved", handle_dl_saved), __name__)
    register_handler(CommandHandler("dl_saved_video", handle_dl_saved_video), __name__)
    register_handler(CommandHandler("dl_saved_photo", handle_dl_saved_photo), __name__)
    register_handler(CommandHandler("dl_watch", handle_dl_watch), __name__)
    register_handler(CommandHandler("dl_watch_video", handle_dl_watch_video), __name__)
    register_handler(CommandHandler("dl_watch_photo", handle_dl_watch_photo), __name__)
    register_handler(CommandHandler("dls", handle_dls), __name__)
    register_handler(CommandHandler("dl_stop", handle_dl_control), __name__)
    register_handler(CommandHandler("dl_continue", handle_dl_control), __name__)
    register_handler(CommandHandler("dl_no", handle_dl_control), __name__)
    register_handler(CommandHandler("dl_clear", handle_dl_clear), __name__)
    
    # 注册收藏夹监听事件（需要 MTProto 客户端就绪）
    if manager.mtproto_client and manager.mtproto_client.client:
        try:
            client = manager.mtproto_client.client
            # 获取用户自己的 ID（用于判断收藏夹消息）
            async def _init_saved_monitor():
                await asyncio.sleep(2)  # 等连接稳定
                try:
                    me = await client.get_me()
                    _engine.SAVED_USER_ID = me.id
                    logger.info(f"👤 收藏夹监听用户ID: {_engine.SAVED_USER_ID}")
                except Exception as e:
                    logger.warning(f"⚠️ 获取用户ID失败，收藏夹监听暂不可用: {e}")
            
            # 物理防重：先移除再添加
            # 明确监听 incoming + outgoing 所有消息，只监听收藏夹(me)
            from telethon import events as tg_events
            client.remove_event_handler(handle_saved_message)
            client.add_event_handler(handle_saved_message, tg_events.NewMessage(incoming=True, outgoing=True, chats=['me']))
            asyncio.create_task(_init_saved_monitor())
            logger.info("📡 收藏夹监听已注册")

            # 群/频道监听改为“按需注册”：仅在存在 watch 任务时挂载全局监听，
            # 见 _ensure_watch_handler / _maybe_remove_watch_handler，避免无谓的消息回调开销
        except Exception as e:
            logger.error(f"❌ 注册收藏夹监听失败: {e}")
    
    # 启动网络监控后台任务（断网时检测恢复）
    global _network_monitor_started, _network_monitor_task
    if not _network_monitor_started:
        _network_monitor_started = True
        _network_monitor_task = asyncio.create_task(_network_monitor(manager))
    
    # Web 下载请求消费（look 插件推送的下载请求，每 5 秒轮询；表不存在时自动跳过）
    try:
        job_queue = manager.bot_app.job_queue
        if job_queue is not None:
            async def _web_dl_loop(context):
                m = context.bot_data.get("manager")
                if m:
                    await _consume_web_download_requests(m)
            job_queue.run_repeating(_web_dl_loop, interval=5.0, first=5.0)
            logger.info("🌐 Web 下载请求消费已挂载（每 5 秒轮询 look 推送的下载请求）")
    except Exception as e:
        logger.error(f"❌ Web 下载请求消费挂载失败: {e}")
    
    # 启动时恢复未完成的任务（延迟3秒）【S3热重载幂等：只调度一次，避免每次reload重复扫描】
    if not getattr(manager, "_resume_scheduled", False):
        manager._resume_scheduled = True
        async def _delayed_resume():
            await asyncio.sleep(3)
            await resume_jobs(manager)
        asyncio.create_task(_delayed_resume())
    
    logger.info(f"✅ [{__MODULE_NAME__}] V3.0 已就绪（统一引擎+数据库驱动+速度统计+收藏夹监听）")
