#openbot\features\downloader\at_downloader.py
"""
转发自动下载引擎 V3.0
- 调用统一下载引擎（mt_downloader）的接口
- 所有任务存入统一数据库，支持 /dls 查看、暂停、继续、取消
- 3秒批次判定：连续转发的消息归为同一任务
"""
import logging
import os
import asyncio
import time
import html
from telethon import events, types
from telegram import Update
from telegram.ext import ContextTypes, CommandHandler
from core.command_registry import register_handler
from core.utils import is_admin
from features.downloader.mt_downloader import (
    create_auto_job, add_auto_task, run_auto_job, format_size,
    _extract_media_id, is_media_downloaded
)

logger = logging.getLogger(__name__)

__MODULE_NAME__ = "转发自动下载引擎"

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DOWNLOAD_DIR = os.path.join(BASE_DIR, "download")

# 用户批次会话：user_id -> {"jid": int, "last_msg_time": float, "count": int}
# 【S3热重载保护】reload 时保留，避免进行中的转发批次会话丢失
if "USER_BATCH_SESSIONS" not in globals():
    USER_BATCH_SESSIONS = {}

# Bot 的 ID（用于判断消息是否发送给 bot）
# 【S3热重载保护】reload 时保留，避免重复获取
if "BOT_ID" not in globals():
    BOT_ID = None

# ===================== MTProto 下载逻辑（调用统一引擎） =====================
async def mtproto_download_logic(client, message, jid):
    """下载单条消息，通过统一引擎的数据库记录"""
    msg_id = message.id
    chat_id = message.chat_id
    
    # 解析来源信息
    source_id = "Unknown"
    chat_name = "Direct_Transfer"
    file_name = f"{msg_id}"
    file_size = 0
    
    if message.forward:
        if message.forward.chat:
            source_id = str(message.forward.chat_id)
            # chat_id 保持 message.chat_id（bot 私聊 ID），存储逻辑不变；
            # 群名 chat_name 记转发来源群 title，来源类型由 jobs.source 记录（"转发自动下载"）
            chat_name = getattr(message.forward.chat, 'title', 'Channel')
        elif message.forward.sender:
            source_id = str(message.forward.sender_id)
            chat_name = f"User_{source_id}"
    else:
        source_id = str(message.chat_id)
        try:
            ent = await client.get_entity(message.chat_id)
            chat_name = getattr(ent, 'title', getattr(ent, 'first_name', 'Private'))
        except: pass
    
    # 提取文件名和大小
    if isinstance(message.media, types.MessageMediaDocument):
        file_size = message.media.document.size or 0
        for a in message.media.document.attributes:
            if isinstance(a, types.DocumentAttributeFilename):
                file_name = a.file_name
    elif isinstance(message.media, types.MessageMediaPhoto):
        file_name = f"photo_{msg_id}.jpg"
    
    # 添加到统一数据库（创建时即写入媒体ID；source_id=转发来源群/用户ID）
    add_auto_task(jid, msg_id, chat_id, chat_name, file_name, file_size, _extract_media_id(message.media), source_id)

# ===================== MTProto 底层监听 (批次判定) =====================
async def mt_on_new_message(event):
    global BOT_ID
    # 只处理发送给 bot 的私聊消息（排除收藏夹和其他私聊）
    if not event.is_private: return
    
    # 延迟获取 BOT_ID（register 时 Bot 可能还没初始化完）
    if not BOT_ID:
        try:
            manager = mt_on_new_message.manager
            BOT_ID = manager.bot_app.bot.id
            logger.info(f"🤖 Bot ID: {BOT_ID}，只监听发送给 bot 的私聊消息")
        except Exception as e:
            logger.warning(f"⚠️ 获取 Bot ID 失败: {e}，跳过本条消息")
            return
    
    if event.chat_id != BOT_ID: return
    manager = mt_on_new_message.manager
    if not is_admin(event.sender_id, manager.config): return
    if not event.message.media: return
    
    # 只处理真正的文档和图片，过滤掉链接预览(MessageMediaWebPage)、投票、位置等不可下载媒体
    if not isinstance(event.message.media, (types.MessageMediaDocument, types.MessageMediaPhoto)):
        return

    # 🚨 全局去重：该媒体（document.id/photo.id）已下载/下载中 → 跳过（不创建任务、不发通知）
    _media_id = _extract_media_id(event.message.media)
    if _media_id and is_media_downloaded(_media_id):
        logger.info(f"⏭️ 转发自动下载去重跳过: media_id={_media_id}")
        return

    user_id = event.sender_id
    now = time.time()
    
    # 3 秒批次判定逻辑
    session = USER_BATCH_SESSIONS.get(user_id)
    if session and (now - session["last_msg_time"] < 3.0):
        # 同批次，复用 jid
        jid = session["jid"]
        session["last_msg_time"] = now
        session["count"] += 1
    else:
        # 新批次，创建新任务
        jid = create_auto_job(user_id, "转发自动下载")
        USER_BATCH_SESSIONS[user_id] = {"jid": jid, "last_msg_time": now, "count": 1}
        
        # 解析转发来源信息
        msg = event.message
        source_name = "未知"
        source_id = "未知"
        source_link = "未知"
        file_name = "未知"
        file_id = "未知"
        file_size = 0
        
        if msg.forward:
            if msg.forward.chat:
                source_id = str(msg.forward.chat_id)
                source_name = getattr(msg.forward.chat, 'title', '未知频道')
                username = getattr(msg.forward.chat, 'username', None)
                if username:
                    source_link = f"https://t.me/{username}"
                else:
                    raw_id = source_id.replace('-100', '') if source_id.startswith('-100') else source_id
                    source_link = f"https://t.me/c/{raw_id}"
            elif msg.forward.sender:
                source_id = str(msg.forward.sender_id)
                source_name = getattr(msg.forward.sender, 'first_name', '未知用户')
                username = getattr(msg.forward.sender, 'username', None)
                source_link = f"https://t.me/{username}" if username else "私聊"
        
        # 解析文件信息
        msg_id_in_chat = "未知"  # 原群中的消息ID
        if msg.forward and hasattr(msg.forward, 'channel_post') and msg.forward.channel_post:
            msg_id_in_chat = str(msg.forward.channel_post)
        
        if isinstance(msg.media, types.MessageMediaDocument):
            doc = msg.media.document
            file_id = str(doc.id)
            file_size = doc.size or 0
            for a in doc.attributes:
                if isinstance(a, types.DocumentAttributeFilename):
                    file_name = a.file_name
        elif isinstance(msg.media, types.MessageMediaPhoto):
            file_id = str(msg.media.photo.id)
            file_name = f"photo_{msg.id}.jpg"
        
        # 复用统一下载引擎的 format_size，删除本文件的重复 fmt_size
        # 通知用户新任务已创建（详细信息）
        try:
            await manager.bot_app.bot.send_message(
                user_id,
                f"📥 <b>自动下载任务已创建</b>\n"
                f"━━━━━━━━━━━━━━━\n"
                f"📋 任务编号: <code>#{jid}</code>\n"
                f"📁 来源群: <b>{html.escape(source_name)}</b>\n"
                f"🆔 群ID: <code>{html.escape(source_id)}</code>\n"
                f"🔗 群链接: {html.escape(source_link)}\n"
                f"📨 群消息ID: <code>{html.escape(msg_id_in_chat)}</code>\n"
                f"📄 文件名: {html.escape(file_name[:50])}\n"
                f"📦 文件大小: {format_size(file_size)}\n"
                f"━━━━━━━━━━━━━━━\n"
                f"💡 3秒内连续转发的消息归为同一任务\n"
                f"用 /dls #{jid} 查看详情和下载速度",
                parse_mode="HTML"
            )
        except: pass
        
        # 延迟启动下载（等3秒批次结束）
        async def _delayed_start():
            await asyncio.sleep(4)  # 等3秒批次窗口 + 1秒缓冲
            if not await manager.mtproto_client.ensure_ready():
                return
            await run_auto_job(manager, jid)
        asyncio.create_task(_delayed_start())
    
    # 添加文件记录到数据库
    await mtproto_download_logic(event.client, event.message, jid)

# ===================== 状态指令 =====================
async def handle_at_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/at - 查看自动下载引擎状态"""
    # 权限校验：仅管理员
    manager = context.bot_data.get('manager')
    if not manager or not is_admin(update.effective_user.id, manager.config):
        return
    # 统计自动下载任务
    from features.downloader.mt_downloader import list_jobs, JOB_DONE
    auto_jobs = [j for j in list_jobs() if j["type"] == "auto"]
    total = len(auto_jobs)
    done = sum(1 for j in auto_jobs if j["status"] == JOB_DONE)
    active = total - done
    
    await update.effective_message.reply_text(
        "🛡️ <b>转发自动下载引擎</b>\n"
        "━━━━━━━━━━━━━━━\n"
        "● 状态: 🟢 运行中\n"
        "● 模式: 转发消息自动下载\n"
        "● 批次窗口: 3秒\n"
        "● 持久化: ✅ 统一数据库\n"
        "━━━━━━━━━━━━━━━\n"
        f"📊 <b>任务统计:</b>\n"
        f"  总计: {total}\n"
        f"  进行中: {active}\n"
        f"  已完成: {done}\n\n"
        "💡 转发消息给机器人即可自动下载\n"
        "用 /dls 查看所有任务，/dls #编号 查看详情",
        parse_mode="HTML"
    )

# ===================== 注册入口 (优雅降级) =====================
def register(manager):
    # BOT_ID 延迟到收到第一条消息时获取（register 时 Bot 可能还没初始化完）
    
    # 1. 先注册 /at 状态指令
    register_handler(CommandHandler("at", handle_at_status), __name__)

    # 2. MTProto 事件监听需要客户端就绪
    if not manager.mtproto_client or not manager.mtproto_client.client:
        logger.warning(f"⚠️ [{__MODULE_NAME__}] MTProto 未就绪，自动下载监听暂不可用")
        return

    try:
        client = manager.mtproto_client.client
        mt_on_new_message.manager = manager
        
        # 物理防重
        client.remove_event_handler(mt_on_new_message)
        client.add_event_handler(mt_on_new_message, events.NewMessage)
        
        logger.info(f"✅ [{__MODULE_NAME__}] 已就绪（调用统一下载引擎）")
        
    except Exception as e:
        logger.error(f"❌ [{__MODULE_NAME__}] 事件监听注册失败: {e}")
