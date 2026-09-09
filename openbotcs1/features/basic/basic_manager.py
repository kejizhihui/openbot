#openbot\features\basic\basic_manager.py
import time
import logging
import html
import platform
import shutil
import os
import sys
import asyncio
import traceback
import subprocess
import re
from telegram import Update
from telegram.ext import ContextTypes, CommandHandler
from core.command_registry import register_handler
from core.utils import is_admin

logger = logging.getLogger(__name__)

# 💡 必须定义，用于扫描器表格左侧显示
__MODULE_NAME__ = "基础命令"

# ===================== 1. 业务处理器 =====================

async def handle_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/start - 入口欢迎"""
    user_name = update.effective_user.full_name or "用户"
    await update.effective_message.reply_text(f"🎉 欢迎 {user_name}！\n使用 /help 查看手册。")

async def handle_ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/ping - 响应测试"""
    start_time = time.time()
    try:
        sent = await update.effective_message.reply_text("🏓 Pong!")
        ms = (time.time() - start_time) * 1000
        await sent.edit_text(f"🏓 Pong!\n响应时间：{ms:.2f} ms")
    except Exception as e:
        logger.error(f"Ping 回复失败: {e}")

async def handle_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/id [链接/ID/空] - 智能身份与实体解析器 (整合原插件1/2所有字段)"""
    manager = getattr(handle_id, "manager", None) or context.bot_data.get('manager')
    msg = update.effective_message
    user = update.effective_user
    chat = update.effective_chat
    
    # --- 场景 A: 无参数 - 完整复刻原 idd 逻辑 (包含语言和类型) ---
    if not context.args:
        # 昵称/标题等用户可控文本必须 HTML 转义，否则含 < > & 会解析报错或破坏格式
        safe_name = html.escape(user.full_name or "用户")
        report = (
            f"🆔 <b>详细 ID 信息报告</b>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"👤 <b>用户信息:</b>\n"
            f"• 姓名: <code>{safe_name}</code>\n"
            f"• UID: <code>{user.id}</code>\n"
            f"• 语言: <code>{html.escape(user.language_code or '未知')}</code>\n\n"
            f"📢 <b>聊天信息:</b>\n"
            f"• 标题: <code>{html.escape(chat.title or '私聊')}</code>\n"
            f"• CID: <code>{chat.id}</code>\n"
            f"• 类型: <code>{html.escape(chat.type)}</code>\n\n"
            f"💡 <i>提示：输入 /id [链接/ID] 可进行跨频道探测</i>"
        )
        return await msg.reply_text(report, parse_mode="HTML")

    # --- 场景 B: 有参数 - 跨频道深度探测 (MTProto 逻辑) ---
    target = context.args[0].strip()
    if not manager or not manager.mtproto_client:
        return await msg.reply_text("❌ MTProto 客户端未就绪，无法进行深度解析")

    wait_msg = await msg.reply_text("🔍 正在检索远程实体信息...")
    
    try:
        client = manager.mtproto_client.client
        if not await manager.mtproto_client.ensure_ready():
            await wait_msg.edit_text("❌ <b>MTProto 未就绪</b>\n未登录或连接失败，请先 /mtlogin。", parse_mode="HTML")
            return

        # 智能识别数字 ID (含负号) 或 字符串链接
        search_param = int(target) if re.match(r'^-?\d+$', target) else target
        
        # MTProto 核心探测
        entity = await client.get_entity(search_param)
        
        # 属性提取与 ID 救助
        raw_id = entity.id
        # 针对频道/超级群组补全 -100 前缀，确保可直接用于下载指令
        is_chan = hasattr(entity, 'broadcast') or hasattr(entity, 'megagroup')
        final_id = f"-100{raw_id}" if is_chan and not str(raw_id).startswith("-100") else str(raw_id)
        
        title = getattr(entity, 'title', None) or f"{getattr(entity, 'first_name', '')} {getattr(entity, 'last_name', '')}".strip()
        username = f"@{entity.username}" if getattr(entity, 'username', None) else "无"
        f_auth = "❌ 禁转/限存" if getattr(entity, 'noforwards', False) else "✅ 允许转发"
        dc_id = getattr(entity.photo, 'dc_id', '未知') if hasattr(entity, 'photo') and entity.photo else 'N/A'

        res_text = (
            f"💎 <b>远程实体解析结果</b>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"• 标题: <b>{html.escape(title)}</b>\n"
            f"• 🆔 ID: <code>{final_id}</code>\n"
            f"• 用户名: {html.escape(username)}\n"
            f"• 转发: {f_auth}\n"
            f"• 分区: <code>DC {dc_id}</code>\n\n"
            f"💡 <i>提示：点击 ID 即可自动复制</i>"
        )
        await wait_msg.edit_text(res_text, parse_mode="HTML")

    except Exception as e:
        logger.error(f"ID 解析失败: {traceback.format_exc()}")
        await wait_msg.edit_text(f"❌ <b>解析失败</b>\n原因: <code>{str(e)}</code>", parse_mode="HTML")

async def handle_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/status - 系统运行状态"""
    manager = getattr(handle_status, "manager", None) or context.bot_data.get('manager')
    if not manager or not is_admin(update.effective_user.id, manager.config): return
    
    mt_status = "❌ 离线"
    if manager.mtproto_client:
        try:
            is_auth = await manager.mtproto_client.is_authorized()
            mt_status = "✅ 就绪" if is_auth else "🔑 待登录"
        except: mt_status = "⚠️ 异常"

    msg = (f"🖥️ <b>系统状态报告</b>\n"
           f"━━━━━━━━━━━━━━━\n"
           f"• Python: <code>{platform.python_version()}</code>\n"
           f"• MTProto: {mt_status}\n"
           f"• 系统: <code>{platform.system()} {platform.release()}</code>")
    await update.message.reply_text(msg, parse_mode="HTML")

async def handle_disk(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/disk - 磁盘监控 (完整版)"""
    manager = getattr(handle_disk, "manager", None) or context.bot_data.get('manager')
    if not manager or not is_admin(update.effective_user.id, manager.config): return
    
    path = context.args[0] if context.args else "."
    try:
        total, used, free = shutil.disk_usage(path)
        msg = (f"💾 <b>磁盘监控:</b> <code>{os.path.abspath(path)}</code>\n"
               f"━━━━━━━━━━━━━━━\n"
               f"• 总总量: {total // (2**30)} GB\n"
               f"• 已使用: {used // (2**30)} GB\n"
               f"• 剩余量: {free // (2**30)} GB\n"
               f"• 使用率: <b>{(used/total)*100:.1f}%</b>")
        await update.message.reply_text(msg, parse_mode="HTML")
    except Exception as e:
        await update.message.reply_text(f"❌ 查询失败: {e}")

async def handle_python(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/python - 远程运维 (Windows 兼容修复版)"""
    manager = getattr(handle_python, "manager", None) or context.bot_data.get('manager')
    if not manager or not is_admin(update.effective_user.id, manager.config): return

    if not context.args:
        py_info = (f"🐍 <b>远程终端</b>\n• Python: <code>{sys.version.split()[0]}</code>\n\n"
                   f"💡 <b>用法:</b> 直接输入任意命令\n"
                   f"• <code>/python pip install requests</code>\n"
                   f"• <code>/python python --version</code>\n"
                   f"• <code>/python python -m pip list</code>\n\n"
                   f"⚠️ 不支持管道符 <code>|</code>，请用 <code>python -m ...</code> 形式")
        await update.message.reply_text(py_info, parse_mode="HTML")
        return

    wait_msg = await update.message.reply_text("⏳ 正在执行子进程...")
    try:
        # S4修复：直接执行管理员输入的命令（远程终端）
        # 兼容 /python pip install xxx、/python python --version 等任意命令
        # 不用 shell=True，用列表形式传参，避免命令注入
        cmd_list = list(context.args)
        # 超时保护：命令最长运行 120 秒，防止超长/挂起命令永久阻塞线程
        RUN_TIMEOUT = 120
        def run_sync():
            return subprocess.run(cmd_list, shell=False, capture_output=True, text=False, timeout=RUN_TIMEOUT)

        # 用 to_thread 替代废弃的 asyncio.get_event_loop() + run_in_executor
        result = await asyncio.to_thread(run_sync)

        def decode_msg(data):
            if not data: return "(无输出)"
            try: return data.decode('utf-8').strip()
            except: return data.decode('gbk', errors='ignore').strip()

        res_icon = "✅" if result.returncode == 0 else "❌"
        report = (f"{res_icon} <b>执行结果 ({result.returncode})</b>\n\n"
                  f"📄 <b>STDOUT</b>:\n<code>{decode_msg(result.stdout)[-1500:]}</code>\n\n"
                  f"⚠️ <b>STDERR</b>:\n<code>{decode_msg(result.stderr)[-500:]}</code>")
        await wait_msg.edit_text(report, parse_mode="HTML")
    except Exception as e:
        await wait_msg.edit_text(f"❌ 失败: <code>{str(e)}</code>")

# ===================== 2. 统一注册入口 =====================

def register(manager):
    """终极 V2.7.2 零删减版"""
    for h in [handle_start, handle_ping, handle_id, handle_status, handle_disk, handle_python]:
        setattr(h, "manager", manager)

    register_handler(CommandHandler("start", handle_start), __name__)
    register_handler(CommandHandler("ping", handle_ping), __name__)
    register_handler(CommandHandler("id", handle_id), __name__)
    register_handler(CommandHandler("status", handle_status), __name__)
    register_handler(CommandHandler("disk", handle_disk), __name__)
    register_handler(CommandHandler("python", handle_python), __name__)

    logger.info(f"✅ [{__MODULE_NAME__}] V1.0 全功能版基础命令已就绪")