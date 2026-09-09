import os
import html
import logging
import asyncio
from pathlib import Path
from telegram import Update
from telegram.ext import ContextTypes, CommandHandler, MessageHandler, filters
from core.command_registry import register_handler, get_plugin_map
from core.utils import is_admin
from core.plugin_scanner import verify_syntax, load_plugins

logger = logging.getLogger(__name__)

# 💡 这个变量现在会被 command_registry 自动抓取作为 UI 显示的标题
__MODULE_NAME__ = "插件管理"

# 【S3热重载保护】reload 时保留进行中的插件安装会话，避免进行到一半的安装会话丢失
if "active_sessions" not in globals():
    active_sessions = {}

def get_save_dir():
    base_path = Path(__file__).resolve().parent.parent
    save_dir = base_path / "custom"
    save_dir.mkdir(parents=True, exist_ok=True)
    (save_dir / "__init__.py").touch(exist_ok=True)
    return save_dir

def escape_html(text):
    """HTML 模式下的特殊字符转义（复用标准库 html.escape，比手写更全：额外转义引号）"""
    return html.escape(text)

# --- 核心业务处理器 ---

async def handle_start_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    manager = getattr(handle_start_add, "manager", None) or context.bot_data.get('manager')
    user_id = update.effective_user.id
    if not is_admin(user_id, manager.config): return

    if user_id in active_sessions:
        try: active_sessions.pop(user_id)["task"].cancel()
        except: pass

    await update.message.reply_html(
        "⏳ <b>进入插件安装模式</b>\n请发送 <code>.py</code> 文件\n"
        "系统将执行：<code>语法预检</code> → <code>热部署</code>"
    )

    async def countdown():
        await asyncio.sleep(60)
        if user_id in active_sessions:
            active_sessions.pop(user_id)
            await update.message.reply_text("❌ 安装超时，已自动退出。")

    active_sessions[user_id] = {"task": asyncio.create_task(countdown())}

async def handle_file_upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in active_sessions: return 

    doc = update.message.document
    if not doc or not doc.file_name.endswith('.py'): return

    session = active_sessions.pop(user_id)
    session["task"].cancel()
    
    manager = getattr(handle_file_upload, "manager", None) or context.bot_data.get('manager')
    # 安全加固：只取文件名最后一段，防止路径穿越（如 ../../evil.py 写出目录）
    safe_name = Path(doc.file_name).name
    if not safe_name.endswith('.py'):
        return
    save_path = get_save_dir() / safe_name
    
    if save_path.exists():
        await update.message.reply_text(f"⚠️ 发现同名插件，正在执行热覆盖...")

    try:
        file_obj = await context.bot.get_file(doc.file_id)
        await file_obj.download_to_drive(str(save_path))

        if not verify_syntax(str(save_path)):
            if save_path.exists(): os.remove(save_path)
            await update.message.reply_html("❌ <b>安装终止：语法错误</b>\n请检查代码缩进或括号。")
            return

        # 执行全量重载
        load_plugins(manager) 
        
        await update.message.reply_html(
            f"✅ <b>插件部署成功！</b>\n模块：<code>{escape_html(doc.file_name)}</code>"
        )
        logger.info(f"成功部署新插件: {doc.file_name}")

    except Exception as e:
        if save_path.exists(): os.remove(save_path)
        await update.message.reply_html(f"❌ <b>安装失败已回滚</b>\n错误：<code>{escape_html(str(e))}</code>")

async def handle_reload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    manager = getattr(handle_reload, "manager", None) or context.bot_data.get('manager')
    if not is_admin(update.effective_user.id, manager.config): return
    
    wait_msg = await update.message.reply_text("🔄 正在同步插件目录...")
    try:
        load_plugins(manager)
        await wait_msg.edit_text("✅ 插件全量重载完成。")
    except Exception as e:
        await wait_msg.edit_text(f"❌ 重载失败: {e}")

async def handle_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/plugins - 核心显示函数：展示中文名和文件名"""
    manager = getattr(handle_list, "manager", None) or context.bot_data.get('manager')
    if not is_admin(update.effective_user.id, manager.config): return
    
    p_map = get_plugin_map()
    if not p_map:
        return await update.message.reply_text("📂 库中无活跃插件。")

    report = "📂 <b>系统插件清单</b>\n"
    report += "━━━━━━━━━━━━━━━\n\n"

    for key in sorted(p_map.keys()):
        data = p_map[key]
        # 从 Registry 中读取我们存入的 alias 和 file
        alias = data.get("alias", key)
        file_name = data.get("file", f"{key}.py")
        cmds = data.get("cmds", set())

        report += f"📦 <b>{alias}</b> (<code>{file_name}</code>)\n"
        
        if cmds:
            cmd_info = " ".join([f"<code>/{c.lstrip('/')}</code>" for c in sorted(cmds)])
            report += f"└ 指令: {cmd_info}\n\n"
        else:
            report += f"└ 状态: 📡 <b>后台监听模式</b>\n\n"

    report += "━━━━━━━━━━━━━━━\n"
    report += f"💡 <i>共计加载 {len(p_map)} 个物理模块</i>"

    await update.message.reply_html(report)

def register(manager):
    handlers = [handle_start_add, handle_file_upload, handle_reload, handle_list]
    for h in handlers: h.manager = manager
    
    # 注册到全局 Registry
    register_handler(CommandHandler("add_plugin", handle_start_add), __name__)
    register_handler(CommandHandler("reload_plugins", handle_reload), __name__)
    register_handler(CommandHandler("plugins", handle_list), __name__)
    register_handler(MessageHandler(filters.Document.FileExtension("py") & filters.ChatType.PRIVATE, handle_file_upload), __name__)

    logger.info(f"✅ [{__MODULE_NAME__}] V1.0 插件管理功能已就绪")