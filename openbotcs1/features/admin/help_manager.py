# openbot\features\admin\help_manager.py
import logging
from telegram import Update
from telegram.ext import ContextTypes, CommandHandler
from core.command_registry import register_handler
from core.utils import is_admin

logger = logging.getLogger(__name__)

# 插件名称
__MODULE_NAME__ = "开发手册"

# --- 核心文档内容 ---
PROJECT_DOCS = (
    "🚀 <b>OpenBot 2026 项目架构说明</b>\n\n"
    "本系统采用 <b>Bot API</b>（指令交互层）+ <b>MTProto</b>（扫描/下载/登录层）双引擎，支持热重载。\n"
    "Web 查看器（look_server，7777 端口）为独立进程，只读数据库 + 写请求队列。\n\n"
    "📂 <b>目录结构：</b>\n"
    "• <code>bootstrap/</code>: 启动编排（launcher）\n"
    "• <code>core/</code>: 核心驱动层（连接/数据库/扫描/下载/日志/配置）\n"
    "• <code>features/</code>: 插件层（每个插件一个目录，独立安装）\n"
    "• <code>download/</code>: 下载存储 + 数据库（download_tasks.db / media_cache.db）\n"
    "• <code>sessions/</code>: MTProto 物理会话\n\n"
    "🛡️ <b>开发准则：</b>\n"
    "1️⃣ <b>插件只依赖 core</b>: 插件之间互不 import，各自独立（可能只装一个，也可能全装）。\n"
    "2️⃣ <b>连接统一走 core</b>: Bot API 用 <code>client_manager</code>，MTProto 用 <code>mtproto_client</code>（<code>manager.mtproto_client.client</code>）。\n"
    "3️⃣ <b>扫描/下载走 core 共享</b>: 扫描用 <code>media_scanner.scan_chat_to_cache</code> 写共享缓存 <code>media_cache</code>，下载用 <code>download_engine</code>，避免重复造车。\n"
    "4️⃣ <b>命令注册</b>: <code>register_handler(CommandHandler('cmd', func), __name__)</code>，插件目录的 help.txt 维护命令描述（菜单自动同步）。\n"
    "5️⃣ <b>配置读取</b>: 通过 <code>manager.config.get('KEY')</code>，管理员校验用 <code>core.utils.is_admin</code>。"
)

# 核心修改：模板现在改为注入模式
CODE_TEMPLATE = (
    "__MODULE_NAME__ = \"新功能名称\"\n\n"
    "async def handle_func(update, context):\n"
    "    manager = getattr(handle_func, 'manager', None)\n"
    "    await update.message.reply_text('✅ 引擎已就绪')\n\n"
    "def register(manager):\n"
    "    handle_func.manager = manager\n"
    "    register_handler(CommandHandler('cmd', handle_func), __name__)"
)

# --- 业务处理器 ---

async def handle_cj(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    
    # 获取注入的 manager 
    manager = getattr(handle_cj, "manager", None) or context.bot_data.get('manager')
    config = manager.config if manager else context.bot_data.get('config')

    # 权限校验
    if not is_admin(user_id, config):
        await update.message.reply_text("🚫 该手册仅限管理员查看。")
        return

    # 发送项目说明
    await update.message.reply_text(PROJECT_DOCS, parse_mode="HTML")
    
    # 发送代码模板
    template_msg = (
        "📄 <b>V2.5 标准插件模板</b>\n"
        f"<pre><code class=\"language-python\">{CODE_TEMPLATE}</code></pre>"
    )
    await update.message.reply_text(template_msg, parse_mode="HTML")

# ===================== 统一注册入口 =====================

def register(manager):
    """
    修改为注入模式：
    1. 绑定 manager 方便 handle_cj 使用
    2. 注册 CommandHandler
    """
    handle_cj.manager = manager
    register_handler(CommandHandler("cj", handle_cj), __name__)

# 注意：不要在末尾手动调用 register()，由 scanner 自动调用
    logger.info(f"✅ [{__MODULE_NAME__}] V1.0 开发手册插件已就绪")