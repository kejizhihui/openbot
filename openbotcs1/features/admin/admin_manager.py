#openbot\features\admin\admin_manager.py
import logging
from telegram import Update
from telegram.ext import ContextTypes, CommandHandler
from core.command_registry import register_handler
from core.utils import is_admin

logger = logging.getLogger(__name__)

__MODULE_NAME__ = "用户管理"

def _get_admin_list(config):
    """读取 ADMIN_LIST 并解析为整数列表"""
    raw = config.get("ADMIN_LIST", "")
    if not raw:
        return []
    try:
        return [int(x.strip()) for x in str(raw).split(",") if x.strip()]
    except (ValueError, TypeError):
        return []

def _save_admin_list(config, admin_list):
    """把管理员列表保存为逗号分隔字符串到 .env"""
    config.set("ADMIN_LIST", ",".join(str(x) for x in admin_list))

# --- 业务处理器 ---

async def handle_add_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """添加管理员权限"""
    manager = getattr(handle_add_admin, "manager", None) or context.bot_data.get('manager')
    config = manager.config

    # 安全加固：仅超级管理员(ADMIN_ID)可添加管理员，防止普通管理员自行提升权限
    if str(update.effective_user.id) != str(config.get("ADMIN_ID")):
        await update.message.reply_text("❌ 仅超级管理员可操作！")
        return

    if not context.args:
        await update.message.reply_text("💡 用法：/add_admin 用户ID")
        return

    try:
        new_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("⚠️ ID 必须是纯数字")
        return

    admin_list = _get_admin_list(config)
    if new_id in admin_list:
        await update.message.reply_text("ℹ️ 该用户已在管理员列表中")
        return

    admin_list.append(new_id)
    _save_admin_list(config, admin_list)
    await update.message.reply_text(f"✅ 已添加管理员: `{new_id}`", parse_mode="Markdown")
    logger.info(f"管理员 {update.effective_user.id} 添加了新管理员 {new_id}")

async def handle_remove_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """移除管理员权限"""
    manager = getattr(handle_remove_admin, "manager", None) or context.bot_data.get('manager')
    config = manager.config

    # 安全加固：仅超级管理员(ADMIN_ID)可移除管理员
    if str(update.effective_user.id) != str(config.get("ADMIN_ID")):
        await update.message.reply_text("❌ 仅超级管理员可操作！")
        return

    if not context.args:
        await update.message.reply_text("💡 用法：/remove_admin 用户ID")
        return

    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("⚠️ ID 必须是纯数字")
        return

    if target_id == update.effective_user.id:
        await update.message.reply_text("⚠️ 不能移除自己的管理员权限")
        return

    admin_list = _get_admin_list(config)
    if target_id not in admin_list:
        await update.message.reply_text("ℹ️ 该用户不在管理员列表中")
        return

    admin_list.remove(target_id)
    _save_admin_list(config, admin_list)
    await update.message.reply_text(f"✅ 已移除管理员: `{target_id}`", parse_mode="Markdown")
    logger.info(f"管理员 {update.effective_user.id} 移除了管理员 {target_id}")

async def handle_admins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """查看管理团队列表"""
    manager = getattr(handle_admins, "manager", None) or context.bot_data.get('manager')
    config = manager.config

    if not is_admin(update.effective_user.id, config):
        return

    super_admin = config.get("ADMIN_ID")
    admin_list = _get_admin_list(config)

    msg = (
        f"👑 **超级管理员**: `{super_admin}`\n"
        f"🛠️ **管理员列表**: `{len(admin_list)}`人\n"
        f"━━━━━━━━━━━━━━\n"
    )
    if admin_list:
        msg += "\n".join([f"• `{a}`" for a in admin_list])
    else:
        msg += "（暂无额外管理员）"
    await update.message.reply_text(msg, parse_mode="Markdown")

async def handle_groupinfo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """高级群组信息统计 (MTProto 暴力引擎)"""
    manager = getattr(handle_groupinfo, "manager", None) or context.bot_data.get('manager')

    if not is_admin(update.effective_user.id, manager.config):
        return

    # 限制只能在群组/超级群组中使用
    if update.effective_chat.type not in ("group", "supergroup"):
        await update.message.reply_text("⚠️ 此命令请在群组中使用")
        return

    status_msg = await update.message.reply_text("🔍 正在通过 MTProto 抓取深度数据...")

    if manager and manager.mtproto_client and manager.mtproto_client.client:
        try:
            client = manager.mtproto_client.client
            if not await manager.mtproto_client.ensure_ready():
                await status_msg.edit_text("❌ MTProto 未就绪（未登录或连接失败），请先 /mtlogin 或检查代理配置。")
                return
            from telethon.tl.functions.channels import GetFullChannelRequest

            full = await client(GetFullChannelRequest(update.effective_chat.id))

            title = full.chats[0].title
            count = full.full_chat.participants_count
            online = getattr(full.full_chat, 'online_count', '未知')

            msg = (
                f"📊 **群组信息统计**\n"
                f"━━━━━━━━━━━━━━\n"
                f"🏷️ 群组名称: `{title}`\n"
                f"👥 成员总数: `{count}`\n"
                f"🌐 在线人数: `{online}`\n"
                f"🆔 内部 ID: `{update.effective_chat.id}`"
            )
            await status_msg.edit_text(msg, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"MTProto 抓取失败: {e}")
            await status_msg.edit_text(f"❌ MTProto 解析失败: {str(e)}")
    else:
        await status_msg.edit_text("❌ MTProto 引擎未就绪，无法获取深度数据。")

async def handle_ban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """封禁群组成员"""
    manager = getattr(handle_ban, "manager", None) or context.bot_data.get('manager')

    if not is_admin(update.effective_user.id, manager.config):
        return

    if update.effective_chat.type not in ("group", "supergroup"):
        await update.message.reply_text("⚠️ 此命令请在群组中使用")
        return

    if not context.args:
        await update.message.reply_text("💡 用法：回复用户消息后执行 /ban，或 /ban 用户ID")
        return

    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("⚠️ ID 必须是纯数字")
        return

    try:
        await context.bot.ban_chat_member(
            chat_id=update.effective_chat.id,
            user_id=target_id
        )
        await update.message.reply_text(f"🚫 已封禁用户: `{target_id}`", parse_mode="Markdown")
        logger.info(f"管理员 {update.effective_user.id} 在群组 {update.effective_chat.id} 封禁了用户 {target_id}")
    except Exception as e:
        await update.message.reply_text(f"❌ 封禁失败: {str(e)}")
        logger.error(f"封禁失败: {e}")

async def handle_unban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """解封群组成员"""
    manager = getattr(handle_unban, "manager", None) or context.bot_data.get('manager')

    if not is_admin(update.effective_user.id, manager.config):
        return

    if update.effective_chat.type not in ("group", "supergroup"):
        await update.message.reply_text("⚠️ 此命令请在群组中使用")
        return

    if not context.args:
        await update.message.reply_text("💡 用法：/unban 用户ID")
        return

    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("⚠️ ID 必须是纯数字")
        return

    try:
        await context.bot.unban_chat_member(
            chat_id=update.effective_chat.id,
            user_id=target_id
        )
        await update.message.reply_text(f"✅ 已解封用户: `{target_id}`", parse_mode="Markdown")
        logger.info(f"管理员 {update.effective_user.id} 在群组 {update.effective_chat.id} 解封了用户 {target_id}")
    except Exception as e:
        await update.message.reply_text(f"❌ 解封失败: {str(e)}")
        logger.error(f"解封失败: {e}")

# ===================== 统一注册入口 =====================

def register(manager):
    handlers = [
        handle_add_admin, handle_remove_admin, handle_admins,
        handle_groupinfo, handle_ban, handle_unban
    ]

    for h in handlers:
        h.manager = manager

    register_handler(CommandHandler("add_admin", handle_add_admin), __name__)
    register_handler(CommandHandler("remove_admin", handle_remove_admin), __name__)
    register_handler(CommandHandler("admins", handle_admins), __name__)
    register_handler(CommandHandler("groupinfo", handle_groupinfo), __name__)
    register_handler(CommandHandler("ban", handle_ban), __name__)
    register_handler(CommandHandler("unban", handle_unban), __name__)

    logger.info(f"✅ [{__MODULE_NAME__}] V2.0 管理员功能已就绪（多管理员+封禁+解封）")
