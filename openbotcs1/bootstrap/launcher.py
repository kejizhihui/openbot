#openbot\bootstrap\launcher.py
import asyncio
import logging
import sys
from telethon import functions
from telegram import Update
from telegram.ext import MessageHandler, filters, ContextTypes
from core.config_manager import ConfigManager
from core.validator import ConfigValidator
from core.client_manager import ClientManager
from core.logger import setup_logger
from core.command_registry import register_system_handler, sync_command_menu
from core.child_services import start_all as child_start_all
from core.child_services import watch as child_watch
from core.child_services import stop_all as child_stop_all
from core.singleton import acquire as singleton_acquire
from core.singleton import release as singleton_release

logger = logging.getLogger(__name__)

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# --- 新增：无效命令兜底处理器 ---
async def unknown_command_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """拦截所有未匹配的斜杠指令"""
    # 仅针对私聊反馈，避免群组干扰
    if update.effective_chat.type == "private":
        await update.message.reply_text(
            "❌ <b>未知指令</b>\n"
            "系统无法识别该命令。请发送 /plugins 查看可用功能清单。",
            parse_mode="HTML"
        )

async def show_status_summary(manager):
    """打印启动后的汇总信息"""
    logger.info("\n📌 OpenBot 启动状态汇总")
    try:
        me = await manager.bot_app.bot.get_me()
        logger.info(f"Bot 状态：已连接 (@{me.username})")
        
        if manager.mtproto_client and manager.mtproto_client.client:
            try:
                await manager.mtproto_client.client(functions.updates.GetStateRequest())
            except:
                pass
            is_auth = await manager.mtproto_client.is_authorized()
            status = "已登录" if is_auth else "未授权 (需 /mtlogin)"
            logger.info(f"MTProto 状态：{status}")
            # 写入系统状态表，供 look_server（Web 查看器）读取并在页面提示登录状态
            try:
                from features.look.db import set_status
                set_status("mtproto_status", "logged_in" if is_auth else "not_logged_in")
            except Exception:
                pass
        else:
            logger.info("MTProto 状态：未初始化")
    except Exception as e:
        logger.warning(f"⚠️ 状态汇总读取部分受阻: {e}")

def run_bot():
    setup_logger()
    # 🚨 单实例保护：已有 main.py 实例在运行则直接拒绝启动，避免双实例抢同一
    #    bot token 导致 getUpdates Conflict / 数据库争抢 locked
    if not singleton_acquire():
        logger.error("❌ 检测到已有 OpenBot 实例在运行（单实例锁被占用），本实例拒绝启动。请先关闭已运行的 main.py 再试。")
        return
    config = ConfigManager()
    
    validator = ConfigValidator(config)
    ok, errors = validator.validate_all()
    if not ok:
        # 配置错误：汇总键级错误 → 自动重置 .env 为模板（备份旧配置）→ 提示后重启
        detail = '；'.join('{}: {}'.format(e.get('key', '?'), e.get('reason', '')) for e in errors)
        logger.error('❌ 配置错误: ' + detail)
        if config.reset_to_template():
            logger.error('已自动用 .env.example 重置 .env（旧配置备份为 .env.bak），请填写后重启')
        return

    # Python 3.10+ 主线程无运行 loop 时 get_event_loop() 会触发 DeprecationWarning，
    # 直接显式创建事件循环更规范，避免告警噪音
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    manager = ClientManager(config, loop)
    watch_task = None

    try:
        # 1. 启动所有组件 (此处内部会完成所有插件的 register_handler 动作)
        # 💡 启动重试：Clash 节点抖动 / Telegram API 瞬时超时会抛 TimedOut，
        #    直接退出会让容器"一抖就死"。捕获瞬时异常 → stop_all 复位 → 5 秒后重试。
        MAX_START_RETRY = 5
        for _attempt in range(1, MAX_START_RETRY + 1):
            try:
                loop.run_until_complete(manager.start_all())
                break
            except Exception as e:
                if _attempt >= MAX_START_RETRY:
                    raise
                logger.warning(f"⚠️ 组件启动失败（第 {_attempt}/{MAX_START_RETRY} 次）：{e}，5 秒后自动重试...")
                try:
                    loop.run_until_complete(manager.stop_all())
                except Exception:
                    pass
                loop.run_until_complete(asyncio.sleep(5))
        
        # 1.5 拉起各插件注册的子进程服务（如 look_server 独立 Web 进程）并挂 15 秒存活看护
        child_start_all()
        watch_task = loop.create_task(child_watch())
        
        # 2. --- 【核心注入：无效命令兜底】 ---
        # ① 注册进"系统级持久列表"(SYSTEM_HANDLERS)：clear_handlers 不会清除它，
        #    热重载(load_plugins) 重挂时会与插件 handler 一起重新挂载，兜底跨 reload 不丢失。
        # ② 同时手动 add_handler 挂到 app：因为 start_all() 已完成首次挂载，
        #    此处必须显式挂载，否则首次启动兜底不生效。
        fallback_handler = MessageHandler(filters.COMMAND, unknown_command_handler)
        register_system_handler(fallback_handler)
        manager.bot_app.add_handler(fallback_handler)
        logger.info("🛡️ 全局无效指令兜底已激活")

        # 2.5 同步命令菜单（所有插件注册完成后，从各 help.txt 生成 Telegram 命令菜单）
        try:
            loop.run_until_complete(sync_command_menu(manager.bot_app))
        except Exception as e:
            logger.warning(f"⚠️ 命令菜单同步失败: {e}")

        # 3. 显示汇总并运行
        loop.run_until_complete(show_status_summary(manager))
        
        logger.info("\n🚀 OpenBot 运行中... (按 Ctrl+C 退出)")
        loop.run_forever()

    except (KeyboardInterrupt, SystemExit):
        logger.info("\n🛑 接收到停止信号，准备安全退出...")
    except Exception as e:
        logger.error(f"\n❌ 系统运行崩溃: {e}", exc_info=True)
    finally:
        if watch_task:
            watch_task.cancel()
            try:
                loop.run_until_complete(watch_task)
            except (asyncio.CancelledError, Exception):
                pass
        if manager:
            try:
                loop.run_until_complete(manager.stop_all())
            except:
                pass

        # 停止所有插件注册的子进程服务（先温和 terminate，超时再 kill）
        child_stop_all()

        # 释放单实例锁（进程崩溃时 OS 也会自动释放）
        singleton_release()

        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
        finally:
            loop.close()
            print("\033[92m" + f"{logging.Formatter().formatTime(logging.makeLogRecord({}), '%Y-%m-%d %H:%M:%S')} - root - INFO - 👋 程序已完全安全退出" + "\033[0m")