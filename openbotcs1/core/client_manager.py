#openbot\core\client_manager.py
import logging
import asyncio
from typing import Optional
import httpx
from telegram.ext import Application
from telegram.request import HTTPXRequest
from telegram.error import NetworkError, TimedOut, Conflict
from core.mtproto_client import MTProtoClient
from core.plugin_scanner import load_plugins

logger = logging.getLogger(__name__)

# 🚨 Bot 网络断连标志：轮询失败时置 True，轮询成功时检测到则提示恢复并复位
_bot_network_down = False

def _on_polling_error(exc):
    """轮询层错误拦截：网络类错误只提示一句，不打印整段 traceback 堆栈。
    说明：PTB 的轮询循环本身会无限自动重试（max_retries<0），网络恢复后自动恢复连接，
    因此这里只需提示"网络断开"，不需要让用户看一长串堆栈。"""
    global _bot_network_down
    if isinstance(exc, (NetworkError, TimedOut)):
        _bot_network_down = True
        logger.warning("⚠️ 与 Telegram Bot API 连接中断，正在自动重连...")
    elif isinstance(exc, Conflict):
        # 同一 token 被多个实例轮询：只提示一句，不打印整段 traceback
        logger.error("❌ Bot 轮询冲突：检测到另一个实例在运行（同一 token 重复轮询）。请只保留一个 main.py 进程。")
    else:
        logger.error("❌ Bot 轮询异常", exc_info=exc)

async def _on_update_error(update, context):
    """更新处理层错误拦截：网络类错误只提示一句，其他异常保留完整 traceback。"""
    exc = context.error
    if isinstance(exc, (NetworkError, TimedOut)):
        logger.warning("⚠️ 回复/处理更新时网络中断，正在自动重连...")
    else:
        logger.error("❌ Bot 更新处理异常", exc_info=exc)

class ClientManager:
    def __init__(self, config, loop):
        self.config = config
        self.loop = loop
        self.bot_app: Optional[Application] = None
        self.mtproto_client: Optional[MTProtoClient] = None
        self.shutdown_callbacks = []  # 停机回调列表，插件可以注册
        self._monitor_stop = asyncio.Event()   # MTProto 状态监控的停止事件
        self._monitor_task: Optional[asyncio.Task] = None

    async def start_all(self) -> None:
        """启动系统：按顺序初始化 Bot 和 MTProto"""
        # 统一读取代理配置：配了 PROXY 就走代理，没配就直连（Bot 和 MTProto 共用同一套）
        # config.get() 已内部覆盖 .env 与系统环境变量，无需再重复 or os.environ.get
        proxy_url = self.config.get("PROXY")

        # 1. 初始化 Bot 实例
        # 💡 显式配置 HTTPXRequest（替代 builder.proxy）：
        #    - pool_timeout 从默认 1.0s 放大到 15s：连接池短暂繁忙时不再误判 TimedOut
        #    - read_timeout 放宽到 30s：代理下 getUpdates 长轮询/大响应需要更久
        #    - httpx_kwargs.limits 禁用 keep-alive（max_keepalive_connections=0）：
        #      每次请求新建连接，避免复用被 Clash/代理空闲关闭的"死连接"导致 get_updates 卡死
        #      （实测：代理下长轮询复用死连接会出现 ConnectError: getaddrinfo failed / 无限挂起）
        builder = Application.builder().token(self.config.get("BOT_TOKEN"))
        bot_proxy = httpx.Proxy(url=proxy_url) if proxy_url else None
        request = HTTPXRequest(
            proxy=bot_proxy,
            connection_pool_size=64,
            pool_timeout=15.0,
            read_timeout=30.0,
            connect_timeout=8.0,
            write_timeout=20.0,
            media_write_timeout=60.0,
            httpx_kwargs={
                "limits": httpx.Limits(max_connections=64, max_keepalive_connections=0),
            },
        )
        builder = builder.request(request)
        # 🚨【关键修复】get_updates 轮询必须复用同一个走代理的 request！
        # 否则 PTB 默认给 get_updates 建一个独立请求：proxy=None（直连被墙）
        # + pool_timeout=1 + connection_pool_size=1，导致：
        #   1) 轮询直连 api.telegram.org 被墙 → connect 超时/卡死 → 命令堆积无响应
        #   2) 连接池仅 1 个连接且 1 秒超时 → "All connections in the pool are occupied"
        # 这是"断网重连后/转发视频后命令无响应"的真正根因（monkey patch 实测证实）
        builder = builder.get_updates_request(request)
        if proxy_url:
            logger.info(f"🔌 Bot API 已配置代理: {proxy_url}")
        else:
            logger.warning("⚠️ Bot API 未配置代理，将直连 Telegram（容器/服务器需有外网）")
        self.bot_app = builder.build()
        # 🚨 注册更新处理层错误处理器：网络中断只提示一句，其他异常保留完整 traceback
        self.bot_app.add_error_handler(_on_update_error)
        
        # 2. 注入管理器和配置到全局 bot_data
        # 💡 这确保了 mtlogin.py 等插件可以通过 context.bot_data['manager'] 访问
        self.bot_app.bot_data['manager'] = self
        self.bot_app.bot_data['config'] = self.config
        
        # 3. 启动 MTProto 持久化引擎
        # 💡 关键修改：增加超时判断，防止连接 Telegram 服务器时死等
        self.mtproto_client = MTProtoClient(
            api_id=int(self.config.get("API_ID")),
            api_hash=self.config.get("API_HASH"),
            loop=self.loop,
            proxy=self.config.get("PROXY")
        )
        
        try:
            # 💡 暴力启动：如果 15 秒内连不上，说明网络环境极差，直接报错不卡死
            success = await asyncio.wait_for(self.mtproto_client.start(), timeout=15.0)
            if success:
                logger.info("✅ MTProto 持久化引擎已就绪")
            else:
                logger.error("⚠️ MTProto 启动异常，部分核心功能（如强制抓取）将受限")
        except asyncio.TimeoutError:
            logger.error("❌ MTProto 启动连接超时：请确认代理配置正确（PROXY=socks5://127.0.0.1:1080）")

        # 4. 扫描并注册插件 (传入 manager 实例供 register 函数使用)
        load_plugins(self) 
        
        # 5. 启动 Bot 轮询
        await self.bot_app.initialize()
        await self.bot_app.start()
        # 🚨 网络断连降噪：轮询错误传自定义 error_callback（网络类错误只提示一句"连接中断，
        # 正在自动重连"，不再打印整段 traceback 堆栈）；PTB 轮询循环自动无限重试，网络恢复即自动连接
        # 🚨 bootstrap_retries=3：启动时 bootstrap（删 webhook）失败后重试 3 次（共尝试 4 次，
        # 间隔 1.5s→2.25s→3.375s 递增），仍失败则抛错 → launcher 打印"系统运行崩溃"并停机退出
        await self.bot_app.updater.start_polling(
            error_callback=_on_polling_error,
            bootstrap_retries=3,
        )
        logger.info("🤖 Bot 系统已完全启动，正在监听指令...")

        # 6. 启动网络状态监控（Bot 恢复探测 + MTProto 恢复检测，正常时零网络请求）
        self._monitor_stop = asyncio.Event()
        self._monitor_task = asyncio.create_task(self._network_monitor())
        logger.info("📡 网络状态监控已启动")

    async def stop_all(self) -> None:
        """安全停止所有服务，并销毁内存残留"""
        logger.info("🛑 正在执行系统停机清理...")
        
        # 1. 先执行所有停机回调（取消下载任务等）
        for cb in self.shutdown_callbacks:
            try:
                await cb()
            except Exception as e:
                logger.error(f"停机回调异常: {e}")
        
        if self.bot_app:
            try:
                # 💡 停止轮询并释放 Bot 资源
                if self.bot_app.updater.running:
                    await self.bot_app.updater.stop()
                await self.bot_app.stop()
                await self.bot_app.shutdown()
            except Exception as e:
                logger.error(f"Bot 关闭异常: {e}")
                
        if self.mtproto_client:
            try:
                # 💡 断开 MTProto TCP 连接
                await self.mtproto_client.stop()
            except Exception as e:
                logger.error(f"MTProto 断开异常: {e}")

        # 停止 MTProto 网络状态监控任务
        if self._monitor_task:
            self._monitor_stop.set()
            try:
                await asyncio.wait_for(self._monitor_task, timeout=5.0)
            except Exception:
                self._monitor_task.cancel()
            self._monitor_task = None
        
        # 💡 极致安全：强制清空内存引用，确保登录凭据不留痕迹
        self.bot_app = None
        self.mtproto_client = None

    async def _network_monitor(self) -> None:
        """后台监控网络状态（每 5 秒）：
        - MTProto：is_connected() 为本地布尔标志（不触发网络请求），从断开→恢复只提示一次
        - Bot：仅在 _bot_network_down 为 True 时发 get_me() 探测（走代理），成功即提示恢复并复位
        正常状态下不发任何额外网络请求，零开销。"""
        global _bot_network_down
        was_connected: Optional[bool] = None
        while not self._monitor_stop.is_set():
            try:
                # MTProto 恢复检测（本地标志，无网络请求）
                if self.mtproto_client:
                    connected = self.mtproto_client.client.is_connected()
                    if was_connected is None:
                        was_connected = connected  # 首次采样，只记录不提示
                    elif connected and not was_connected:
                        logger.info("✅ MTProto 网络已恢复")
                    was_connected = connected
                # Bot 恢复检测：仅当断连标志为 True 时探测（get_me 走代理，成功即恢复）
                if _bot_network_down:
                    try:
                        await self.bot_app.bot.get_me()
                        _bot_network_down = False
                        logger.info("✅ 网络已恢复，与 Telegram Bot API 连接正常")
                    except Exception:
                        pass  # 仍未恢复，下一轮再探
            except Exception:
                pass
            try:
                await asyncio.wait_for(self._monitor_stop.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                pass

    @property
    def bot(self):
        """快捷访问底层的 Bot 对象"""
        return self.bot_app.bot if self.bot_app else None
