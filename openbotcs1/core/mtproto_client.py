#openbot\core\mtproto_client.py
import logging
import os
import asyncio
from telethon import TelegramClient
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

def _parse_proxy(proxy_url: str) -> Optional[Tuple]:
    """
    解析代理 URL 为 Telethon 所需的元组格式。
    支持: socks5://127.0.0.1:1080  /  http://127.0.0.1:7890
    也支持带认证: socks5://user:pass@127.0.0.1:1080
    """
    if not proxy_url:
        return None
    try:
        from urllib.parse import urlparse
        parsed = urlparse(proxy_url.strip())
        proxy_type = parsed.scheme.lower()  # socks5 / http / socks4
        host = parsed.hostname
        port = parsed.port
        if not host or not port:
            logger.warning(f"⚠️ 代理地址格式无效: {proxy_url}")
            return None
        # Telethon 代理元组: (type, host, port, rdns=True, username, password)
        rdns = True
        username = parsed.username or None
        password = parsed.password or None
        return (proxy_type, host, port, rdns, username, password)
    except Exception as e:
        logger.warning(f"⚠️ 解析代理地址失败: {e}")
        return None

class MTProtoClient:
    def __init__(self, api_id: int, api_hash: str, loop=None, proxy: Optional[str] = None):
        self.api_id = api_id
        self.api_hash = api_hash
        
        session_dir = "sessions"
        os.makedirs(session_dir, exist_ok=True)
        self.session_path = os.path.join(session_dir, "openbot") 
        
        # 连接锁：防止多个协程并发调用 connect() 导致 Telethon 内部竞态条件
        # 错误表现: RuntimeError: readexactly() called while another coroutine is already waiting for incoming data
        self._connect_lock = asyncio.Lock()
        
        # 代理逻辑：配了 PROXY 就走代理，没配就直连（不自动检测系统代理）
        proxy_tuple = _parse_proxy(proxy) if proxy else None
        if proxy_tuple:
            logger.info(f"🔌 MTProto 已配置代理: {proxy_tuple[0]}://{proxy_tuple[1]}:{proxy_tuple[2]}")
        else:
            logger.warning("⚠️ MTProto 未配置代理，将直连 Telegram（容器/服务器需有外网）")
        
        # 💡 对齐 loop 并保持暴力连接参数
        self.client = TelegramClient(
            self.session_path,
            api_id,
            api_hash,
            loop=loop,
            proxy=proxy_tuple,
            connection_retries=10, 
            # 💡 重连间隔 2s→5s：降低代理节点抖动时的重连风暴频率，
            #    给 TIME_WAIT 连接释放时间，避免本地端口耗尽 (WinError 10048)
            retry_delay=5,
            auto_reconnect=True,
            sequential_updates=False,   
            timeout=10,
            receive_updates=True
        )
    
    async def ensure_connected(self, timeout: float = 10.0) -> bool:
        """
        确保客户端已连接，带异步锁防止并发连接。
        所有需要连接的地方都应调用此方法，不要直接调用 client.connect()
        """
        async with self._connect_lock:
            if self.client.is_connected():
                return True
            try:
                await asyncio.wait_for(self.client.connect(), timeout=timeout)
                return True
            except Exception as e:
                logger.error(f"❌ MTProto 连接失败: {e}")
                return False
    
    async def start(self) -> bool:
        try:
            if not await self.ensure_connected(timeout=10.0):
                return False
            
            self.client.max_concurrent_transfers = 16
            
            # 💡 只有在已授权情况下才拉取 dialogs，否则登录前拉取会报错
            if await self.client.is_user_authorized():
                await self.client.get_dialogs(limit=1)
            
            logger.info("✅ MTProto 物理引擎已就绪")
            return True
        except Exception as e:
            logger.error(f"❌ MTProto 启动连接失败: {e}")
            return False

    async def is_authorized(self) -> bool:
        try:
            if not await self.ensure_connected(timeout=5.0):
                return False
            return await self.client.is_user_authorized()
        except:
            return False

    async def ensure_ready(self, timeout: float = 10.0) -> bool:
        """
        统一"问"接口：MTProto 是否就绪（连接 + 授权一步到位）。
        所有需要 MTProto 的插件/调用点都应只问这个方法，不要各自判断。
        返回 True = 已连接且已登录，可直接干活；False = 未就绪（原因已记录日志）。
        """
        if not await self.ensure_connected(timeout=timeout):
            logger.warning("⚠️ MTProto 未就绪：连接失败（检查网络/代理）")
            return False
        try:
            if not await self.client.is_user_authorized():
                logger.warning("⚠️ MTProto 未就绪：账号未登录，请先 /mtlogin")
                return False
        except Exception as e:
            logger.warning(f"⚠️ MTProto 未就绪：授权检查异常 {e}")
            return False
        return True

    async def stop(self) -> None:
        if self.client and self.client.is_connected():
            await self.client.disconnect()
            logger.info("🔌 MTProto 已安全断开")
