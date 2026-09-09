#openbot\core\logger.py
import logging
import os
import sys
import time
from logging.handlers import RotatingFileHandler

class ThrottleFilter(logging.Filter):
    """网络断连类警告限流：同一 logger 的同一消息在窗口期内只放行一次，
    避免断网/代理抖动时 Telethon 反复刷"Server closed the connection"。
    """
    def __init__(self, window=30.0):
        super().__init__()
        self._window = window
        self._last = {}

    def filter(self, record):
        key = (record.name, record.getMessage())
        now = time.monotonic()
        if key in self._last and now - self._last[key] < self._window:
            return False
        self._last[key] = now
        return True

class FullLineColorFormatter(logging.Formatter):
    CYAN = "\033[96m"
    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    RESET = "\033[0m"
    # 对齐模块名至 25 位宽度，方便观察是哪个插件在运行
    BASE_FORMAT = "%(asctime)s - %(name)-25s - %(levelname)s - %(message)s"

    def format(self, record):
        log_message = logging.Formatter(self.BASE_FORMAT).format(record)
        if record.levelno == logging.DEBUG: return f"{self.CYAN}{log_message}{self.RESET}"
        if record.levelno == logging.INFO: return f"{self.GREEN}{log_message}{self.RESET}"
        if record.levelno == logging.WARNING: return f"{self.YELLOW}{log_message}{self.RESET}"
        if record.levelno >= logging.ERROR: return f"{self.RED}{log_message}{self.RESET}"
        return log_message

def get_plugin_logger(rel_mod_path: str):
    """
    动态生成插件 Logger。
    输入: features.admin.plugin_manager
    输出: logs/plugins/admin/plugin_manager.log
    """
    logger = logging.getLogger(rel_mod_path)
    
    # 防止重复挂载 Handler (热重载时尤为重要)
    if not logger.handlers:
        # 转换路径: features.admin.plugin_manager -> admin/plugin_manager
        plugin_path = rel_mod_path.replace("features.", "").replace(".", os.sep)
        log_dir = os.path.join("logs", "plugins", os.path.dirname(plugin_path))
        os.makedirs(log_dir, exist_ok=True)
        
        log_file = os.path.join("logs", "plugins", f"{plugin_path}.log")
        
        handler = RotatingFileHandler(
            log_file, maxBytes=5*1024*1024, backupCount=3, encoding='utf-8'
        )
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(handler)
        
        # 允许日志向上传递给 root 从而在控制台显示
        logger.propagate = True
    return logger

def setup_logger():
    # 1. 抑制第三方库噪音 (保持控制台清爽)
    for name in ["telethon", "httpx", "telegram", "asyncio", "httpcore",
                 "apscheduler", "apscheduler.executors", "apscheduler.scheduler"]:
        logging.getLogger(name).setLevel(logging.WARNING)
    # 🚨 Telethon 断连警告限流：断网/节点抖动时"Server closed the connection"只提示一次，
    # 不刷屏；Telethon 本身会自动重连，网络恢复即自动恢复
    telethon_conn_logger = logging.getLogger("telethon.network.connection")
    telethon_conn_logger.addFilter(ThrottleFilter(window=30.0))

    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    # 2. 根日志处理器 (记录所有 DEBUG 信息，用于深度复盘)
    all_handler = RotatingFileHandler(
        os.path.join(log_dir, "openbot_main.log"), 
        maxBytes=10*1024*1024, backupCount=5, encoding='utf-8'
    )
    all_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    all_handler.setLevel(logging.DEBUG)

    # 3. 控制台处理器 (只看 INFO，保证运行感官)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(FullLineColorFormatter())
    console_handler.setLevel(logging.INFO)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.handlers.clear()
    root.addHandler(all_handler)
    root.addHandler(console_handler)

    logging.info("✨ 日志系统已全量激活：主日志(DEBUG) | 控制台(INFO) | 插件日志自动分流")