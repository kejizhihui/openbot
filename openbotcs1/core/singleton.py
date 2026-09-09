# core/singleton.py
"""
【core 单实例锁】保证整个项目同一时间只有一个主进程(main.py)在运行。
- Windows : 命名互斥量 CreateMutexW（进程崩溃/被杀由 OS 自动释放，无残留锁文件）
- Linux/Docker : fcntl.flock 文件锁（进程退出自动释放）
- 调用 : launcher.run_bot() 最顶部 acquire()，已被占用则直接拒绝启动；
         正常退出时 finally 里 release()（进程崩溃时无需手动，OS 自动释放）。
"""
import sys
import os
import logging

logger = logging.getLogger(__name__)

# Windows 命名互斥名（Local\ 前缀限制在当前会话，避免权限问题）
_MUTEX_NAME = "Local\\OpenBot_OpenBotCS1_Main"
# Linux 锁文件路径（项目根/logs/openbot.lock）
_LOCK_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "logs", "openbot.lock",
)
_handle = None  # 保持引用，防止被 GC 回收导致锁失效


def acquire() -> bool:
    """尝试获取单实例锁。返回 True=成功(可启动)；False=已有实例在运行(拒绝启动)。
    锁机制自身异常时不阻塞启动（返回 True 放行），避免"锁坏了 bot 起不来"。
    """
    global _handle
    try:
        if sys.platform == "win32":
            import ctypes
            k32 = ctypes.WinDLL("kernel32", use_last_error=True)
            k32.CreateMutexW.restype = ctypes.c_void_p
            h = k32.CreateMutexW(None, False, _MUTEX_NAME)
            if not h:
                logger.warning("🔒 创建单实例互斥失败，放行启动")
                return True
            if ctypes.get_last_error() == 183:  # ERROR_ALREADY_EXISTS
                return False
            _handle = h
            return True
        else:
            import fcntl
            os.makedirs(os.path.dirname(_LOCK_FILE), exist_ok=True)
            lock_fd = open(_LOCK_FILE, "w")
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                lock_fd.close()
                return False
            _handle = lock_fd
            return True
    except Exception as e:
        logger.error(f"🔒 单实例锁获取异常: {e}，放行启动")
        return True


def release() -> None:
    """显式释放锁（正常退出时调用；进程崩溃/被杀时由 OS 自动释放）"""
    global _handle
    try:
        if _handle is not None:
            if sys.platform == "win32":
                import ctypes
                ctypes.windll.kernel32.CloseHandle(_handle)
            else:
                _handle.close()
            _handle = None
    except Exception:
        pass
