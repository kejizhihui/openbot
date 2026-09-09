# core/child_services.py
"""
【core 子进程服务看护器】通用机制：插件注册自己的独立子进程服务，core 统一拉起/看护/停机。

设计原则（插件只依赖 core，core 不感知插件）：
  - core 只提供"注册 + 拉起 + 存活看护 + 停机"的通用能力，不硬编码任何插件路径或端口；
  - 插件在 register() 时调用 register_web_service() 声明自己的子进程服务；
  - 插件未安装 → 无人注册 → core 不会拉起任何东西，天然无报错。

适用场景：look_server 这类 HTTP 服务（serve_forever 阻塞运行，不能塞进 asyncio 事件循环，
必须独立进程；由本模块统一管理，主程序启动时拉起、挂了自动重启、停机时一起关闭）。
"""
import asyncio
import logging
import os
import socket
import subprocess
import sys

logger = logging.getLogger(__name__)

# 项目根（core/child_services.py 位于 <项目根>/core/ 下，向上两层即项目根）
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ===================== 注册表（热重载保留） =====================
if "child_services" not in globals():
    child_services = {}  # name -> {script, host, port, proc, fail_streak}
    _watch_started = False


def register_web_service(name, script, host="127.0.0.1", port=None):
    """插件注册自己的独立 Web 服务子进程（幂等）。

    参数：
      name   : 服务名（日志标识），如 "群文件查看器"
      script : 脚本路径（相对项目根，如 "features/look/look_server.py"）
      host   : 服务监听地址
      port   : 服务端口（提供时用于"已监听则跳过拉起"探测）
    返回：True=注册成功 / False=脚本不存在或参数非法（不会抛错）
    """
    if port is None:
        logger.warning(f"🌐 服务 {name} 注册失败：未提供 port，无法做端口探测")
        return False
    abs_script = os.path.join(BASE_DIR, script)
    if not os.path.exists(abs_script):
        logger.warning(f"🌐 服务 {name} 注册失败：脚本不存在 {abs_script}（插件可能未安装完整）")
        return False
    # 已注册且配置相同：保留现有运行状态（热重载重复注册不打断）
    old = child_services.get(name)
    if old and old["script"] == script and old["port"] == port and old["proc"] and old["proc"].poll() is None:
        return True
    child_services[name] = {
        "name": name,
        "script": script,
        "abs_script": abs_script,
        "host": host,
        "port": port,
        "proc": (old["proc"] if old else None),
        "fail_streak": 0,
        # 端口被外部实例（如 CMD 直接启动/手动运行）接管时置 True：仅首次提示，避免每轮看护刷屏
        "external_taken": bool(old and old.get("external_taken")),
    }
    logger.info(f"🌐 服务已注册: {name} ({script}, {host}:{port})")
    return True


def get_registered():
    """返回已注册服务名列表（调试/日志用）"""
    return list(child_services.keys())


def _probe(host, port, timeout=1.0):
    """探测端口是否已被监听（有外部实例接管则跳过拉起）"""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _start_one(svc):
    """拉起单个服务子进程（端口已被占用则跳过）"""
    if _probe(svc["host"], svc["port"]):
        logger.info(f"🌐 {svc['name']} 已在运行（{svc['host']}:{svc['port']} 已监听），跳过拉起")
        return False
    try:
        log_dir = os.path.join(BASE_DIR, "logs")
        os.makedirs(log_dir, exist_ok=True)
        log_file = open(os.path.join(log_dir, "child_services.log"), "a", encoding="utf-8")
        flags = subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0
        # 强制子进程以 UTF-8 输出（Windows 默认 GBK，print 中文/emoji 会 UnicodeEncodeError 崩溃）
        env = dict(os.environ)
        env["PYTHONIOENCODING"] = "utf-8"
        proc = subprocess.Popen(
            [sys.executable, svc["abs_script"]],
            cwd=BASE_DIR,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            creationflags=flags,
            env=env,
        )
        svc["proc"] = proc
        svc["fail_streak"] = 0
        logger.info(f"🌐 已拉起 {svc['name']} (PID {proc.pid})")
        return True
    except Exception as e:
        logger.error(f"🌐 {svc['name']} 拉起失败: {e}")
        return False


def start_all():
    """拉起所有已注册且未在运行的服务（主程序启动时调用）"""
    for svc in child_services.values():
        if not svc["proc"] or svc["proc"].poll() is not None:
            _start_one(svc)


async def watch():
    """看护循环：每 15 秒检查所有注册服务，意外退出且端口未被接管时自动重启（退避防崩溃循环）"""
    global _watch_started
    if _watch_started:
        return
    _watch_started = True
    logger.info(f"🌐 子进程看护已启动，正在看护 {len(child_services)} 个服务")
    while True:
        await asyncio.sleep(15)
        for svc in list(child_services.values()):
            try:
                proc = svc["proc"]
                if proc is None or proc.poll() is not None:
                    # 无进程或进程已退出
                    if _probe(svc["host"], svc["port"]):
                        # 端口仍被监听（外部实例接管），不重复拉起
                        svc["proc"] = None
                        # 仅首次提示，之后静默（避免每 15 秒刷屏）
                        if not svc.get("external_taken"):
                            svc["external_taken"] = True
                            logger.warning(f"🌐 {svc['name']} 端口已被外部实例监听（外部托管模式），本实例不再拉起")
                        continue
                    if proc is not None:
                        svc["fail_streak"] += 1
                        delay = min(60, 15 * (2 ** (svc["fail_streak"] - 1)))  # 15/30/60 秒退避
                        logger.warning(f"🌐 {svc['name']} 进程意外退出(rc={proc.returncode})，{delay}s 后重启（连续第 {svc['fail_streak']} 次）")
                        await asyncio.sleep(delay)
                    _start_one(svc)
                else:
                    svc["fail_streak"] = 0
                    # 服务已由本实例托管，若之前判定过外部接管则解除标记
                    svc["external_taken"] = False
            except Exception as e:
                logger.warning(f"🌐 {svc['name']} 看护异常: {e}")


def stop_all():
    """停机：结束所有子进程（先温和 terminate，超时再 kill）"""
    for svc in child_services.values():
        proc = svc["proc"]
        if proc and proc.poll() is None:
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
            logger.info(f"🌐 {svc['name']} 已随主程序停止")
        svc["proc"] = None
