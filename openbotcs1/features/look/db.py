# openbot\features\look\db.py
"""
【look 插件数据层】群文件查看器专用（web_requests 请求队列 + dialogs 对话列表）。

职责：
  1. 数据库文件：download/look.db（与 core 的 media_cache.db / download_tasks.db 同目录，
     重置数据时删 download/ 目录即可全清）
  2. 建表：dialogs（对话列表）、web_requests（web 请求队列）【group_stats 已迁移到 core/media_cache】
  3. 连接工具：_db_conn（WAL + 30s timeout，解决 bot 与 7777 并发写 locked 问题）
  4. 查询工具：query_db（返回字典行）

注意：group_files（群文件扫描缓存）+ group_stats（群统计）已迁移到 core/media_cache.py 共享缓存，
look 和 downloader 都通过 core 读写，避免重复扫描。

与下载插件的关系：look 只通过 web_requests 表的 type='download' 记录传递下载请求，
downloader 插件读取本表消费下载动作。两个插件互不读写对方的业务表。
"""
import logging
import os
import sqlite3
from datetime import datetime

logger = logging.getLogger(__name__)

# ===================== 路径 =====================
# 本文件位于 <项目根>/features/look/ 下，向上三层即项目根；
# 数据库统一放项目根 download/ 目录（与 media_cache.db / download_tasks.db 集中，重置全清）
# Docker 部署可用环境变量 LOOK_DB_PATH 指定持久化路径（如 /app/data/look.db）
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LOOK_DB_PATH = os.getenv("LOOK_DB_PATH", os.path.join(BASE_DIR, "download", "look.db"))
os.makedirs(os.path.dirname(LOOK_DB_PATH), exist_ok=True)

# ===================== 连接 =====================
def _db_conn():
    """look 数据库连接：WAL 模式 + 30 秒 busy timeout。"""
    conn = sqlite3.connect(LOOK_DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL")
    except Exception:
        pass
    return conn


def query_db(sql, params=()):
    """查询工具：返回字典行列表。"""
    conn = _db_conn()
    try:
        cur = conn.execute(sql, params)
        return cur.fetchall()
    finally:
        conn.close()


def _now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ===================== 建表 =====================
def init_db():
    """初始化 look 数据库，建所有表（幂等）。"""
    conn = _db_conn()
    try:
        # web_requests：web 请求队列（scan/scan_inc/download/sync_dialogs/get_count）
        # download 类型额外带 msg_id/file_name/document_id/file_size，供 downloader 消费
        conn.execute('''CREATE TABLE IF NOT EXISTS web_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT NOT NULL,
            chat_id INTEGER,
            chat_name TEXT,
            msg_id INTEGER DEFAULT 0,
            file_name TEXT DEFAULT '',
            document_id TEXT DEFAULT '',
            file_size INTEGER DEFAULT 0,
            status INTEGER DEFAULT 0,
            result TEXT DEFAULT '',
            created_at TEXT,
            updated_at TEXT
        )''')
        # dialogs：对话列表缓存（群/频道/用户，用于群文件查询的群列表）
        conn.execute('''CREATE TABLE IF NOT EXISTS dialogs (
            chat_id INTEGER PRIMARY KEY,
            chat_name TEXT,
            chat_type TEXT,
            username TEXT DEFAULT '',
            updated_at TEXT
        )''')
        # system_status：系统级状态（key-value）
        # 当前用途：mtproto_status = logged_in / not_logged_in / connecting
        # 由主进程（main.py / login_manager）写入，look_server 独立进程读取用于页面提示
        conn.execute('''CREATE TABLE IF NOT EXISTS system_status (
            key TEXT PRIMARY KEY,
            value TEXT DEFAULT '',
            updated_at TEXT
        )''')
        # group_stats 已迁移到 core/media_cache.py（download/media_cache.db）：
        # 重置数据时删 download/ 目录即可全清，look 只读不持有
        conn.commit()
        logger.info(f"✅ look 数据库已就绪: {LOOK_DB_PATH}")
    finally:
        conn.close()


# ===================== 系统状态读写 =====================
def set_status(key, value):
    """写入系统状态（幂等 upsert），供 look_server 等独立进程读取。"""
    conn = _db_conn()
    try:
        conn.execute(
            "INSERT INTO system_status (key, value, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
            (key, value, _now()),
        )
        conn.commit()
    finally:
        conn.close()


def get_status(key, default=""):
    """读取系统状态，不存在返回 default。"""
    rows = query_db("SELECT value FROM system_status WHERE key=?", (key,))
    return rows[0]["value"] if rows else default


# 兼容旧代码调用名
_ensure_tables = init_db
