# openbot\core\database.py
"""
【core 数据层】全项目统一数据库入口（插件只依赖本模块，不互相依赖）。

职责：
  1. 路径与连接：BASE_DIR / DOWNLOAD_DIR / DB_PATH / _db_conn（WAL + 30s timeout，
     解决 bot 主进程与 7777 Web 独立进程并发写同一 SQLite 文件的 locked 问题）
  2. 主表结构：init_db() 建 jobs/tasks 表 + 历史迁移
  3. 数据操作：jobs（create/update/get/list/delete）、tasks（add/update/pending/stats）
  4. 全局去重：_extract_media_id / is_media_downloaded / update_task_document_id
  5. 展示工具：format_size

插件自己的专属表（如 look 插件的 group_files/web_requests/dialogs）由各插件自建，
core 不感知也不管理它们。
"""
import logging
import os
import sqlite3
from datetime import datetime

from telethon import types

logger = logging.getLogger(__name__)

# ===================== 路径 =====================
# core/database.py 位于 <项目根>/core/ 下，向上两层即项目根（与 features/downloader/ 不同，那里是三层）
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOWNLOAD_DIR = os.path.join(BASE_DIR, "download")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
DB_PATH = os.path.join(DOWNLOAD_DIR, "download_tasks.db")

# ===================== 状态常量 =====================
# 任务状态
JOB_PENDING = 0      # 待扫描
JOB_SCANNING = 1     # 扫描中
JOB_WAIT_DL = 2      # 待下载
JOB_DOWNLOADING = 3  # 下载中
JOB_DONE = 4         # 已完成
JOB_PAUSED = 5       # 已暂停
JOB_CANCELLED = 6    # 已取消

# 文件状态
TASK_PENDING = 0
TASK_DOWNLOADING = 1
TASK_DONE = 2
TASK_FAILED = 3
TASK_SKIPPED = 4    # 已删除/无媒体（源文件已失效，跳过下载）


def _db_conn():
    """统一数据库连接：WAL 模式 + 30 秒 busy timeout。"""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
    except Exception:
        pass
    return conn


def init_db():
    """初始化数据库，自动升级表结构"""
    conn = _db_conn()
    # jobs 表：任务主表
    conn.execute('''CREATE TABLE IF NOT EXISTS jobs (
        jid INTEGER PRIMARY KEY AUTOINCREMENT,
        type TEXT NOT NULL,
        source TEXT,
        tag TEXT,
        status INTEGER DEFAULT 0,
        user_chat_id INTEGER,
        last_msg_id INTEGER DEFAULT 0,
        last_scanned_id INTEGER DEFAULT 0,
        created_at TEXT,
        updated_at TEXT
    )''')
    # tasks 表：文件明细
    conn.execute('''CREATE TABLE IF NOT EXISTS tasks (
        tid INTEGER PRIMARY KEY AUTOINCREMENT,
        jid INTEGER NOT NULL,
        msg_id INTEGER,
        chat_id INTEGER,
        chat_name TEXT,
        file_name TEXT,
        file_size INTEGER DEFAULT 0,
        status INTEGER DEFAULT 0,
        downloaded_bytes INTEGER DEFAULT 0,
        created_at TEXT,
        updated_at TEXT,
        document_id TEXT,
        source_id TEXT
    )''')
    # 兼容旧表：添加 document_id 列（媒体全局唯一ID，用于全局去重）
    try:
        cols = [row[1] for row in conn.execute("PRAGMA table_info(tasks)").fetchall()]
        if "document_id" not in cols:
            conn.execute("ALTER TABLE tasks ADD COLUMN document_id TEXT")
    except Exception:
        pass
    # 兼容旧表：添加 source_id 列（真实来源ID：转发任务=转发来源群/用户ID，非转发=所在会话ID）
    try:
        cols = [row[1] for row in conn.execute("PRAGMA table_info(tasks)").fetchall()]
        if "source_id" not in cols:
            conn.execute("ALTER TABLE tasks ADD COLUMN source_id TEXT")
    except Exception:
        pass
    # 兼容旧表：添加扫描进度字段（用于扫描中途关闭后续扫）
    try:
        cols = [row[1] for row in conn.execute("PRAGMA table_info(jobs)").fetchall()]
        if "scan_progress_id" not in cols:
            conn.execute("ALTER TABLE jobs ADD COLUMN scan_progress_id INTEGER DEFAULT NULL")
        if "scan_start_id" not in cols:
            conn.execute("ALTER TABLE jobs ADD COLUMN scan_start_id INTEGER DEFAULT NULL")
    except Exception:
        pass
    # 兼容旧表：如果存在旧表 dl_tasks 和 active_jobs，保留但不再使用
    conn.commit()
    conn.close()


# ===================== 数据库操作 =====================
def _now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def create_job(job_type, source, tag, user_chat_id):
    """创建任务，返回 jid"""
    conn = _db_conn()
    now = _now()
    cursor = conn.execute(
        "INSERT INTO jobs (type, source, tag, status, user_chat_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (job_type, source, tag, JOB_PENDING, user_chat_id, now, now)
    )
    jid = cursor.lastrowid
    conn.commit()
    conn.close()
    return jid


def update_job_status(jid, status):
    conn = _db_conn()
    conn.execute("UPDATE jobs SET status = ?, updated_at = ? WHERE jid = ?", (status, _now(), jid))
    conn.commit()
    conn.close()


def update_job_progress(jid, last_msg_id=None, last_scanned_id=None):
    conn = _db_conn()
    sets, vals = [], []
    if last_msg_id is not None:
        sets.append("last_msg_id = ?"); vals.append(last_msg_id)
    if last_scanned_id is not None:
        sets.append("last_scanned_id = ?"); vals.append(last_scanned_id)
    sets.append("updated_at = ?"); vals.append(_now())
    vals.append(jid)
    conn.execute(f"UPDATE jobs SET {', '.join(sets)} WHERE jid = ?", vals)
    conn.commit()
    conn.close()


def get_job(jid):
    conn = _db_conn()
    row = conn.execute("SELECT * FROM jobs WHERE jid = ?", (jid,)).fetchone()
    conn.close()
    if not row: return None
    return {
        "jid": row[0], "type": row[1], "source": row[2], "tag": row[3],
        "status": row[4], "user_chat_id": row[5], "last_msg_id": row[6],
        "last_scanned_id": row[7], "created_at": row[8], "updated_at": row[9]
    }


def list_jobs(status_filter=None, limit=200):
    """列出所有任务，可选状态过滤（默认最多 200 条，避免任务多时 /dls 列表被截断）"""
    conn = _db_conn()
    if status_filter is not None:
        rows = conn.execute("SELECT * FROM jobs WHERE status = ? ORDER BY jid DESC", (status_filter,)).fetchall()
    else:
        rows = conn.execute("SELECT * FROM jobs ORDER BY jid DESC LIMIT ?", (limit,)).fetchall()
    conn.close()
    result = []
    for row in rows:
        result.append({
            "jid": row[0], "type": row[1], "source": row[2], "tag": row[3],
            "status": row[4], "user_chat_id": row[5], "last_msg_id": row[6],
            "last_scanned_id": row[7], "created_at": row[8], "updated_at": row[9]
        })
    return result


def delete_job(jid):
    """删除任务及其所有文件记录（仅数据层；运行时状态清理见 core.download_engine）"""
    conn = _db_conn()
    conn.execute("DELETE FROM tasks WHERE jid = ?", (jid,))
    conn.execute("DELETE FROM jobs WHERE jid = ?", (jid,))
    conn.commit()
    conn.close()


def add_task(jid, msg_id, chat_id, chat_name, file_name="", file_size=0, document_id=None, source_id=None):
    """添加文件记录，返回 tid
    💡 document_id：创建任务时即写入媒体全局唯一ID（Telegram 消息自带），
       让"下载中/失败"的任务也能显示 ID、全局去重更早生效。
    💡 source_id：真实来源ID（转发任务=转发来源群/用户ID，收藏夹/群扫描=自身ID），
       chat_id 保持原存储逻辑不变，web 层用 source_id 显示真实来源群。
    """
    conn = _db_conn()
    now = _now()
    cursor = conn.execute(
        "INSERT INTO tasks (jid, msg_id, chat_id, chat_name, file_name, file_size, status, created_at, updated_at, document_id, source_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (jid, msg_id, chat_id, chat_name, file_name, file_size, TASK_PENDING, now, now, document_id, source_id)
    )
    tid = cursor.lastrowid
    conn.commit()
    conn.close()
    return tid


# ===================== 全局去重（document.id / photo.id） =====================
def _extract_media_id(media):
    """从媒体对象提取 Telegram 全局唯一 ID：
    - document 类（视频/文件/音频）→ document.id（同一文件转发到任何群 ID 不变）
    - photo 类（图片）→ photo.id
    提取失败返回 None（无 ID 时回退到旧的"同名同大小"本地文件检查）
    """
    try:
        if isinstance(media, types.MessageMediaDocument) and media.document:
            return str(media.document.id)
        if isinstance(media, types.MessageMediaPhoto) and media.photo:
            return str(media.photo.id)
    except Exception:
        pass
    return None


def is_media_downloaded(media_id):
    """🚨 全局去重核心：该媒体ID是否已在任意任务中存在（已下载/下载中/待下载）。
    - 跨任务全局查询（不限 jid）→ 同一视频无论在收藏夹还是群里出现，都只下载一次
    - 排除 TASK_FAILED（下载失败允许重新下载重试）
    返回 True 表示应跳过（不再重复下载）
    """
    if not media_id:
        return False
    try:
        conn = _db_conn()
        row = conn.execute(
            "SELECT 1 FROM tasks WHERE document_id = ? AND status != ? LIMIT 1",
            (media_id, TASK_FAILED)
        ).fetchone()
        conn.close()
        return row is not None
    except Exception:
        return False


def update_task_document_id(tid, media_id):
    """下载成功后把媒体ID写入任务记录（供全局去重查询）"""
    if not media_id:
        return
    try:
        conn = _db_conn()
        conn.execute("UPDATE tasks SET document_id = ? WHERE tid = ?", (media_id, tid))
        conn.commit()
        conn.close()
    except Exception:
        pass


def update_task_status(tid, status, downloaded_bytes=None):
    conn = _db_conn()
    sets, vals = ["status = ?", "updated_at = ?"], [status, _now()]
    if downloaded_bytes is not None:
        sets.append("downloaded_bytes = ?"); vals.append(downloaded_bytes)
    vals.append(tid)
    conn.execute(f"UPDATE tasks SET {', '.join(sets)} WHERE tid = ?", vals)
    conn.commit()
    conn.close()


def _persist_progress(tid, current):
    """在后台线程池中执行进度落库，避免 sqlite 同步 IO 阻塞事件循环。
    背景：下载大文件时 progress_cb 若直接调 update_task_status，
    会在事件循环线程里每 0.5s 做一次 connect/commit/close，
    阻塞 PTB 轮询导致命令全部无响应（pending update 堆积）。"""
    try:
        update_task_status(tid, TASK_DOWNLOADING, downloaded_bytes=current)
    except Exception:
        pass


def get_pending_tasks(jid):
    """获取任务中所有待下载的文件"""
    conn = _db_conn()
    rows = conn.execute("SELECT * FROM tasks WHERE jid = ? AND status = ? ORDER BY tid", (jid, TASK_PENDING)).fetchall()
    conn.close()
    result = []
    for row in rows:
        result.append({
            "tid": row[0], "jid": row[1], "msg_id": row[2], "chat_id": row[3],
            "chat_name": row[4], "file_name": row[5], "file_size": row[6],
            "status": row[7], "downloaded_bytes": row[8]
        })
    return result


def get_job_stats(jid):
    """获取任务的文件统计（v1.2 含 skipped：已删除/无媒体）"""
    conn = _db_conn()
    total = conn.execute("SELECT COUNT(*) FROM tasks WHERE jid = ?", (jid,)).fetchone()[0]
    done = conn.execute("SELECT COUNT(*) FROM tasks WHERE jid = ? AND status = ?", (jid, TASK_DONE)).fetchone()[0]
    failed = conn.execute("SELECT COUNT(*) FROM tasks WHERE jid = ? AND status = ?", (jid, TASK_FAILED)).fetchone()[0]
    pending = conn.execute("SELECT COUNT(*) FROM tasks WHERE jid = ? AND status = ?", (jid, TASK_PENDING)).fetchone()[0]
    skipped = conn.execute("SELECT COUNT(*) FROM tasks WHERE jid = ? AND status = ?", (jid, TASK_SKIPPED)).fetchone()[0]
    conn.close()
    return {"total": total, "done": done, "failed": failed, "pending": pending, "skipped": skipped}


# ===================== 展示工具 =====================
def format_size(size_bytes):
    if size_bytes <= 0: return "0 B"
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024: return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"
