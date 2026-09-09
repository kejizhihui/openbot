# core/media_cache.py
"""
全项目共享数据缓存（download/media_cache.db）：
  1. group_files : 群文件扫描明细（look 展示 + downloader 下载共用）
  2. group_stats : 群媒体分类统计（图/视/音/文 count 聚合值，look web 显示）
统一放 download/ 目录：重置数据时删整个目录即可全清。

【v1.2 整改】group_files 新增 media_type / hashtag 列（幂等迁移）：
  - media_type : 扫描器按 mime 判定的分类（photo/video/audio/document），
                 web 分类直接用它，与扫描器同一口径（根治"识别数不一致"）。
  - hashtag    : 消息正文中的 #标签（空格分隔），供筛选/搜索。
"""
import sqlite3
import os
from datetime import datetime

DB_PATH = os.path.join("download", "media_cache.db")


def _conn():
    """获取连接（WAL模式，多进程安全）"""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    return c


def init_db():
    """建表（幂等）"""
    conn = _conn()
    conn.execute('''CREATE TABLE IF NOT EXISTS group_files (
        chat_id    INTEGER,
        chat_name  TEXT,
        msg_id     INTEGER,
        file_name  TEXT,
        document_id TEXT,
        file_size  INTEGER,
        source     TEXT DEFAULT '',
        invalid    INTEGER DEFAULT 0,
        scanned_at TEXT,
        PRIMARY KEY (chat_id, msg_id)
    )''')
    # group_stats：群媒体分类统计（Telegram count 聚合值，扫描前即可显示）
    conn.execute("CREATE TABLE IF NOT EXISTS group_stats ("
                 "chat_id INTEGER PRIMARY KEY,"
                 "photo INTEGER DEFAULT 0,"
                 "video INTEGER DEFAULT 0,"
                 "audio INTEGER DEFAULT 0,"
                 "file INTEGER DEFAULT 0,"
                 "unavailable INTEGER DEFAULT 0,"
                 "updated_at TEXT)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_gf_chat ON group_files(chat_id)")
    # 迁移：v1.5 补 grouped_id 列（媒体组/相册 ID，聚合转发用）
    try:
        conn.execute("ALTER TABLE group_files ADD COLUMN grouped_id INTEGER DEFAULT 0")
    except Exception:
        pass
    # 迁移：旧库补 unavailable 列（已存在则忽略）
    try:
        conn.execute("ALTER TABLE group_stats ADD COLUMN unavailable INTEGER DEFAULT 0")
    except Exception:
        pass
    # 迁移：旧库补 group_files.invalid 列（源频道不可用空壳标记）
    try:
        conn.execute("ALTER TABLE group_files ADD COLUMN invalid INTEGER DEFAULT 0")
    except Exception:
        pass
    # 迁移：v1.2 补 media_type 列（扫描器 mime 分类，web 分类统一口径）
    try:
        conn.execute("ALTER TABLE group_files ADD COLUMN media_type TEXT DEFAULT ''")
    except Exception:
        pass
    # 迁移：v1.2 补 hashtag 列（消息 #标签）
    try:
        conn.execute("ALTER TABLE group_files ADD COLUMN hashtag TEXT DEFAULT ''")
    except Exception:
        pass
    # 分类筛选索引（推广转发/群查询按 media_type 过滤加速）
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_gf_type ON group_files(chat_id, media_type)")
    except Exception:
        pass
    conn.commit()
    conn.close()


def get_max_msg_id(chat_id):
    """返回该群已扫描的最大 msg_id（增量扫描起点），没有则返回 0"""
    conn = _conn()
    row = conn.execute("SELECT MAX(msg_id) AS m FROM group_files WHERE chat_id=?", (chat_id,)).fetchone()
    conn.close()
    return row["m"] if row and row["m"] else 0


def get_files(chat_id):
    """返回该群所有已扫描文件（按 msg_id 倒序）"""
    conn = _conn()
    rows = conn.execute(
        "SELECT * FROM group_files WHERE chat_id=? ORDER BY msg_id DESC",
        (chat_id,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def add_files(records):
    """批量插入/更新，PRIMARY KEY(chat_id,msg_id) 自动去重。返回新增/更新条数
    v1.2：写入 media_type / hashtag 列（扫描器口径，web 直接使用）。
    v1.5：写入 grouped_id 列（媒体组/相册 ID，聚合转发用）。"""
    if not records:
        return 0
    conn = _conn()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn.executemany(
        "INSERT OR REPLACE INTO group_files "
        "(chat_id, chat_name, msg_id, file_name, document_id, file_size, source, invalid, scanned_at, media_type, hashtag, grouped_id) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            (
                r["chat_id"], r.get("chat_name", ""), r["msg_id"],
                r.get("file_name", ""), r.get("document_id", ""),
                r.get("file_size", 0), r.get("source", ""),
                1 if r.get("invalid") else 0, now,
                r.get("media_type", "") or "", r.get("hashtag", "") or "",
                r.get("grouped_id") or 0,
            )
            for r in records
        ],
    )
    conn.commit()
    conn.close()
    return len(records)


def add_invalid_files(chat_id, chat_name, msg_ids):
    """批量写入源频道不可用的空壳消息（无媒体：无文件名/无document_id/无大小）。
    INSERT OR IGNORE：同 msg_id 已有有效记录时不覆盖。返回新增条数"""
    if not msg_ids:
        return 0
    conn = _conn()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn.executemany(
        "INSERT OR IGNORE INTO group_files "
        "(chat_id, chat_name, msg_id, file_name, document_id, file_size, source, invalid, scanned_at) "
        "VALUES (?,?,?,?,?,?,?,1,?)",
        [(chat_id, chat_name or "", m, "", "", 0, "invalid", now) for m in msg_ids],
    )
    conn.commit()
    conn.close()
    return len(msg_ids)


def clear_chat(chat_id):
    """清空该群的扫描缓存（全量重扫前调用），返回删除条数"""
    conn = _conn()
    n = conn.execute("DELETE FROM group_files WHERE chat_id=?", (chat_id,)).rowcount
    conn.commit()
    conn.close()
    return n


def count_files(chat_id):
    """返回该群已扫描文件数"""
    conn = _conn()
    row = conn.execute("SELECT COUNT(*) AS c FROM group_files WHERE chat_id=?", (chat_id,)).fetchone()
    conn.close()
    return row["c"] if row else 0


def get_invalid_count(chat_id):
    """返回该群失效（源频道不可用空壳）文件数 —— 统一对账口径以 group_files.invalid 为准"""
    conn = _conn()
    try:
        row = conn.execute("SELECT COUNT(*) AS c FROM group_files WHERE chat_id=? AND invalid=1", (chat_id,)).fetchone()
        return row["c"] if row else 0
    except Exception:
        return 0
    finally:
        conn.close()


def get_all_chats():
    """返回所有有扫描记录的群：chat_id, chat_name, 文件数, 最后扫描时间"""
    conn = _conn()
    rows = conn.execute(
        "SELECT chat_id, chat_name, COUNT(*) AS cnt, MAX(scanned_at) AS last_scan "
        "FROM group_files GROUP BY chat_id ORDER BY cnt DESC"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ===================== group_stats：群媒体分类统计 =====================

def upsert_group_stats(chat_id, photo, video, audio, file):
    """写入/更新群统计（count 聚合值），ON CONFLICT 保留 unavailable，返回是否成功"""
    conn = _conn()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(
        "INSERT INTO group_stats (chat_id, photo, video, audio, file, updated_at) VALUES (?,?,?,?,?,?) "
        "ON CONFLICT(chat_id) DO UPDATE SET photo=excluded.photo, video=excluded.video, "
        "audio=excluded.audio, file=excluded.file, updated_at=excluded.updated_at",
        (chat_id, int(photo or 0), int(video or 0), int(audio or 0), int(file or 0), now),
    )
    conn.commit()
    conn.close()
    return True


def update_group_unavailable(chat_id, unavailable):
    """更新源频道不可用（无媒体空壳消息）数量，只改 unavailable 列，不动其它统计。
    【deprecated】v1.2 起对账口径统一为 group_files.invalid（见 get_invalid_count），
    本函数保留仅为兼容旧数据，不再作为 web 对账来源。"""
    conn = _conn()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(
        "INSERT INTO group_stats (chat_id, photo, video, audio, file, unavailable, updated_at) "
        "VALUES (?,0,0,0,0,?,?) "
        "ON CONFLICT(chat_id) DO UPDATE SET unavailable=excluded.unavailable, updated_at=excluded.updated_at",
        (chat_id, int(unavailable or 0), now),
    )
    conn.commit()
    conn.close()
    return True


def get_group_stats(chat_id):
    """返回某群统计 dict，没有则返回 None"""
    conn = _conn()
    row = conn.execute(
        "SELECT chat_id, photo, video, audio, file, unavailable, updated_at FROM group_stats WHERE chat_id=?",
        (chat_id,),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def get_all_group_stats():
    """返回全部群统计 {chat_id: {photo,video,audio,file,updated_at}}"""
    conn = _conn()
    rows = conn.execute(
        "SELECT chat_id, photo, video, audio, file, unavailable, updated_at FROM group_stats"
    ).fetchall()
    conn.close()
    return {r["chat_id"]: dict(r) for r in rows}


def clear_group_stats():
    """清空全部群统计（强制重新获取 count 聚合值），返回删除条数"""
    conn = _conn()
    n = conn.execute("DELETE FROM group_stats").rowcount
    conn.commit()
    conn.close()
    return n
