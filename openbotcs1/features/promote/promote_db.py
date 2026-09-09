#openbot\features\promote\promote_db.py
"""推广转发插件专属库（download/promote.db）：promote_configs + promote_tasks

符合项目约定："插件专属表由插件自建，core 不感知"；重置数据仍删 download/ 全清。
- promote_configs : web 上保存的转发配置（可多条并存，含开关/自动监听/推广插入）
- promote_tasks   : 每次执行记录（web 显示进度/结果；status=0 待处理由 bot 轮询领取）
并发说明：look_server 与 main 是两个进程，都读写本库 → WAL + busy_timeout + 写操作 try/finally 关连接，避免 database is locked / 残留写锁。
"""
import os
import sqlite3
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "download", "promote.db")


def _conn():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    try:
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA busy_timeout=8000")
        c.execute("PRAGMA synchronous=NORMAL")
    except Exception:
        pass
    return c


def init_db():
    """建表（幂等）"""
    conn = _conn()
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS promote_configs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT DEFAULT '',
            src_chat_id INTEGER NOT NULL,
            dst_chat_id INTEGER NOT NULL,
            filters TEXT DEFAULT 'all',
            caption_mode INTEGER DEFAULT 0,
            custom_text TEXT DEFAULT '',
            tag INTEGER DEFAULT 0,
            promo_every INTEGER DEFAULT 0,
            promo_mode INTEGER DEFAULT 0,
            promo_src_chat_id INTEGER,
            promo_msg_id INTEGER,
            promo_text TEXT DEFAULT '',
            auto_listen INTEGER DEFAULT 0,
            enabled INTEGER DEFAULT 0,
            created_at TEXT,
            updated_at TEXT
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS promote_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cfg_id INTEGER,
            status INTEGER DEFAULT 0,
            processed INTEGER DEFAULT 0,
            success INTEGER DEFAULT 0,
            success_files INTEGER DEFAULT 0,
            failed INTEGER DEFAULT 0,
            skipped INTEGER DEFAULT 0,
            total INTEGER DEFAULT 0,
            result TEXT DEFAULT '',
            error TEXT DEFAULT '',
            filters TEXT DEFAULT 'all',
            msg_ids TEXT DEFAULT '',
            caption_mode INTEGER DEFAULT 0,
            custom_text TEXT DEFAULT '',
            tag INTEGER DEFAULT 0,
            album INTEGER DEFAULT 1,
            type TEXT DEFAULT 'forward',
            src_task_id INTEGER,
            src_chat_id INTEGER,
            dst_chat_id INTEGER,
            created_at TEXT,
            updated_at TEXT
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS promote_sends (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id INTEGER NOT NULL,
            src_chat_id INTEGER,
            src_msg_id INTEGER,
            dst_chat_id INTEGER NOT NULL,
            dst_msg_id INTEGER NOT NULL,
            kind TEXT DEFAULT 'file',
            created_at TEXT
        )""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sends_task ON promote_sends(task_id)")
        _migrate(conn)
        conn.commit()
    finally:
        conn.close()


def _migrate(conn):
    """老库补列（幂等）：promote_configs.tag、promote_tasks.caption_mode/custom_text/tag"""
    try:
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(promote_configs)")}
        if "tag" not in cols:
            conn.execute("ALTER TABLE promote_configs ADD COLUMN tag INTEGER DEFAULT 0")
        tcols = {r["name"] for r in conn.execute("PRAGMA table_info(promote_tasks)")}
        if "success_files" not in tcols:
            conn.execute("ALTER TABLE promote_tasks ADD COLUMN success_files INTEGER DEFAULT 0")
        if "caption_mode" not in tcols:
            conn.execute("ALTER TABLE promote_tasks ADD COLUMN caption_mode INTEGER DEFAULT 0")
        if "custom_text" not in tcols:
            conn.execute("ALTER TABLE promote_tasks ADD COLUMN custom_text TEXT DEFAULT ''")
        if "tag" not in tcols:
            conn.execute("ALTER TABLE promote_tasks ADD COLUMN tag INTEGER DEFAULT 0")
        if "album" not in tcols:
            conn.execute("ALTER TABLE promote_tasks ADD COLUMN album INTEGER DEFAULT 1")
        if "type" not in tcols:
            conn.execute("ALTER TABLE promote_tasks ADD COLUMN type TEXT DEFAULT 'forward'")
        if "src_task_id" not in tcols:
            conn.execute("ALTER TABLE promote_tasks ADD COLUMN src_task_id INTEGER")
        conn.commit()
    except Exception:
        pass


def _now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def list_configs():
    init_db()
    conn = _conn()
    try:
        return [dict(r) for r in conn.execute("SELECT * FROM promote_configs ORDER BY id DESC").fetchall()]
    finally:
        conn.close()


def get_config(cfg_id):
    init_db()
    conn = _conn()
    try:
        r = conn.execute("SELECT * FROM promote_configs WHERE id=?", (cfg_id,)).fetchone()
        return dict(r) if r else None
    finally:
        conn.close()


def save_config(data):
    """新增或更新配置，返回 cfg_id"""
    init_db()
    conn = _conn()
    try:
        now = _now()
        fields = ("name", "src_chat_id", "dst_chat_id", "filters", "caption_mode",
                  "custom_text", "tag", "promo_every", "promo_mode", "promo_src_chat_id",
                  "promo_msg_id", "promo_text", "auto_listen", "enabled")

        def _v(k):
            v = data.get(k)
            if v is None:
                return v
            if k in ("src_chat_id", "dst_chat_id", "promo_src_chat_id", "promo_msg_id"):
                try:
                    return int(v)
                except (TypeError, ValueError):
                    return None
            if k in ("promo_every",):
                try:
                    return max(0, int(v))
                except (TypeError, ValueError):
                    return 0
            if k in ("caption_mode", "promo_mode"):
                try:
                    return int(v)
                except (TypeError, ValueError):
                    return 0
            if k in ("auto_listen", "enabled", "tag"):
                return 1 if str(v) in ("1", "true", "True", "on") else 0
            return str(v) if v is not None else ""

        cfg_id = data.get("id")
        if cfg_id:
            sets = ", ".join("%s=?" % k for k in fields) + ", updated_at=?"
            vals = [_v(k) for k in fields] + [now]
            conn.execute("UPDATE promote_configs SET %s WHERE id=?" % sets, vals + [int(cfg_id)])
        else:
            cols = ", ".join(fields + ("created_at", "updated_at"))
            ph = ", ".join("?" * (len(fields) + 2))
            conn.execute(
                "INSERT INTO promote_configs (%s) VALUES (%s)" % (cols, ph),
                [_v(k) for k in fields] + [now, now],
            )
            cfg_id = conn.execute("SELECT last_insert_rowid() AS i").fetchone()["i"]
        conn.commit()
        return cfg_id
    finally:
        conn.close()


def delete_config(cfg_id):
    init_db()
    conn = _conn()
    try:
        conn.execute("DELETE FROM promote_configs WHERE id=?", (cfg_id,))
        conn.commit()
    finally:
        conn.close()


def create_task(cfg_id, src_chat_id, dst_chat_id, filters="all", msg_ids="",
                caption_mode=0, custom_text="", tag=0, album=1, task_type="forward", src_task_id=None):
    """创建转发/撤回任务，返回 tid（勾选直转任务 cfg_id=0 时 caption/标签参数存任务行；album=1 媒体组聚合发送）"""
    init_db()
    conn = _conn()
    try:
        now = _now()
        conn.execute(
            "INSERT INTO promote_tasks (cfg_id, status, filters, msg_ids, caption_mode, custom_text, tag, album, type, src_task_id, src_chat_id, dst_chat_id, created_at, updated_at) "
            "VALUES (?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (cfg_id, filters, msg_ids, int(caption_mode or 0), custom_text or "",
             1 if tag else 0, 1 if album else 0, task_type or "forward", src_task_id, src_chat_id, dst_chat_id, now, now),
        )
        tid = conn.execute("SELECT last_insert_rowid() AS i").fetchone()["i"]
        conn.commit()
        return tid
    finally:
        conn.close()


def get_task(tid):
    init_db()
    conn = _conn()
    try:
        r = conn.execute("SELECT * FROM promote_tasks WHERE id=?", (tid,)).fetchone()
        return dict(r) if r else None
    finally:
        conn.close()


def list_pending_tasks(limit=3):
    """待处理任务（status=0），按 id 升序"""
    init_db()
    conn = _conn()
    try:
        rows = conn.execute(
            "SELECT id, cfg_id, src_chat_id, dst_chat_id, filters, msg_ids, type, src_task_id FROM promote_tasks "
            "WHERE status=0 ORDER BY id ASC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def list_tasks(limit=30):
    init_db()
    conn = _conn()
    try:
        rows = conn.execute(
            "SELECT * FROM promote_tasks ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def update_task_progress(tid, **kw):
    """更新任务进度/结果（status/processed/success/failed/skipped/total/result/error）"""
    init_db()
    conn = _conn()
    try:
        if kw:
            sets = ", ".join("%s=?" % k for k in kw) + ", updated_at=?"
            vals = list(kw.values()) + [_now()]
            conn.execute("UPDATE promote_tasks SET %s WHERE id=?" % sets, vals + [tid])
            conn.commit()
    finally:
        conn.close()


def stop_task(tid):
    """请求停止（status=4），由执行器在下一批检查点生效"""
    update_task_progress(tid, status=4, result="已请求停止")



def record_sends(task_id, sends):
    """记录转发成功发送的目标群消息：sends=[(src_chat_id, src_msg_id, dst_chat_id, dst_msg_id, kind), ...]"""
    if not sends:
        return
    init_db()
    conn = _conn()

    def _i(x):
        # 防御：对象（Telethon Message 等）取其 id，其余强制 int，失败返回 None
        try:
            if x is None:
                return None
            if isinstance(x, int):
                return x
            v = getattr(x, "id", x)
            return int(v) if v is not None else None
        except Exception:
            return None

    try:
        now = _now()
        conn.executemany(
            "INSERT INTO promote_sends (task_id, src_chat_id, src_msg_id, dst_chat_id, dst_msg_id, kind, created_at) "
            "VALUES (?,?,?,?,?,?,?)",
            [(_i(task_id), _i(s[0]), _i(s[1]), _i(s[2]), _i(s[3]), s[4] or "file", now) for s in sends],
        )
        conn.commit()
    finally:
        conn.close()


def get_sends_by_task(task_id):
    """某任务的全部发送记录（撤回用）"""
    init_db()
    conn = _conn()
    try:
        rows = conn.execute(
            "SELECT * FROM promote_sends WHERE task_id=? ORDER BY id ASC", (task_id,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def count_sends_by_task(task_id):
    """某任务已发送条数（任务列表显示用）"""
    init_db()
    conn = _conn()
    try:
        r = conn.execute("SELECT COUNT(*) AS c FROM promote_sends WHERE task_id=?", (task_id,)).fetchone()
        return r["c"] if r else 0
    finally:
        conn.close()


def delete_sends_by_task(task_id):
    init_db()
    conn = _conn()
    try:
        conn.execute("DELETE FROM promote_sends WHERE task_id=?", (task_id,))
        conn.commit()
    finally:
        conn.close()


def retry_task(tid):
    """重试：复制原任务参数新建一条待处理任务（status=0），返回新 tid"""
    init_db()
    conn = _conn()
    try:
        r = conn.execute("SELECT * FROM promote_tasks WHERE id=?", (tid,)).fetchone()
        if not r:
            return None
        now = _now()
        conn.execute(
            "INSERT INTO promote_tasks (cfg_id, status, filters, msg_ids, caption_mode, custom_text, tag, album, type, src_chat_id, dst_chat_id, created_at, updated_at) "
            "VALUES (?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (r["cfg_id"], r["filters"] or "all", r["msg_ids"] or "",
             int(r["caption_mode"] or 0), r["custom_text"] or "", 1 if r["tag"] else 0,
             1 if r["album"] else 0, r["type"] or "forward",
             r["src_chat_id"], r["dst_chat_id"], now, now),
        )
        new_id = conn.execute("SELECT last_insert_rowid() AS i").fetchone()["i"]
        conn.commit()
        return new_id
    finally:
        conn.close()


def delete_task(tid):
    """删除任务记录（连同发送记录）"""
    init_db()
    conn = _conn()
    try:
        conn.execute("DELETE FROM promote_sends WHERE task_id=?", (tid,))
        conn.execute("DELETE FROM promote_tasks WHERE id=?", (tid,))
        conn.commit()
    finally:
        conn.close()
