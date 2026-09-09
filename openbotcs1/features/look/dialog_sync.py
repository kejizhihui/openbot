# openbot\features\look\dialog_sync.py
"""
对话列表自动同步（供 7777 Web 查看器显示"全部群聊"）。

- 通过 MTProto client.iter_dialogs() 拉取账号的全部对话（群/频道/收藏夹/用户）
- 写入 download_tasks.db 的 dialogs 表（与下载任务同库，web 只读）
- 由 look_manager.register() 挂到 PTB JobQueue：启动后 30 秒首次，之后每 DIALOGS_REFRESH_MIN 分钟刷新
  （默认 30 分钟，可在 .env 配置 DIALOGS_REFRESH_MIN）
- 全程容错：MTProto 未就绪/拉取失败只记日志，不影响 bot 主流程
"""
import logging
import sqlite3

from features.look.db import LOOK_DB_PATH as DB_PATH, _db_conn

logger = logging.getLogger(__name__)

# 默认刷新间隔（分钟），.env 可覆盖
DEFAULT_REFRESH_MIN = 30

# Telegram 实体类型 → 中文显示名
_TYPE_MAP = {
    "Channel": "频道",
    "Chat": "群",
    "User": "用户",
    "Saved": "收藏夹",
}


def _ensure_dialogs_table():
    """确保 dialogs 表存在（幂等，兼容旧库）"""
    conn = _db_conn()
    try:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS dialogs (
                chat_id INTEGER PRIMARY KEY,
                chat_name TEXT,
                chat_type TEXT,
                username TEXT,
                updated_at TEXT
            )"""
        )
        conn.commit()
    finally:
        conn.close()


async def sync_dialogs(manager):
    """拉取全部对话并写入 dialogs 表（容错：任何异常只记日志）"""
    try:
        if not manager or not manager.mtproto_client or not manager.mtproto_client.client:
            logger.warning("⚠️ MTProto 未就绪，跳过对话列表同步")
            return 0

        if not await manager.mtproto_client.ensure_ready():
            logger.warning("⚠️ MTProto 未就绪，跳过对话列表同步（未登录请先 /mtlogin）")
            return 0

        client = manager.mtproto_client.client
        _ensure_dialogs_table()

        # 提前拿自己的 user_id（收藏夹识别用，避免循环里反复请求）
        try:
            me_id = (await client.get_me()).id
        except Exception:
            me_id = None

        rows = []
        async for dlg in client.iter_dialogs(limit=None):
            try:
                ent = dlg.entity
                chat_id = int(dlg.id)
                # 收藏夹（Saved Messages）：Telethon 里是 User(is_self=True)，
                # type(ent).__name__ 是 "User" 不是 "Saved"，需主动识别
                is_saved = bool(getattr(ent, 'is_self', False)) or (me_id is not None and dlg.id == me_id)
                if is_saved:
                    name = "收藏夹 (Saved Messages)"
                    ctype = "收藏夹"
                else:
                    # 名称：群/频道取 title，用户取 first_name，最后兜底 dlg.name
                    name = (getattr(ent, 'title', None)
                            or getattr(ent, 'first_name', '')
                            or getattr(ent, 'name', '')
                            or "未知")
                    ctype = _TYPE_MAP.get(type(ent).__name__, type(ent).__name__)
                username = getattr(ent, 'username', None)
                rows.append((chat_id, name, ctype, username))
            except Exception:
                continue  # 单条解析失败跳过，不中断整体

        if not rows:
            logger.warning("⚠️ 对话列表为空（可能账号无任何对话）")
            return 0

        from datetime import datetime
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn = _db_conn()
        try:
            conn.executemany(
                "INSERT OR REPLACE INTO dialogs (chat_id, chat_name, chat_type, username, updated_at) VALUES (?, ?, ?, ?, ?)",
                [(cid, n, t, u, now) for (cid, n, t, u) in rows]
            )
            conn.commit()
        finally:
            conn.close()

        logger.info(f"✅ 对话列表已同步：{len(rows)} 个对话（群/频道/用户）")
        return len(rows)
    except Exception as e:
        logger.error(f"❌ 对话列表同步失败: {e}", exc_info=True)
        return 0


def register_dialog_sync(manager):
    """挂载定时同步：启动后 30 秒首次，之后每 N 分钟刷新（PTB JobQueue / APScheduler）"""
    try:
        job_queue = manager.bot_app.job_queue
        if job_queue is None:
            logger.warning("⚠️ JobQueue 不可用，对话列表不会自动刷新（手动 /look dialogs_sync 也无法触发）")
            return

        interval_min = int(manager.config.get("DIALOGS_REFRESH_MIN", DEFAULT_REFRESH_MIN) or DEFAULT_REFRESH_MIN)
        interval_sec = max(60, interval_min * 60)  # 下限 1 分钟，防误配

        async def _sync_job(context):
            m = context.bot_data.get('manager')
            if m:
                await sync_dialogs(m)

        job_queue.run_repeating(_sync_job, interval=interval_sec, first=30.0)
        logger.info(f"🗓️ 对话列表自动同步已挂载：首次 30 秒后，之后每 {interval_min} 分钟刷新")
    except Exception as e:
        logger.error(f"❌ 对话列表定时同步挂载失败: {e}")
