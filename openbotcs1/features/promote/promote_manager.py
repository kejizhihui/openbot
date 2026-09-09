#openbot\features\promote\promote_manager.py
"""推广转发插件入口：命令 /promote + JobQueue 5 秒轮询 promote_tasks + 自动监听（events.NewMessage）

- 只依赖 core（promote_db / promote_engine / command_registry），不 import 其它插件（架构约定）。
- 自动监听：为 enabled+auto_listen 的来源群注册事件；配置变化时重挂（remove→add，防重复触发）。
- 模块级状态（_listen_srcs / _recent）在热重载时会重建：事件只在配置开启后生效，
  重载不会回溯旧消息，故不会重复转发已处理内容；_recent 仅用于防 Telegram 重复投递。
"""
import asyncio
import logging

from telegram import Update
from telegram.ext import CommandHandler, ContextTypes

from core.command_registry import register_handler
from features.promote import promote_db, promote_engine

logger = logging.getLogger(__name__)

# 自动监听状态（模块级，热重载保留性见 docstring）
_listen_srcs = set()      # 当前已监听来源群集合
_recent = []              # 最近处理过的 "src:msg_id"（去重窗口 200 条）
_RECENT_MAX = 200


async def _cmd_promote(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📤 <b>推广转发</b>\n"
        "配置与操作在 Web 端：<code>http://127.0.0.1:7777/promote.html</code>\n"
        "支持：从来源群按文件 id 重发到目标群（绕开禁转）、筛选、附带文字、"
        "每 N 条插入推广消息、自动监听转发。"
    )


# ===================== 自动监听 =====================

async def _on_new_message(event):
    """来源群新消息 → 匹配配置转发（多配置并存时逐条执行）"""
    src = event.chat_id
    try:
        cfgs = [c for c in promote_db.list_configs()
                if c["enabled"] and c["auto_listen"] and c["src_chat_id"] == src]
        if not cfgs:
            return
        msg_id = event.message.id
        key = "%s:%s" % (src, msg_id)
        if key in _recent:
            return
        _recent.append(key)
        if len(_recent) > _RECENT_MAX:
            del _recent[:len(_recent) - _RECENT_MAX]

        client = event.client
        for cfg in cfgs:
            try:
                if not promote_engine._match_media_type(event.message, cfg.get("filters") or "all"):
                    continue
                caption = promote_engine._build_caption(
                    event.message, cfg.get("caption_mode"), cfg.get("custom_text"))
                res = await promote_engine._send_once(
                    client, src, msg_id, cfg["dst_chat_id"], caption=caption)
                if res == "success":
                    logger.info("👂 自动监听转发成功 src=%s msg=%s → dst=%s", src, msg_id, cfg["dst_chat_id"])
                    # 按 promo_every 计数插入推广（每配置独立计数）
                    promo_every = int(cfg.get("promo_every") or 0)
                    if promo_every > 0:
                        _auto_promo_count[cfg["id"]] = _auto_promo_count.get(cfg["id"], 0) + 1
                        if _auto_promo_count[cfg["id"]] % promo_every == 0:
                            await promote_engine._send_promo(client, cfg["dst_chat_id"], cfg)
            except Exception as e:
                logger.warning("⚠️ 自动监听转发失败 cfg=%s msg=%s: %s", cfg.get("id"), msg_id, e)
    except Exception as e:
        logger.warning("⚠️ 自动监听处理异常: %s", e)


_auto_promo_count = {}   # cfg_id → 已转发计数（每 promo_every 插推广）


def _sync_auto_listen(manager):
    """遍历 enabled+auto_listen 配置，重挂来源群监听（差集/变化才动）"""
    global _listen_srcs
    try:
        client = manager.mtproto_client.client if manager.mtproto_client else None
        if not client:
            _listen_srcs = set()
            return
        import telethon.events as tg_events
        target = {c["src_chat_id"] for c in promote_db.list_configs()
                  if c["enabled"] and c["auto_listen"] and c["src_chat_id"]}
        if target == _listen_srcs:
            return
        # 变化 → 全量重挂（remove→add，防重复触发）
        try:
            client.remove_event_handler(_on_new_message)
        except Exception:
            pass
        if target:
            client.add_event_handler(_on_new_message, tg_events.NewMessage(incoming=True, chats=list(target)))
            logger.info("👂 自动监听已更新：%s 个来源群", len(target))
        _listen_srcs = set(target)
    except Exception as e:
        logger.warning("⚠️ 自动监听同步失败: %s", e)


# ===================== 任务轮询 =====================

async def _poll_tasks(context):
    """每 5 秒：同步自动监听 + 领取 promote_tasks(status=0) 执行"""
    manager = context.bot_data.get("manager")
    if not manager:
        return
    try:
        _sync_auto_listen(manager)
        for t in promote_db.list_pending_tasks(limit=3):
            try:
                await promote_engine.run_promote_task(manager, t["id"])
            except Exception as e:
                logger.error("❌ 推广任务 #%s 执行异常: %s", t["id"], e)
                promote_db.update_task_progress(t["id"], status=3, error=str(e))
    except Exception as e:
        logger.warning("⚠️ 推广轮询异常: %s", e)


# ===================== 插件入口 =====================

def register(manager):
    promote_db.init_db()
    register_handler(CommandHandler("promote", _cmd_promote), __name__)
    try:
        job_queue = manager.bot_app.job_queue
        if job_queue is not None:
            job_queue.run_repeating(_poll_tasks, interval=5.0, first=10.0)
    except Exception as e:
        logger.warning("⚠️ 推广任务轮询挂载失败: %s", e)
    logger.info("📤 推广转发插件已注册（Web: /promote.html）")
