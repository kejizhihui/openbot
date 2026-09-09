#openbot\features\promote\promote_engine.py
"""推广转发执行器：清单（media_cache.group_files）→ get_messages 取媒体 → send_file 重发 → 推广插入

关键点（与用户确认过的逻辑）：
- 用 get_messages(chat_id, ids=msg_id) 重新拉完整媒体对象，Telethon 自动带回 access_hash/file_reference，
  无需入库存储 → 目标群禁转也能 send_file 触发（不是 forward）。
- 无媒体/已删除 → skipped（计入进度，不影响其它）。
- 连续发送间隔 3 秒起步（可调）；FloodWait 等待；网络错误复用 download_engine 的断网恢复机制。
- 每 promo_every 条成功插入 1 条推广消息（指定消息转发 / 自定义文案两种模式）。
- v1.5 聚合模式（album=1，默认）：按 grouped_id 把同一媒体组（相册）的多条消息
  用 send_file(album=True) 合成一条相册发送，勾选集合内聚合（没勾的同组消息不发送）；
  相册文字只带组第一条（Telegram 平台限制）；album 失败自动回退逐条单发。
"""
import asyncio
import logging
import re

from telethon.errors import FloodWaitError

from core import media_cache
from core.download_engine import _is_network_error, on_network_error, _wait_for_network

logger = logging.getLogger(__name__)

TAG_RE = re.compile(r'#[\w\u4e00-\u9fa5]+')


def _extract_tags(text):
    """提取消息文字中的 #标签，空格连接；无标签返回 ''"""
    if not text:
        return ""
    return " ".join(TAG_RE.findall(text))

SEND_INTERVAL = 3.0       # 连续发送间隔（秒），可调
MAX_TASK_SEND = 2000      # 单任务最大发送条数（防风控/失控）
CHECK_EVERY = 50          # 每 N 条检查一次停止请求
PROGRESS_EVERY = 10       # 每 N 条回写一次进度
FETCH_BATCH = 100         # 聚合模式批量拉取消息的分批大小


def _parse_filters(filters):
    """filters 形如 'photo,video' 或 'all'；返回 set 或 None（all）"""
    f = (filters or "all").strip().lower()
    if f in ("", "all"):
        return None
    return {x.strip() for x in f.split(",") if x.strip()}


def _load_source_list(src_chat_id, filters):
    """从共享缓存取来源群文件清单（invalid=0，media_type 匹配 filters，msg_id 倒序）。
    与群文件查询共用一份缓存（用户确认的口径）。"""
    where = "chat_id=? AND invalid=0"
    params = [src_chat_id]
    mt = _parse_filters(filters)
    if mt is not None:
        in_list = ",".join("'%s'" % x for x in sorted(mt))
        where += " AND media_type IN (%s)" % in_list
    conn = media_cache._conn()
    try:
        rows = conn.execute(
            "SELECT chat_id, msg_id, file_name, media_type, hashtag, grouped_id FROM group_files "
            "WHERE %s ORDER BY msg_id DESC" % where,
            params,
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def _detect_media_type(msg):
    """按 Telegram 媒体类型判断分类（与扫描器口径一致）"""
    if not msg or not msg.media:
        return "text"
    m = msg.media
    import telethon
    if isinstance(m, telethon.tl.types.MessageMediaPhoto):
        return "photo"
    if isinstance(m, telethon.tl.types.MessageMediaDocument):
        d = m.document
        if d and d.mime_type:
            mt = d.mime_type.lower()
            if mt.startswith("video/"):
                return "video"
            if mt.startswith("audio/"):
                return "audio"
        return "document"
    return "document"


def _match_media_type(event_msg, filters):
    """自动监听时判断新消息是否匹配筛选（photo/video/audio/document）"""
    mt = _parse_filters(filters)
    if mt is None:
        return True
    return _detect_media_type(event_msg) in mt


def _build_caption(m, caption_mode, custom_text, with_tag=False):
    """附带文字四模式：0=不带 1=保留原文(含#标签原样) 2=自定义 3=原样+追加
    with_tag=True 时：0 → 只发提取的#标签；1 → 原文（标签已在原文）；2 → 标签+自定义；3 → 原文+自定义"""
    original = ""
    if m is not None and getattr(m, "message", None):
        original = m.message or ""
    caption_mode = int(caption_mode or 0)
    custom_text = (custom_text or "").strip()
    tags = _extract_tags(original) if with_tag else ""
    if caption_mode == 0:
        return tags or None
    if caption_mode == 1:
        return original or None
    if caption_mode == 2:
        if tags and custom_text:
            return "%s %s" % (tags, custom_text)
        return (tags or custom_text) or None
    if caption_mode == 3:
        if original and custom_text:
            return "%s\n%s" % (original, custom_text)
        return (original or custom_text) or None
    return None


async def _send_once(client, src_chat_id, msg_id, dst_chat_id, caption=None):
    """单条重发：get_messages 重新拉完整媒体（自动带 access_hash）→ send_file。
    返回 'success' / 'skipped' / 'failed'；网络错误抛出由上层处理。"""
    try:
        m = await client.get_messages(src_chat_id, ids=msg_id)
        if not m or not m.media:
            return "skipped"
        await client.send_file(dst_chat_id, m.media, caption=caption or None)
        return "success"
    except FloodWaitError as e:
        logger.warning("⏳ FloodWait %s 秒，等待后继续", e.seconds)
        await asyncio.sleep(min(int(getattr(e, "seconds", 30) or 30), 300))
        return "failed"
    except Exception as e:
        if _is_network_error(e):
            raise
        logger.error("❌ 重发失败 msg=%s: %s", msg_id, e)
        return "failed"


async def _fetch_msgs_batch(client, src_chat_id, msg_ids, task_id=None):
    """聚合模式批量拉取消息（每批 FETCH_BATCH 条），返回 {msg_id: msg}。
    网络错误走断网恢复后重试（最多 3 次）；FloodWait 等待。"""
    out = {}
    for i in range(0, len(msg_ids), FETCH_BATCH):
        chunk = msg_ids[i:i + FETCH_BATCH]
        retry = 0
        while True:
            try:
                msgs = await client.get_messages(src_chat_id, ids=chunk)
                for m in msgs:
                    if m:
                        out[m.id] = m
                break
            except FloodWaitError as e:
                logger.warning("⏳ FloodWait %s 秒，等待后继续", e.seconds)
                await asyncio.sleep(min(int(getattr(e, "seconds", 30) or 30), 300))
                retry += 1
                if retry >= 3:
                    logger.error("❌ FloodWait 多次后放弃本批 %s 条", len(chunk))
                    break
            except Exception as e:
                if _is_network_error(e) and retry < 3:
                    retry += 1
                    on_network_error()
                    if task_id:
                        await _wait_for_network(task_id)
                    else:
                        await asyncio.sleep(15)
                    continue
                logger.warning("⚠️ 批量拉取消息失败（%s 条段）: %s", len(chunk), e)
                break
    return out


async def _send_album(client, group_msgs, dst_chat_id, caption=None):
    """媒体组相册发送：同一 grouped_id 的多条媒体 → send_file(album=True) 合成一条相册。
    失败自动回退逐条单发（保证不丢）。返回 (ok, sent_msgs)：sent_msgs 为目标群收到的消息列表。"""
    media_list = [m.media for m in group_msgs if m and m.media]
    if not media_list:
        return False, []
    try:
        sent = await client.send_file(dst_chat_id, media_list, caption=caption or None)
        if isinstance(sent, (list, tuple)):
            return True, [m for m in sent if m]
        return True, ([sent] if sent else [])
    except FloodWaitError as e:
        logger.warning("⏳ FloodWait %s 秒，等待后继续", e.seconds)
        await asyncio.sleep(min(int(getattr(e, "seconds", 30) or 30), 300))
        raise
    except Exception as e:
        if _is_network_error(e):
            raise
        # album 失败回退：逐条单发（数量对不上也保证内容送达）
        logger.warning("⚠️ 相册发送失败（%s 条）回退逐条: %s", len(group_msgs), e)
        ok = 0
        sent_msgs = []
        for m in group_msgs:
            if not m or not m.media:
                continue
            try:
                sm = await client.send_file(dst_chat_id, m.media)
                ok += 1
                if sm:
                    sent_msgs.append(sm)
            except Exception as e2:
                logger.warning("⚠️ 相册回退单发失败 msg=%s: %s", m.id, e2)
        return ok > 0, sent_msgs


async def _send_promo(client, dst_chat_id, cfg, task_id=None):
    """按配置插入 1 条推广消息：
    promo_mode=0 → 转发指定内容（群链接+消息ID：promo_src_chat_id + promo_msg_id）；
    promo_mode=1 → 发送自定义文案 promo_text。task_id 非空时记录发送（撤回可覆盖推广消息）。"""
    from features.promote import promote_db as pdb
    rec = []
    try:
        mode = int(cfg.get("promo_mode") or 0)
        src = cfg.get("promo_src_chat_id")
        mid = cfg.get("promo_msg_id")
        if mode == 0 and src and mid:
            m = await client.get_messages(int(src), ids=int(mid))
            if m and m.media:
                sm = await client.send_file(dst_chat_id, m.media, caption=(m.message or None))
                rec.append((int(src) if src else None, int(mid) if mid else None, dst_chat_id, sm.id if sm else 0, "promo"))
            elif m and m.message:
                sm = await client.send_message(dst_chat_id, m.message)
                rec.append((int(src) if src else None, int(mid) if mid else None, dst_chat_id, sm.id if sm else 0, "promo"))
            else:
                logger.warning("⚠️ 推广指定消息无内容 src=%s id=%s", src, mid)
        else:
            text = (cfg.get("promo_text") or "").strip()
            if text:
                sm = await client.send_message(dst_chat_id, text)
                rec.append((None, None, dst_chat_id, sm.id if sm else 0, "promo"))
        logger.info("📣 已插入推广消息（配置 #%s）", cfg.get("id"))
        if task_id and rec:
            pdb.record_sends(task_id, rec)
    except FloodWaitError as e:
        await asyncio.sleep(min(int(getattr(e, "seconds", 30) or 30), 300))
    except Exception as e:
        logger.warning("⚠️ 推广插入失败: %s", e)


async def _run_items(manager, client, task, cfg, items, album_mode=True):
    """逐条/聚合执行重发（含限流/推广插入/进度/停止检查）。
    album_mode=True：同一媒体组聚合为相册发送（勾选集合内聚合）；False：逐条单发（原行为）。"""
    from features.promote import promote_db as pdb

    src = task["src_chat_id"]
    dst = task["dst_chat_id"]
    total = len(items)
    sent = processed = failed = skipped = 0
    sent_files = 0   # 转发成功的文件数（聚合相册按组内文件数累计；sent 是转发次数，聚合算 1 次）
    stop = False

    pdb.update_task_progress(task["id"], total=total, result="已加载 %s 条" % total)

    rec = []          # 发送记录缓存 [src_chat, src_msg, dst_chat, dst_msg, kind]
    def _flush_rec():
        nonlocal rec
        if rec:
            try:
                pdb.record_sends(task["id"], rec)
                rec = []
            except Exception as e:
                logger.warning("⚠️ 写入发送记录失败: %s", e)

    # 组装发送单元
    units = []
    if album_mode:
        mid_list = [it["msg_id"] for it in items]
        msgs = await _fetch_msgs_batch(client, src, mid_list, task_id=task["id"])
        gmap = {}          # grouped_id -> [msg,...]
        single_msgs = []   # 非组消息
        for mid in mid_list:
            m = msgs.get(mid)
            if not m or not m.media:
                skipped += 1
                processed += 1
                continue
            gid = getattr(m, "grouped_id", None)
            if gid:
                gmap.setdefault(gid, []).append(m)
            else:
                single_msgs.append(m)
        for gl in gmap.values():
            gl.sort(key=lambda x: x.id)
        pending = []
        for gid, gl in gmap.items():
            pending.append((gl[0].id, gl))
        for m in single_msgs:
            pending.append((m.id, m))
        pending.sort(key=lambda x: x[0])
        units = [u for _, u in pending]
        if skipped:
            pdb.update_task_progress(task["id"], result="已加载 %s 条（跳过失效 %s）" % (total, skipped))
    else:
        units = [it["msg_id"] for it in items]

    for u in units:
        # 停止检查（每 CHECK_EVERY 条）
        if processed % CHECK_EVERY == 0:
            cur = pdb.get_task(task["id"])
            if cur and cur["status"] == 4:
                stop = True
                break
        # 单任务上限保护
        if sent >= MAX_TASK_SEND:
            pdb.update_task_progress(task["id"], result="达到单任务上限 %s 条，自动停止" % MAX_TASK_SEND)
            stop = True
            break

        if isinstance(u, list):
            # 聚合模式：媒体组相册发送
            try:
                first = u[0]
                caption = None
                if cfg:
                    caption = _build_caption(
                        first,
                        cfg.get("caption_mode"), cfg.get("custom_text"),
                        bool(cfg.get("tag")),
                    )
                ok, sent_msgs = await _send_album(client, u, dst, caption)
                if ok:
                    sent += 1
                    sent_files += len(u)
                processed += 1
                if not ok:
                    failed += 1
                elif sent_msgs:
                    # 相册组：源消息与目标消息按序对应（撤回按目标消息ID）
                    for m, sm in zip(u, sent_msgs):
                        rec.append((src, m.id, dst, sm.id, "file"))
                promo_every = int((cfg or {}).get("promo_every") or 0) if cfg else 0
                if cfg and promo_every > 0 and ok and sent % promo_every == 0:
                    await _send_promo(client, dst, cfg, task["id"])
            except FloodWaitError as e:
                await asyncio.sleep(min(int(getattr(e, "seconds", 30) or 30), 300))
                failed += 1
                processed += 1
            except Exception as e:
                if _is_network_error(e):
                    on_network_error()
                    await _wait_for_network(task["id"])
                    continue
                logger.error("❌ 相册转发失败（%s 条）: %s", len(u), e)
                failed += 1
                processed += 1
        else:
            # 逐条模式：现状（get_messages → send_file）
            # album_mode=True 时 u 可能是 Message 对象（非组的单条）；全量模式 u 是 msg_id int
            msg_id = u if isinstance(u, int) else getattr(u, "id", None)
            try:
                m = await client.get_messages(src, ids=msg_id)
            except Exception as e:
                if _is_network_error(e):
                    on_network_error()
                    await _wait_for_network(task["id"])
                    continue  # 网络恢复后重试本条
                failed += 1
                processed += 1
                continue

            if not m or not m.media:
                skipped += 1
                processed += 1
                if processed % PROGRESS_EVERY == 0:
                    pdb.update_task_progress(
                        task["id"], processed=processed, success=sent, success_files=sent_files, failed=failed, skipped=skipped,
                        result="转发文件 %s/%s（跳过 %s，失败 %s），转发%s次" % (sent_files, total, skipped, failed, sent),
                    )
                continue

            caption = _build_caption(m, (cfg or {}).get("caption_mode"), (cfg or {}).get("custom_text"), bool((cfg or {}).get("tag"))) if cfg else None
            try:
                sent_msg = await client.send_file(dst, m.media, caption=caption or None)
                sent += 1
                sent_files += 1
                processed += 1
                if sent_msg:
                    rec.append((src, msg_id, dst, sent_msg.id, "file"))
                # 推广插入：每 promo_every 条成功插入 1 条
                promo_every = int((cfg or {}).get("promo_every") or 0) if cfg else 0
                if cfg and promo_every > 0 and sent % promo_every == 0:
                    await _send_promo(client, dst, cfg, task["id"])
            except FloodWaitError as e:
                await asyncio.sleep(min(int(getattr(e, "seconds", 30) or 30), 300))
                failed += 1
                processed += 1
            except Exception as e:
                if _is_network_error(e):
                    on_network_error()
                    await _wait_for_network(task["id"])
                    continue
                logger.error("❌ 重发失败 msg=%s: %s", msg_id, e)
                failed += 1
                processed += 1

        # 进度回写（每 PROGRESS_EVERY 条）
        if processed % PROGRESS_EVERY == 0:
            _flush_rec()
            pdb.update_task_progress(
                task["id"], processed=processed, success=sent, success_files=sent_files, failed=failed, skipped=skipped,
                result="转发文件 %s/%s（跳过 %s，失败 %s），转发%s次" % (sent_files, total, skipped, failed, sent),
            )
        # 限流间隔
        await asyncio.sleep(SEND_INTERVAL)

    _flush_rec()
    # 收尾
    if stop:
        pdb.update_task_progress(
            task["id"], status=4, processed=processed, success=sent, success_files=sent_files, failed=failed, skipped=skipped,
            result="已停止：转发文件 %s/%s（跳过 %s，失败 %s），转发%s次" % (sent_files, total, skipped, failed, sent),
        )
    else:
        pdb.update_task_progress(
            task["id"], status=2, processed=processed, success=sent, success_files=sent_files, failed=failed, skipped=skipped,
            result="完成：转发文件 %s/%s（跳过 %s，失败 %s），转发%s次" % (sent_files, total, skipped, failed, sent),
        )
    return stop


async def run_promote_task(manager, task_id):
    """执行一条待处理任务（status=0）"""
    from features.promote import promote_db as pdb

    task = pdb.get_task(task_id)
    if not task or task["status"] != 0:
        return
    # 撤回任务分发给撤回执行器
    if (task.get("type") or "forward") == "recall":
        await run_recall_task(manager, task_id)
        return

    if task["cfg_id"]:
        cfg = pdb.get_config(task["cfg_id"])
    else:
        # 勾选直转任务（cfg_id=0）：caption/标签参数存在任务行里，构造伪 cfg
        cfg = {
            "caption_mode": task.get("caption_mode") or 0,
            "custom_text": task.get("custom_text") or "",
            "tag": task.get("tag") or 0,
            "promo_every": 0, "promo_mode": 0, "promo_text": "",
        }
    client = None
    if manager.mtproto_client:
        client = manager.mtproto_client.client
    if not client:
        pdb.update_task_progress(task_id, status=3, error="MTProto 未就绪，请先 /mtlogin")
        return

    pdb.update_task_progress(task_id, status=1, result="准备中")

    # 手动勾选清单（msg_ids 非空）优先；否则整群按 filters 从缓存取
    if (task.get("msg_ids") or "").strip():
        items = [{"msg_id": int(x.strip())} for x in str(task["msg_ids"]).split(",") if x.strip().isdigit()]
        if not items:
            pdb.update_task_progress(task_id, status=3, error="勾选的 msg_ids 为空")
            return
    else:
        items = _load_source_list(task["src_chat_id"], task.get("filters") or "all")
        if not items:
            pdb.update_task_progress(
                task_id, status=2, total=0,
                result="清单为空（来源群未扫描或筛选无匹配，请先在群文件查询里扫描来源群）",
            )
            return

    album_mode = 1 if task.get("album") else 0
    try:
        await _run_items(manager, client, task, cfg, items, album_mode=bool(album_mode))
    except Exception as e:
        logger.error("❌ 推广任务 #%s 执行异常: %s", task_id, e)
        pdb.update_task_progress(task_id, status=3, error=str(e))


async def send_single(manager, src_chat_id, msg_id, dst_chat_id, caption=None):
    """web 勾选单条立即转发（不建任务）。返回 (ok, status/error)"""
    client = None
    if manager.mtproto_client:
        client = manager.mtproto_client.client
    if not client:
        return False, "MTProto 未就绪，请先 /mtlogin"
    try:
        res = await _send_once(client, src_chat_id, msg_id, dst_chat_id, caption)
        return res == "success", res
    except Exception as e:
        return False, str(e)



async def run_recall_task(manager, task_id):
    """撤回任务：按 src_task_id 查发送记录 → delete_messages 撤回目标群已发内容"""
    from features.promote import promote_db as pdb

    task = pdb.get_task(task_id)
    if not task or task["status"] != 0:
        return
    src_task_id = task.get("src_task_id")
    if not src_task_id:
        pdb.update_task_progress(task_id, status=3, error="缺少来源任务 ID，无法撤回")
        return
    client = None
    if manager.mtproto_client:
        client = manager.mtproto_client.client
    if not client:
        pdb.update_task_progress(task_id, status=3, error="MTProto 未就绪，请先 /mtlogin")
        return
    sends = pdb.get_sends_by_task(src_task_id)
    if not sends:
        pdb.update_task_progress(task_id, status=2, success=0, total=0, result="该任务没有可撤回的发送记录")
        return

    pdb.update_task_progress(task_id, status=1, total=len(sends), result="准备撤回 %s 条" % len(sends))
    by_dst = {}
    for s in sends:
        by_dst.setdefault(s["dst_chat_id"], []).append(s["dst_msg_id"])
    ok = fail = 0
    for dst, ids in by_dst.items():
        try:
            await client.delete_messages(dst, ids)
            ok += len(ids)
        except FloodWaitError as e:
            logger.warning("⏳ FloodWait %s 秒，等待后继续", e.seconds)
            await asyncio.sleep(min(int(getattr(e, "seconds", 30) or 30), 300))
            fail += len(ids)
        except Exception as e:
            logger.error("❌ 撤回失败 dst=%s (%s 条): %s", dst, len(ids), e)
            fail += len(ids)
        await asyncio.sleep(1.0)
    if fail:
        pdb.update_task_progress(task_id, status=2, success=ok, failed=fail, result="撤回完成：成功 %s 失败 %s" % (ok, fail))
    else:
        pdb.update_task_progress(task_id, status=2, success=ok, result="撤回完成：成功 %s 条" % ok)
