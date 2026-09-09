# openbot\core\media_scanner.py
"""
【core 扫描底层】全项目唯一的"消息遍历 + 媒体提取"能力（插件只依赖本模块，不互相依赖）。

职责：
  - iter_media_messages: 极薄的 iter_messages 封装（限速可选、服务器端媒体过滤可选），
    返回原始消息对象流——上层（下载引擎/群文件扫描）保持自己的业务判断不变。
  - extract_media_info: 从消息提取结构化媒体信息
    （media_id / file_name / file_size / kind），供清单类扫描直接使用。
  - list_media_messages: 组合两者，遍历并 yield 结构化媒体信息（scan 专用便捷入口）。

为什么用服务器过滤（media_filter）：
  Telegram 服务器端索引了每个会话的媒体消息，支持"只返回文件/图片消息"的过滤
  （InputMessagesFilterDocument / InputMessagesFilterPhotos）。
  全量 iter_messages 会把群里每条纯文本也拉下来；带过滤只拉媒体消息，
  扫同一群可快几十倍，FloodWait 风险大幅下降。

边界：
  - 本模块不做业务：不判断"是否下载"、不写 group_files、不建任务。
  - "扫描即下载"（downloader 插件的任务扫描）与"扫描出清单"（look 插件的
    群文件查看）各自在上层完成。
"""
import asyncio
import logging

from telethon import types
from telethon.tl.functions.messages import SearchRequest
from telethon.tl.types import (
    InputMessagesFilterDocument, InputMessagesFilterPhotos,
    InputMessagesFilterVideo, InputMessagesFilterMusic,
    InputMessagesFilterGif, InputMessagesFilterVoice,
    InputMessagesFilterRoundVideo,
)

from core import media_cache
from core.database import _extract_media_id

logger = logging.getLogger(__name__)

# 预置过滤器选择
FILTER_ALL = None                # 不过滤（遍历全部消息）
FILTER_FILES = InputMessagesFilterDocument()  # 只拉文档（视频/文件/音频）
FILTER_PHOTOS = InputMessagesFilterPhotos()   # 只拉图片
FILTER_VIDEO = InputMessagesFilterVideo()     # 只拉视频
FILTER_AUDIO = InputMessagesFilterMusic()     # 只拉音频/音乐
FILTER_GIF = InputMessagesFilterGif()         # 只拉动图(GIF)
FILTER_VOICE = InputMessagesFilterVoice()     # 只拉语音消息
FILTER_ROUND = InputMessagesFilterRoundVideo()  # 只拉圆视频(视频笔记)

# Telegram 全部媒体分类过滤器（互斥、物理完整；按序探测后只遍历有内容的分类）
MEDIA_FILTERS_ALL = [
    FILTER_PHOTOS, FILTER_VIDEO, FILTER_GIF,
    FILTER_AUDIO, FILTER_VOICE, FILTER_ROUND, FILTER_FILES,
]


async def count_media_messages(client, entity, media_filter):
    """服务器返回匹配媒体消息的【总数】（messages.Search 的 count 字段）。

    用途：扫描进度百分比基准。只发一次 limit=1 的 Search 请求，
    响应自带 count=总匹配数（比全量遍历快得多）。
    失败时返回 None（上层降级为只显示绝对数，不显示百分比）。
    """
    try:
        r = await client(SearchRequest(
            peer=entity, q="", filter=media_filter,
            min_date=None, max_date=None,
            offset_id=0, add_offset=0, limit=1,
            max_id=0, min_id=0, hash=0,
        ))
        return getattr(r, "count", None)
    except Exception as e:
        logger.warning(f"⚠️ 获取媒体总数失败（进度百分比降级为绝对数）: {e}")
        return None


async def count_media_by_category(client, entity):
    """一次获取该群的图片/视频/音频/文件分类数量（4次 count 请求，不遍历消息）。

    对应手机端"图片 1,234 / 视频 567"的聚合值，扫描前即可显示。
    返回 {"photo": N, "video": N, "audio": N, "file": N}，失败的项为 0。
    """
    result = {"photo": 0, "video": 0, "audio": 0, "file": 0}
    # 图片
    c = await count_media_messages(client, entity, FILTER_PHOTOS)
    if c is not None: result["photo"] = c
    await asyncio.sleep(0.5)
    # 视频
    c = await count_media_messages(client, entity, FILTER_VIDEO)
    if c is not None: result["video"] = c
    await asyncio.sleep(0.5)
    # 音频
    c = await count_media_messages(client, entity, FILTER_AUDIO)
    if c is not None: result["audio"] = c
    await asyncio.sleep(0.5)
    # 文档总数（含视频+音频+文件），文件数 = 文档 - 视频 - 音频
    c = await count_media_messages(client, entity, FILTER_FILES)
    if c is not None:
        result["file"] = max(0, c - result["video"] - result["audio"])
    return result


async def iter_media_messages(client, entity, *, wait_time=0.3, limit=None, offset_id=None, min_id=None, search=None, media_filter=FILTER_ALL):
    """遍历对话消息（可选限速/断点/筛选/服务器媒体过滤），yield 原始 Message 对象。

    参数与 client.iter_messages 对齐（limit/offset_id/min_id/search），
    media_filter 传 FILTER_FILES / FILTER_PHOTOS 时服务器只返回对应媒体消息。
    额外提供 wait_time：每 20 条停 wait_time 秒，避免长时间扫描触发 FloodWait。
    断点续扫：传 offset_id=上次扫到的最大消息ID。
    """
    kwargs = {}
    if limit is not None:
        kwargs["limit"] = limit
    if offset_id is not None:
        kwargs["offset_id"] = offset_id
    if min_id is not None:
        kwargs["min_id"] = min_id
    if search is not None:
        kwargs["search"] = search
    if media_filter is not None:
        kwargs["filter"] = media_filter

    count = 0
    async for m in client.iter_messages(entity, **kwargs):
        yield m
        count += 1
        if wait_time and count % 20 == 0:
            await asyncio.sleep(wait_time)


def _extract_hashtags(text):
    """从消息文本提取 #标签（支持中文/英文/数字/下划线），空格拼接返回"""
    if not text:
        return ""
    try:
        import re
        tags = re.findall(r"#([^\s#]+)", text)
        return " ".join(t for t in tags if t)
    except Exception:
        return ""


def extract_media_info(msg):
    """从消息提取结构化媒体信息；非媒体消息返回 None。

    返回 dict:
      media_id   Telegram 全局唯一 ID（document.id / photo.id，跨群不变）
      file_name  原始文件名（文档类取 DocumentAttributeFilename，否则用消息ID）
      file_size  字节数
      kind       'video' / 'photo' / 'audio' / 'document'
    """
    if not msg or not msg.media:
        return None
    media = msg.media
    hashtag = _extract_hashtags(getattr(msg, "message", None) or "")

    # 文档类（视频/文件/音频）
    if isinstance(media, types.MessageMediaDocument) and media.document:
        doc = media.document
        fname = f"{msg.id}"
        for attr in getattr(doc, "attributes", []):
            if isinstance(attr, types.DocumentAttributeFilename):
                fname = attr.file_name
                break
        mime = doc.mime_type or ""
        if mime.startswith("video/"):
            kind = "video"
        elif mime.startswith("audio/"):
            kind = "audio"
        elif mime.startswith("image/"):
            kind = "photo"
        else:
            kind = "document"
        return {
            "media_id": _extract_media_id(media),
            "file_name": fname,
            "file_size": doc.size or 0,
            "kind": kind,
            "hashtag": hashtag,
        }

    # 图片类
    if isinstance(media, types.MessageMediaPhoto) and media.photo:
        return {
            "media_id": _extract_media_id(media),
            "file_name": f"photo_{msg.id}.jpg",
            "file_size": getattr(media.photo, "size", 0) or 0,
            "kind": "photo",
            "hashtag": hashtag,
        }

    return None


async def list_media_messages(client, entity, *, wait_time=0.3, limit=None, offset_id=None, min_id=None, search=None, media_filter=FILTER_ALL):
    """遍历对话并 yield 结构化媒体信息（extract_media_info 的便捷组合，扫描清单用）。

    用法示例（look 插件群文件扫描，只拉文件类消息）：
        from core.media_scanner import list_media_messages, FILTER_FILES
        async for info in list_media_messages(client, entity, media_filter=FILTER_FILES):
            # info: {"media_id":..., "file_name":..., "file_size":..., "kind":...}
            ...
    """
    async for m in iter_media_messages(
        client, entity, wait_time=wait_time, limit=limit,
        offset_id=offset_id, min_id=min_id, search=search,
        media_filter=media_filter,
    ):
        info = extract_media_info(m)
        if info is not None:
            info["msg_id"] = m.id
            info["date"] = m.date
            yield info


# ===================== 扫描并写入共享缓存（look / downloader 共用） =====================
async def scan_chat_to_cache(client, chat_id, chat_name, *, mode="incremental",
                              source="扫描", scan_style="unfiltered",
                              progress_callback=None):
    """扫描群/频道的全部媒体文件，写入 core/media_cache 共享缓存，返回文件总数。

    参数:
      client            Telethon client（已连接）
      chat_id           对话 ID（整数）
      chat_name         对话名（用于日志和缓存记录）
      mode              "incremental"=从上次最大msg_id继续（默认）；"full"=清空旧缓存全量重扫
      source            写入缓存的 source 字段（如"群文件查询"/"/dl命令"）
      scan_style        "unfiltered"=不过滤全遍历（默认，最稳：单遍遍历全部消息，
                        图/视/音/文一个循环全识别，不存在过滤器失效漏扫）；
                        "filtered"=服务器分类过滤（快路径：文档一遍+图片一遍，
                        收藏夹文档过滤器失效时自动回退 视频+音频+图片 分类扫描）
      progress_callback 可选回调 fn(scanned_count, total_count, identified_count)
                        用于 look 更新 web 进度 / downloader 更新任务状态

    完整性策略：默认 unfiltered 全遍历（物理上不会漏）；filtered 留给对速度敏感的调用方。
    """
    media_cache.init_db()

    # 1. 验证可访问
    try:
        entity = await client.get_entity(chat_id)
    except Exception as e:
        raise RuntimeError(f"无法访问该对话（可能不在群内或被移出）: {e}")

    # 2. 确定扫描起点
    min_id = None
    existing_count = 0
    if mode == "incremental":
        max_mid = media_cache.get_max_msg_id(chat_id)
        if max_mid > 0:
            min_id = max_mid
            existing_count = media_cache.count_files(chat_id)
            logger.info(f"📈 增量扫描: {chat_name}({chat_id}) 从 msg_id>{min_id} 开始（已有 {existing_count} 条）")
        else:
            logger.info(f"📈 增量扫描: {chat_name}({chat_id}) 无历史数据，按全量处理")
            mode = "full"

    if mode == "full":
        media_cache.clear_chat(chat_id)
        logger.info(f"🔄 全量扫描: {chat_name}({chat_id}) 已清空旧缓存")

    # 3. 按 scan_style 分派
    if scan_style == "filtered":
        return await _scan_chat_filtered(
            client, entity, chat_id, chat_name,
            mode=mode, source=source, min_id=min_id, existing_count=existing_count,
            progress_callback=progress_callback,
        )
    return await _scan_chat_unfiltered(
        client, entity, chat_id, chat_name,
        mode=mode, source=source, min_id=min_id, existing_count=existing_count,
        progress_callback=progress_callback,
    )


async def _scan_chat_unfiltered(client, entity, chat_id, chat_name, *, mode, source, min_id, existing_count, progress_callback):
    """不过滤全遍历：单遍遍历对话全部消息，extract_media_info 识别图/视/音/文入库（最稳，默认）。"""
    # 进度基准：四类 count 聚合加总（仅用于百分比显示，不影响扫描本身）
    counts = await count_media_by_category(client, entity)
    total = counts["photo"] + counts["video"] + counts["audio"] + counts["file"]
    if total > 0:
        logger.info(f"📊 {chat_name}({chat_id}) 服务器识别 {total} 条媒体（图{counts['photo']} 视{counts['video']} 音{counts['audio']} 文{counts['file']}）")

    BATCH = 500
    LOG_EVERY = 50
    count = 0          # 本次识别到的文件数
    scanned = 0        # 本次拉取的消息数（含纯文本）
    batch_records = []

    async for msg in iter_media_messages(client, entity, wait_time=0.3, min_id=min_id):
        scanned += 1
        if scanned % LOG_EVERY == 0:
            pct = min(round(count * 100 / total), 100) if total else 0
            logger.info(f"⏳ 扫描中: {chat_name} 已拉取 {scanned}，识别 {count} 个文件 ({pct}%)")
            if progress_callback:
                try:
                    progress_callback(scanned, total, count)
                except Exception:
                    pass
        info = extract_media_info(msg)
        if info is None:
            continue
        batch_records.append({
            "chat_id": chat_id,
            "chat_name": chat_name or "",
            "msg_id": msg.id,
            "file_name": info["file_name"],
            "document_id": info["media_id"] or "",
            "file_size": info["file_size"],
            "source": source,
            "media_type": info["kind"],
            "hashtag": info.get("hashtag", "") or "",
            "grouped_id": getattr(msg, "grouped_id", None) or 0,
        })
        count += 1
        # 分批写入
        if len(batch_records) >= BATCH:
            media_cache.add_files(batch_records)
            batch_records = []

    # 写入剩余
    if batch_records:
        media_cache.add_files(batch_records)

    total_count = existing_count + count if mode == "incremental" else count
    logger.info(f"✅ 扫描完成: {chat_name}({chat_id}) mode={mode} 新增 {count}，共 {total_count} 个文件")
    if progress_callback:
        try:
            progress_callback(scanned, total, count)
        except Exception:
            pass
    return total_count


async def _scan_chat_filtered(client, entity, chat_id, chat_name, *, mode, source, min_id, existing_count, progress_callback):
    """服务器分类过滤分遍扫描（scan_style="filtered"，物理完整）：
    按 Telegram 全部媒体分类过滤器（图片/视频/GIF/音频/语音/圆视频/文档）逐类探测，
    只遍历有内容的分类；add_files 以 (chat_id,msg_id) 主键去重，分类间无重复、无遗漏。
    ⚠️ 为什么不用无过滤全遍历：Telegram 对超大对话的全部消息搜索历史索引不全，
    实测稳定漏消息（收藏夹 2.4 万媒体漏 1345 条）；而媒体分类过滤器走完整索引，一条不漏。
    """
    # 3. 探测各分类服务器总数（进度基准 + 决定遍历哪些分类）
    total = 0
    active_filters = []
    for f in MEDIA_FILTERS_ALL:
        try:
            c = await count_media_messages(client, entity, f) or 0
        except Exception:
            c = 0
        # ⚠️ 不按 count 过滤：实测 FilterGif 的 count 探测返回 0 但遍历能拉到内容，
        #    因此全部过滤器都遍历（空分类仅多 1 次请求即返回），确保物理完整。
        active_filters.append((f, c))
        total += c
    if total > 0:
        detail = " + ".join(f"{c}" for _, c in active_filters)
        logger.info(f"📊 {chat_name}({chat_id}) 服务器识别 {total} 条媒体（分遍: {detail}）")
    else:
        logger.warning(f"⚠️ {chat_name}({chat_id}) 服务器未识别到任何媒体，跳过")
        return existing_count if mode == "incremental" else 0

    # 4. 分遍扫描（只遍历有内容的分类，主键去重）
    BATCH = 500
    LOG_EVERY = 50
    count = 0          # 本次识别到的文件数
    scanned = 0        # 本次拉取的消息数
    no_media = 0       # 源频道不可用（无媒体空壳，如被移出群后转发的占位）条数
    batch_records = []
    invalid_msg_ids = []   # 空壳消息 msg_id 列表（有消息ID、无媒体，入库供“状态=失效”显示）

    for media_filter, _ in active_filters:
        async for msg in iter_media_messages(client, entity, wait_time=0.3, media_filter=media_filter, min_id=min_id):
            scanned += 1
            if scanned % LOG_EVERY == 0:
                pct = round(count * 100 / total) if total else 0
                logger.info(f"⏳ 扫描中: {chat_name} 已拉取 {scanned}，识别 {count} 个文件 ({pct}%)")
                if progress_callback:
                    try:
                        progress_callback(scanned, total, count)
                    except Exception:
                        pass
            info = extract_media_info(msg)
            if info is None:
                # 媒体过滤器返回但无 media 内容 = 源频道不可用的空壳占位消息：
                # 无文件 id/文件名/大小，仅消息 ID 可识别。计数 no_media 供对账显示
                # （总数 = 入库 + 不可用），同时记录 msg_id 入库标记 invalid（状态列显示“失效”）
                no_media += 1
                invalid_msg_ids.append(msg.id)
                if len(invalid_msg_ids) >= BATCH:
                    try:
                        media_cache.add_invalid_files(chat_id, chat_name, invalid_msg_ids)
                    except Exception as e:
                        logger.warning(f"⚠️ 写入失效消息失败: {e}")
                    invalid_msg_ids = []
                continue
            batch_records.append({
                "chat_id": chat_id,
                "chat_name": chat_name or "",
                "msg_id": msg.id,
                "file_name": info["file_name"],
                "document_id": info["media_id"] or "",
                "file_size": info["file_size"],
                "source": source,
                "media_type": info["kind"],
                "hashtag": info.get("hashtag", "") or "",
                "grouped_id": getattr(msg, "grouped_id", None) or 0,
            })
            count += 1
            # 分批写入
            if len(batch_records) >= BATCH:
                media_cache.add_files(batch_records)
                batch_records = []

    # 写入剩余
    if batch_records:
        media_cache.add_files(batch_records)
    if invalid_msg_ids:
        try:
            media_cache.add_invalid_files(chat_id, chat_name, invalid_msg_ids)
        except Exception as e:
            logger.warning(f"⚠️ 写入失效消息失败: {e}")

    total_count = existing_count + count if mode == "incremental" else count
    # 全量扫描时把源频道不可用数写入 group_stats（no_media 为全程累计值；增量只扫新增，不覆盖）
    if mode == "full":
        try:
            media_cache.update_group_unavailable(chat_id, no_media)
        except Exception as e:
            logger.warning(f"⚠️ 更新不可用统计失败: {e}")
        if no_media:
            logger.info(f"ℹ️ {chat_name}({chat_id}) 源频道不可用 {no_media} 条（无媒体内容，已标记失效）")
    logger.info(f"✅ 扫描完成: {chat_name}({chat_id}) mode={mode} 新增 {count}，共 {total_count} 个文件")
    if progress_callback:
        try:
            progress_callback(scanned, total, count)
        except Exception:
            pass
    return total_count
