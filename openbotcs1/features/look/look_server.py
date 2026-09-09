# -*- coding: utf-8 -*-
"""
【插件】/look Web 服务 - OpenBot 群文件查看器（端口 7777）
- 启动: python features/look/look_server.py
- 访问: http://127.0.0.1:7777
- Docker 挂载: -p 7777:7777
功能:
  1. 文件清单表格（搜索/状态/重复/来源筛选 + 表头排序）
  2. 来源下拉列出你拥有的全部群（可输入搜索选定群）
  3. 点击"来源"单元格或选定群 → 显示群详细信息
数据来源: download/download_tasks.db（只读，不修改任何数据）
"""
import json
import os
import sys
import sqlite3
import threading
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

# Windows 下 stdout 默认 GBK，print 中文/emoji 会 UnicodeEncodeError 崩溃；
# 强制 UTF-8 + 容错替换，保证独立进程/子进程拉起时都能正常输出
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
if hasattr(sys.stderr, "reconfigure"):
    try:
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# 独立进程启动（python features/look/look_server.py）时，项目根不在 sys.path，
# 需手动注入，否则 from features.xxx import ... 会 ModuleNotFoundError
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# look 插件独立数据库（群文件查询/对话列表/web请求）
from features.look.db import _db_conn, init_db as look_init_db
# 下载任务数据库（只读，用于视图1显示下载任务列表）
from core.database import _db_conn as _download_db_conn, init_db as core_init_db, DB_PATH as DOWNLOAD_DB_PATH

PORT = 7777
HOST = "0.0.0.0"

# 任务状态
TASK_PENDING = 0
TASK_DOWNLOADING = 1
TASK_DONE = 2
TASK_FAILED = 3
TASK_SKIPPED = 4    # 已删除/无媒体（源文件已失效）


def query_db(sql, params=()):
    """查 look 数据库（dialogs/web_requests/group_stats）"""
    conn = _db_conn()
    conn.row_factory = sqlite3.Row
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return rows


def query_cache_db(sql, params=()):
    """查 core 共享扫描缓存（media_cache.db 的 group_files）"""
    from core.media_cache import DB_PATH
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return rows


def _cache_db_path():
    """media_cache.db 路径"""
    from core.media_cache import DB_PATH
    return DB_PATH


def _has_media_type_col():
    """检测 group_files 是否已有 media_type 列（v1.2 迁移后为 True）。
    True → 分类统一用扫描器写入的 media_type（与扫描日志同口径）；
    False → 降级用扩展名现算（兼容未迁移的旧库）。"""
    try:
        conn = sqlite3.connect(_cache_db_path(), timeout=30)
        cols = [r[1] for r in conn.execute("PRAGMA table_info(group_files)").fetchall()]
        conn.close()
        return "media_type" in cols
    except Exception:
        return False


def query_download_db(sql, params=()):
    """查下载数据库（jobs/tasks，只读，用于视图1显示下载任务）"""
    conn = _download_db_conn()
    conn.row_factory = sqlite3.Row
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return rows


def load_data():
    """读取数据库，返回 {files: [...], groups: [...], dialogs: [...]}"""
    # 下载任务表在 download_tasks.db（只读显示）
    rows = query_download_db("""
        SELECT t.file_name, t.document_id, t.file_size, t.status, t.jid,
               t.chat_id, t.chat_name, t.msg_id, t.updated_at,
               COALESCE(t.source_id, '') AS source_id,
               COALESCE(j.source, '') AS source
        FROM tasks t LEFT JOIN jobs j ON t.jid = j.jid
        ORDER BY t.tid DESC
    """)

    files = []
    # 群聚合 + document_id 计数（判重）
    id_counts = defaultdict(int)
    for r in rows:
        if r["document_id"]:
            id_counts[r["document_id"]] += 1
    for r in rows:
        doc_id = r["document_id"] or ""
        files.append([
            r["file_name"] or "",
            doc_id,
            r["file_size"] or 0,
            r["status"],
            r["jid"],
            r["chat_id"],
            r["chat_name"] or "",
            r["msg_id"],
            r["source"],
            r["source_id"] or "",      # 真实来源ID（转发来源群/用户ID）
            id_counts.get(doc_id, 0),   # 出现次数（判重）
            r["updated_at"] or "",
        ])

    # 群聚合：按 (chat_id, chat_name) 组合键聚合
    # 💡 chat_id 只作展示；转发任务的 chat_id 是 bot 私聊 ID（存储逻辑不变），
    #    不同来源群的区分靠 chat_name（群名），来源类型由 jobs.source 记录
    groups_map = defaultdict(lambda: {
        "id": 0, "name": "", "jobs": set(), "files": 0, "size": 0,
        "done": 0, "failed": 0, "noid": 0, "dup": 0,
        "last": "", "sources": set(), "source_ids": set(),
    })
    for r in rows:
        key = (r["chat_id"], r["chat_name"])  # 组合键：同名同 id 合并
        g = groups_map[key]
        g["id"] = r["chat_id"]
        g["name"] = r["chat_name"] or f"群 {r['chat_id']}"
        g["jobs"].add(r["jid"])
        g["files"] += 1
        g["size"] += r["file_size"] or 0
        if r["status"] == TASK_DONE:
            g["done"] += 1
        elif r["status"] == TASK_FAILED:
            g["failed"] += 1
        if not r["document_id"]:
            g["noid"] += 1
        elif id_counts[r["document_id"]] > 1:
            g["dup"] += 1
        if r["source"]:
            g["sources"].add(r["source"])
        if r["source_id"]:
            g["source_ids"].add(r["source_id"])
        if r["updated_at"] and r["updated_at"] > g["last"]:
            g["last"] = r["updated_at"]

    groups = [{
        "id": g["id"],
        "name": g["name"],
        "jobs": len(g["jobs"]),
        "files": g["files"],
        "size": g["size"],
        "done": g["done"],
        "failed": g["failed"],
        "noid": g["noid"],
        "dup": g["dup"],
        "last": g["last"],
        "sources": sorted(g["sources"]),
        "source_ids": sorted(g["source_ids"]),
    } for g in groups_map.values()]
    groups.sort(key=lambda x: (x["name"] or "").lower())

    # 💡 全部对话列表（来自 dialogs 表，bot 定时同步 Telegram 对话；web 只读）
    dialogs = []
    try:
        d_rows = query_db(
            "SELECT chat_id, chat_name, chat_type, username, updated_at FROM dialogs ORDER BY chat_name"
        )
        g_by_id = {g["id"]: g for g in groups}
        for r in d_rows:
            g = g_by_id.get(r["chat_id"])
            dialogs.append({
                "id": r["chat_id"],
                "name": r["chat_name"] or f"群 {r['chat_id']}",
                "type": r["chat_type"] or "",
                "username": r["username"] or "",
                "updated_at": r["updated_at"] or "",
                "has_tasks": g is not None,
                "tasks": None if g is None else {
                    "files": g["files"], "size": g["size"],
                    "done": g["done"], "failed": g["failed"],
                    "dup": g["dup"], "noid": g["noid"],
                    "last": g["last"], "sources": g["sources"],
                    "source_ids": g["source_ids"],
                },
            })
    except Exception:
        pass  # dialogs 表不存在时静默（老库），只显示有任务的群

    return {"files": files, "groups": groups, "dialogs": dialogs}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # 安静日志，不刷屏

    def _send(self, code, body, ctype="text/html; charset=utf-8"):
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        self.end_headers()
        try:
            self.wfile.write(body)
        except BrokenPipeError:
            pass  # 客户端已断开连接，无需处理
        except (ConnectionResetError, ConnectionAbortedError):
            pass

    def _send_json(self, obj):
        self._send(200, json.dumps(obj, ensure_ascii=False), "application/json; charset=utf-8")

    def do_GET(self):
        url = urlparse(self.path)
        qs = parse_qs(url.query)
        if url.path == "/" or url.path == "/index.html":
            self._send(200, PAGE_HTML)
        elif url.path == "/api/data":
            try:
                self._send_json(load_data())
            except Exception as e:
                print(f"⚠️ API 异常 {url.path}: {e}", flush=True)
                self._send_json({"error": str(e)})
        elif url.path == "/api/status":
            # 系统状态（MTProto 登录状态等），页面据此显示提示横幅
            try:
                from features.look.db import get_status
                self._send_json({
                    "mtproto": get_status("mtproto_status", "unknown"),
                })
            except Exception as e:
                print(f"⚠️ API 异常 {url.path}: {e}", flush=True)
                self._send_json({"error": str(e)})
        elif url.path == "/api/groups":
            # 全部群列表：只列真实可扫描的对话（dialogs，排除 bot 自己）
            # 💡 转发任务群（chat_id=bot 私聊）不在此列——bot 不在来源群里、扫不了，
            #    它们的下载记录在视图1（下载任务）查看
            try:
                d = load_data()
                # 批量统计各群已扫描文件的分类数（一次 SQL CASE WHEN，避免 425 个群逐个查）
                # 分类：图片 / 视频 / 音频 / 文件（其它）
                type_stats = {}
                use_mt = _has_media_type_col()
                _tstats_sql = ("""
                        SELECT chat_id,
                          SUM(CASE WHEN media_type='photo' THEN 1 ELSE 0 END) AS photo,
                          SUM(CASE WHEN media_type='video' THEN 1 ELSE 0 END) AS video,
                          SUM(CASE WHEN media_type='audio' THEN 1 ELSE 0 END) AS audio,
                          COUNT(*) AS total,
                          SUM(CASE WHEN invalid=1 THEN 1 ELSE 0 END) AS invalid
                        FROM group_files WHERE invalid=0 GROUP BY chat_id
                    """ if use_mt else """
                        SELECT chat_id,
                          SUM(CASE WHEN LOWER(file_name) LIKE '%.jpg' OR LOWER(file_name) LIKE '%.jpeg' OR LOWER(file_name) LIKE '%.png'
                            OR LOWER(file_name) LIKE '%.gif' OR LOWER(file_name) LIKE '%.webp' OR LOWER(file_name) LIKE '%.bmp'
                            OR LOWER(file_name) LIKE '%.svg' OR LOWER(file_name) LIKE '%.heic' OR LOWER(file_name) LIKE '%.ico'
                            OR LOWER(file_name) LIKE '%.tiff' THEN 1 ELSE 0 END) AS photo,
                          SUM(CASE WHEN LOWER(file_name) LIKE '%.mp4' OR LOWER(file_name) LIKE '%.mkv' OR LOWER(file_name) LIKE '%.avi'
                            OR LOWER(file_name) LIKE '%.mov' OR LOWER(file_name) LIKE '%.wmv' OR LOWER(file_name) LIKE '%.flv'
                            OR LOWER(file_name) LIKE '%.webm' OR LOWER(file_name) LIKE '%.m4v' OR LOWER(file_name) LIKE '%.mpg'
                            OR LOWER(file_name) LIKE '%.mpeg' OR LOWER(file_name) LIKE '%.3gp' OR LOWER(file_name) LIKE '%.ts'
                            OR LOWER(file_name) LIKE '%.rmvb' THEN 1 ELSE 0 END) AS video,
                          SUM(CASE WHEN LOWER(file_name) LIKE '%.mp3' OR LOWER(file_name) LIKE '%.wav' OR LOWER(file_name) LIKE '%.flac'
                            OR LOWER(file_name) LIKE '%.aac' OR LOWER(file_name) LIKE '%.ogg' OR LOWER(file_name) LIKE '%.wma'
                            OR LOWER(file_name) LIKE '%.m4a' OR LOWER(file_name) LIKE '%.ape' OR LOWER(file_name) LIKE '%.opus'
                            THEN 1 ELSE 0 END) AS audio,
                          COUNT(*) AS total,
                          SUM(CASE WHEN invalid=1 THEN 1 ELSE 0 END) AS invalid
                        FROM group_files WHERE invalid=0 GROUP BY chat_id
                    """)
                try:
                    for r in query_cache_db(_tstats_sql):
                        type_stats[str(r["chat_id"])] = {
                            "photo": r["photo"] or 0, "video": r["video"] or 0,
                            "audio": r["audio"] or 0, "other": (r["total"] or 0) - (r["photo"] or 0) - (r["video"] or 0) - (r["audio"] or 0),
                            "total": r["total"] or 0,
                        }
                except Exception:
                    pass
                # 批量查询各群最近一次扫描/统计请求的状态
                req_status = {}
                try:
                    for r in query_db("""
                        SELECT chat_id, type, status, result FROM web_requests
                        WHERE type IN ('scan', 'scan_inc', 'get_count')
                        ORDER BY id DESC
                    """):
                        sid = str(r["chat_id"])
                        if sid not in req_status:
                            req_status[sid] = {"type": r["type"], "status": r["status"], "result": r["result"] or ""}
                except Exception:
                    pass
                # 批量查询群媒体分类统计（Telegram count 接口，扫描前即可显示）
                tg_stats = {}
                try:
                    for r in query_cache_db("SELECT chat_id, photo, video, audio, file FROM group_stats"):
                        tg_stats[str(r["chat_id"])] = {
                            "photo": r["photo"] or 0, "video": r["video"] or 0,
                            "audio": r["audio"] or 0, "file": r["file"] or 0,
                        }
                except Exception:
                    pass
                seen = {}
                for x in d["dialogs"]:
                    # 排除 bot 自己的对话（用户名以 _bot 结尾的用户条目，如 kjzhcs1_bot）
                    if x["type"] == "用户" and x["name"].lower().endswith("_bot"):
                        continue
                    sid = str(x["id"])
                    ts = type_stats.get(sid, {"photo": 0, "video": 0, "audio": 0, "other": 0, "total": 0})
                    rs = req_status.get(sid, {"type": "", "status": -1, "result": ""})
                    # 区分扫描请求和统计请求
                    scan_status = rs["status"] if rs["type"] in ("scan", "scan_inc") else -1
                    count_status = rs["status"] if rs["type"] == "get_count" else -1
                    tg = tg_stats.get(sid, None)
                    # Telegram 统计（扫描前就有，右侧显示）
                    if tg:
                        tg_photo, tg_video, tg_audio, tg_file = tg["photo"], tg["video"], tg["audio"], tg["file"]
                        has_stats = True
                        tg_total = tg_photo + tg_video + tg_audio + tg_file
                    else:
                        tg_photo, tg_video, tg_audio, tg_file = 0, 0, 0, 0
                        has_stats = False
                        tg_total = 0
                    # 已扫描数量（中间显示进度）
                    scanned_count = ts["total"]
                    seen[sid] = {
                        "id": x["id"], "name": x["name"], "type": x["type"],
                        "has_tasks": x["has_tasks"], "files": x["tasks"]["files"] if x["tasks"] else 0,
                        "scanned": _group_scanned(x["id"]),
                        "has_stats": has_stats,
                        "scanned_count": scanned_count,
                        "tg_total": tg_total,
                        "tg_photo": tg_photo, "tg_video": tg_video, "tg_audio": tg_audio, "tg_file": tg_file,
                        "scanned_photo": ts["photo"], "scanned_video": ts["video"],
                        "scanned_audio": ts["audio"], "scanned_other": ts["other"],
                        "req_status": scan_status, "req_result": rs["result"],
                        "count_status": count_status,
                    }
                groups = sorted(seen.values(), key=lambda x: (x["name"] or "").lower())
                self._send_json({"groups": groups})
            except Exception as e:
                print(f"⚠️ API 异常 {url.path}: {e}", flush=True)
                self._send_json({"error": str(e)})
        elif url.path == "/api/group_files":
            # 某群的文件清单（分页，core 共享缓存）+ 重复计数 + 最近请求状态
            try:
                chat_id = (qs.get("chat_id") or [""])[0]
                page = (qs.get("page") or ["1"])[0]
                page_size = (qs.get("page_size") or ["100"])[0]
                q = (qs.get("q") or [""])[0]
                kind = (qs.get("kind") or ["all"])[0]
                sort = (qs.get("sort") or [""])[0]
                order = (qs.get("order") or ["desc"])[0]
                if not chat_id:
                    self._send_json({"error": "缺少 chat_id"})
                    return
                self._send_json(_load_group_files(chat_id, page=page, page_size=page_size, q=q, kind=kind, sort=sort, order=order))
            except Exception as e:
                print(f"⚠️ API 异常 {url.path}: {e}", flush=True)
                self._send_json({"error": str(e)})
        elif url.path == "/api/scan":
            # 写入群扫描请求（bot 轮询执行）。mode=full 重新扫描 / incremental 增量扫描
            try:
                from features.look.group_browse import add_scan_request
                chat_id = (qs.get("chat_id") or [""])[0]
                chat_name = (qs.get("chat_name") or [""])[0]
                mode = (qs.get("mode") or ["full"])[0]
                if not chat_id:
                    self._send_json({"error": "缺少 chat_id"})
                    return
                req_id, status = add_scan_request(int(chat_id), chat_name, mode=mode)
                self._send_json({"id": req_id, "status": status, "mode": mode})
            except Exception as e:
                print(f"⚠️ API 异常 {url.path}: {e}", flush=True)
                self._send_json({"error": str(e)})
        elif url.path == "/api/get_count":
            # 写入群媒体分类统计请求（bot 调用 Telegram count 接口，扫描前即可显示数量）
            try:
                from features.look.group_browse import add_count_request
                chat_id = (qs.get("chat_id") or [""])[0]
                chat_name = (qs.get("chat_name") or [""])[0]
                if not chat_id:
                    self._send_json({"error": "缺少 chat_id"})
                    return
                req_id, status = add_count_request(int(chat_id), chat_name)
                self._send_json({"id": req_id, "status": status})
            except Exception as e:
                print(f"⚠️ API 异常 {url.path}: {e}", flush=True)
                self._send_json({"error": str(e)})
        elif url.path == "/api/sync_dialogs":
            # 写入对话列表同步请求（bot 轮询执行 sync_dialogs），点"刷新频道列表"触发
            try:
                from features.look.group_browse import add_sync_request
                req_id, status = add_sync_request()
                self._send_json({"id": req_id, "status": status})
            except Exception as e:
                print(f"⚠️ API 异常 {url.path}: {e}", flush=True)
                self._send_json({"error": str(e)})
        elif url.path == "/api/download":
            # 写入文件下载请求（bot 轮询执行）
            try:
                from features.look.group_browse import add_download_request
                chat_id = (qs.get("chat_id") or [""])[0]
                chat_name = (qs.get("chat_name") or [""])[0]
                msg_id = (qs.get("msg_id") or ["0"])[0]
                file_name = (qs.get("file_name") or [""])[0]
                document_id = (qs.get("document_id") or [""])[0]
                file_size = (qs.get("file_size") or ["0"])[0]
                if not chat_id:
                    self._send_json({"error": "缺少 chat_id"})
                    return
                req_id, status = add_download_request(
                    int(chat_id), chat_name, int(msg_id or 0),
                    file_name, document_id, int(file_size or 0),
                )
                self._send_json({"id": req_id, "status": status})
            except Exception as e:
                print(f"⚠️ API 异常 {url.path}: {e}", flush=True)
                self._send_json({"error": str(e)})
        elif url.path == "/api/requests":
            # 查询请求状态（scan/download）
            try:
                from features.look.group_browse import get_request
                req_id = (qs.get("id") or [""])[0]
                if not req_id:
                    self._send_json({"error": "缺少 id"})
                    return
                r = get_request(int(req_id))
                if not r:
                    self._send_json({"error": "请求不存在"})
                    return
                self._send_json({
                    "id": r["id"], "type": r["type"], "status": r["status"],
                    "result": r["result"] or "", "chat_id": r["chat_id"],
                })
            except Exception as e:
                print(f"⚠️ API 异常 {url.path}: {e}", flush=True)
                self._send_json({"error": str(e)})
        elif url.path == "/promote.html":
            # 推广转发页面（promote 插件资产：features/promote/promote.html，静态单文件，打包进镜像）
            try:
                page_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "promote", "promote.html")
                if not os.path.exists(page_path):
                    self._send_json({"error": "promote.html 不存在，请放入 features/promote/promote.html（promote 插件资产）"})
                    return
                with open(page_path, "r", encoding="utf-8") as f:
                    self._send(200, f.read())
            except Exception as e:
                print(f"⚠️ 静态页异常 {url.path}: {e}", flush=True)
                self._send_json({"error": str(e)})
        elif url.path == "/api/promote/configs":
            # 转发配置列表（含开关/自动监听/推广插入）
            try:
                pdb = _promote_db()
                if not pdb:
                    self._send_json({"error": "推广转发插件未安装"})
                    return
                self._send_json({"ok": True, "configs": pdb.list_configs()})
            except Exception as e:
                print(f"⚠️ API 异常 {url.path}: {e}", flush=True)
                self._send_json({"error": str(e)})
        elif url.path == "/api/promote/tasks":
            # 转发任务列表（进度/结果）
            try:
                pdb = _promote_db()
                if not pdb:
                    self._send_json({"error": "推广转发插件未安装"})
                    return
                limit = int((qs.get("limit") or ["30"])[0])
                tasks = pdb.list_tasks(limit)
                for t in tasks:
                    t["send_count"] = pdb.count_sends_by_task(t["id"])
                self._send_json({"ok": True, "tasks": tasks})
            except Exception as e:
                print(f"⚠️ API 异常 {url.path}: {e}", flush=True)
                self._send_json({"error": str(e)})
        elif url.path == "/api/promote/preview":
            # 来源群文件清单预览（查 media_cache.group_files，分页 + 筛选 + 搜索，防大群卡死）
            try:
                chat_id = (qs.get("chat_id") or [""])[0]
                page = max(1, int((qs.get("page") or ["1"])[0]))
                page_size = min(200, max(10, int((qs.get("page_size") or ["50"])[0])))
                kind = (qs.get("kind") or ["all"])[0]
                q = (qs.get("q") or [""])[0].strip()
                all_ids = (qs.get("all_ids") or [""])[0] in ("1", "true")
                dedup = (qs.get("dedup") or [""])[0] in ("1", "true")
                if not chat_id:
                    self._send_json({"error": "缺少 chat_id"})
                    return
                self._send_json(_promote_preview(int(chat_id), page, page_size, kind, q, all_ids, dedup))
            except Exception as e:
                print(f"⚠️ API 异常 {url.path}: {e}", flush=True)
                self._send_json({"error": str(e)})
        else:
            self._send(404, "<h3>404 Not Found</h3><p>OpenBot 群文件查看器 - 请访问 /</p>")

    def do_POST(self):
        """POST 接口：推广转发的写操作（保存配置/发起任务/停止/删除/勾选立即转发）"""
        url = urlparse(self.path)
        try:
            length = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(length).decode("utf-8")) if length else {}
        except Exception:
            body = {}
        if url.path == "/api/promote/save":
            # 保存/更新转发配置（含开关、自动监听、推广插入）
            try:
                pdb = _promote_db()
                if not pdb:
                    self._send_json({"error": "推广转发插件未安装"})
                    return
                cfg_id = pdb.save_config(body)
                self._send_json({"ok": True, "id": cfg_id})
            except Exception as e:
                print(f"⚠️ API 异常 {url.path}: {e}", flush=True)
                self._send_json({"error": str(e)})
        elif url.path == "/api/promote/delete":
            try:
                pdb = _promote_db()
                if not pdb:
                    self._send_json({"error": "推广转发插件未安装"})
                    return
                pdb.delete_config(int(body.get("id") or 0))
                self._send_json({"ok": True})
            except Exception as e:
                print(f"⚠️ API 异常 {url.path}: {e}", flush=True)
                self._send_json({"error": str(e)})
        elif url.path == "/api/promote/start":
            # 按配置发起转发任务（可选 filters 覆盖）
            try:
                pdb = _promote_db()
                if not pdb:
                    self._send_json({"error": "推广转发插件未安装"})
                    return
                cfg = pdb.get_config(int(body.get("cfg_id") or 0))
                if not cfg:
                    self._send_json({"error": "配置不存在"})
                    return
                filters = body.get("filters") or cfg.get("filters") or "all"
                album = 1 if str(body.get("album", 1)) in ("1", "true", "True", "on") else 0
                tid = pdb.create_task(cfg["id"], cfg["src_chat_id"], cfg["dst_chat_id"], filters=filters, album=album)
                self._send_json({"ok": True, "task_id": tid})
            except Exception as e:
                print(f"⚠️ API 异常 {url.path}: {e}", flush=True)
                self._send_json({"error": str(e)})
        elif url.path == "/api/promote/stop":
            # 停止任务（status=4，执行器下一检查点生效）
            try:
                pdb = _promote_db()
                if not pdb:
                    self._send_json({"error": "推广转发插件未安装"})
                    return
                pdb.stop_task(int(body.get("task_id") or 0))
                self._send_json({"ok": True})
            except Exception as e:
                print(f"⚠️ API 异常 {url.path}: {e}", flush=True)
                self._send_json({"error": str(e)})
        elif url.path == "/api/promote/recall":
            # 撤回：按任务发送记录删除目标群已发内容（建 type=recall 任务由 bot 执行）
            try:
                pdb = _promote_db()
                if not pdb:
                    self._send_json({"error": "推广转发插件未安装"})
                    return
                src_task = int(body.get("task_id") or 0)
                if not src_task:
                    self._send_json({"error": "缺少 task_id"})
                    return
                cnt = pdb.count_sends_by_task(src_task)
                if cnt <= 0:
                    self._send_json({"error": "该任务没有可撤回的发送记录"})
                    return
                t = pdb.get_task(src_task)
                tid = pdb.create_task(0, t["src_chat_id"] if t else 0, t["dst_chat_id"] if t else 0,
                                      filters="all", task_type="recall", src_task_id=src_task)
                self._send_json({"ok": True, "task_id": tid, "count": cnt})
            except Exception as e:
                print(f"⚠️ API 异常 {url.path}: {e}", flush=True)
                self._send_json({"error": str(e)})
        elif url.path == "/api/promote/retry":
            # 重试：复制原任务参数新建待处理任务
            try:
                pdb = _promote_db()
                if not pdb:
                    self._send_json({"error": "推广转发插件未安装"})
                    return
                new_id = pdb.retry_task(int(body.get("task_id") or 0))
                if not new_id:
                    self._send_json({"error": "任务不存在"})
                    return
                self._send_json({"ok": True, "task_id": new_id})
            except Exception as e:
                print(f"⚠️ API 异常 {url.path}: {e}", flush=True)
                self._send_json({"error": str(e)})
        elif url.path == "/api/promote/delete_task":
            # 删除任务记录（连同发送记录）
            try:
                pdb = _promote_db()
                if not pdb:
                    self._send_json({"error": "推广转发插件未安装"})
                    return
                pdb.delete_task(int(body.get("task_id") or 0))
                self._send_json({"ok": True})
            except Exception as e:
                print(f"⚠️ API 异常 {url.path}: {e}", flush=True)
                self._send_json({"error": str(e)})
        elif url.path == "/api/promote/send":
            # 勾选单条/多条立即转发（cfg_id=0，msg_ids 逗号串，bot 轮询执行）
            try:
                pdb = _promote_db()
                if not pdb:
                    self._send_json({"error": "推广转发插件未安装"})
                    return
                src = int(body.get("src_chat_id") or 0)
                dst = int(body.get("dst_chat_id") or 0)
                msg_ids = body.get("msg_ids") or []
                if isinstance(msg_ids, list):
                    msg_ids = ",".join(str(x) for x in msg_ids if str(x).isdigit())
                if not src or not dst or not str(msg_ids).strip():
                    self._send_json({"error": "缺少 src_chat_id / dst_chat_id / msg_ids"})
                    return
                caption_mode = int(body.get("caption_mode") or 0)
                custom_text = body.get("custom_text") or ""
                tag = 1 if str(body.get("tag") or 0) in ("1", "true", "True", "on") else 0
                album = 1 if str(body.get("album", 1)) in ("1", "true", "True", "on") else 0
                # 勾选直转也可挂配置：cfg_id>0 → 任务从配置取附带文字/标签/推广插入等参数
                cfg_id = int(body.get("cfg_id") or 0)
                tid = pdb.create_task(cfg_id, src, dst, filters="all", msg_ids=str(msg_ids),
                                      caption_mode=caption_mode, custom_text=custom_text, tag=tag, album=album)
                self._send_json({"ok": True, "task_id": tid})
            except Exception as e:
                print(f"⚠️ API 异常 {url.path}: {e}", flush=True)
                self._send_json({"error": str(e)})
        else:
            self._send_json({"error": "未知 POST 路径"})


def _promote_db():
    """promote.db 访问（插件缺失时返回 None）"""
    try:
        from features.promote import promote_db
        return promote_db
    except Exception:
        return None


def _promote_preview(chat_id, page=1, page_size=50, kind="all", q="", all_ids=False, dedup=False):
    """来源群文件清单预览（与群文件查询同库同口径：media_cache.group_files，invalid=0）"""
    use_mt = _has_media_type_col()
    conds = ["chat_id=?"]
    params = [chat_id]
    if q:
        like = "%%%s%%" % q
        conds.append("(file_name LIKE ? OR hashtag LIKE ?)")
        params.extend([like, like])
    # kind 支持逗号多选：all / photo / video / audio / document / other / tag
    kinds = [k for k in str(kind).split(',') if k in ('all', 'photo', 'video', 'audio', 'document', 'other', 'tag', 'invalid')]
    if kinds and 'all' not in kinds and 'invalid' in kinds:
        # 失效优先：只看失效文件，忽略其它类型条件
        conds.append('invalid=1')
        kinds = [k for k in kinds if k != 'invalid']
        if not kinds:
            kinds = ['__invalid_only__']
    if kinds and 'all' not in kinds:
        ors = []
        mt = []
        if use_mt:
            if 'tag' in kinds:
                ors.append("(hashtag IS NOT NULL AND hashtag != '')")
            mt = [k for k in kinds if k in ('photo', 'video', 'audio', 'document')]
            if mt:
                ors.append("media_type IN (%s)" % ",".join("?" * len(mt)))
            if 'other' in kinds:
                ors.append("media_type IN ('document','')")
        else:
            cmap = {'photo': "(%s)" % _ext_cond(_IMG_EXT), 'video': "(%s)" % _ext_cond(_VID_EXT),
                    'audio': "(%s)" % _ext_cond(_AUD_EXT)}
            for k in kinds:
                if k in cmap:
                    ors.append(cmap[k])
            if 'other' in kinds:
                ors.append("(NOT (%s) AND NOT (%s) AND NOT (%s))" % (_ext_cond(_IMG_EXT), _ext_cond(_VID_EXT), _ext_cond(_AUD_EXT)))
        if ors:
            conds.append("(" + " OR ".join(ors) + ")")
            params += mt
    where = " AND ".join(conds)
    # 全选全部：返回当前筛选条件下所有有效 msg_id（不含失效）
    if all_ids:
        ids = []
        try:
            rows = query_cache_db(
                "SELECT msg_id FROM group_files WHERE %s AND invalid=0 ORDER BY msg_id DESC" % where, params)
            ids = [r["msg_id"] for r in rows]
        except Exception:
            try:
                rows = query_cache_db(
                    "SELECT msg_id FROM group_files WHERE %s ORDER BY msg_id DESC" % where, params)
                ids = [r["msg_id"] for r in rows]
            except Exception:
                ids = []
        return {"ok": True, "all_ids": ids, "count": len(ids)}
    # 去除重复：按 document_id 去重（每个文件保留最新一条 msg_id；document_id 为空的正常保留）
    if dedup:
        ids = []
        try:
            rows = query_cache_db(
                "SELECT msg_id FROM group_files WHERE %s AND invalid=0 AND document_id='' "
                "UNION "
                "SELECT MAX(msg_id) FROM group_files WHERE %s AND invalid=0 AND document_id!='' GROUP BY document_id "
                "ORDER BY msg_id DESC" % (where, where), params + params)
            ids = [r["msg_id"] for r in rows]
        except Exception:
            ids = []
        return {"ok": True, "dedup_ids": ids, "count": len(ids)}
    offset = (page - 1) * page_size
    total = 0
    try:
        r = query_cache_db("SELECT COUNT(*) AS c FROM group_files WHERE %s" % where, params)
        total = r[0]["c"] if r else 0
    except Exception:
        pass
    rows = []
    try:
        rows = query_cache_db(
            "SELECT file_name, document_id, file_size, msg_id, media_type, hashtag, invalid, grouped_id FROM group_files "
            "WHERE %s ORDER BY msg_id DESC LIMIT ? OFFSET ?" % where,
            params + [page_size, offset],
        )
    except Exception:
        # 旧库无 media_type/hashtag 列 → 降级查询
        try:
            rows = query_cache_db(
                "SELECT file_name, document_id, file_size, msg_id, invalid FROM group_files "
                "WHERE %s ORDER BY msg_id DESC LIMIT ? OFFSET ?" % where,
                params + [page_size, offset],
            )
        except Exception:
            rows = []
    files = [{
        "name": r["file_name"] or "",
        "doc_id": r["document_id"] or "",
        "size": r["file_size"] or 0,
        "msg_id": r["msg_id"],
        "media_type": r["media_type"] if "media_type" in r.keys() else "",
        "hashtag": r["hashtag"] if "hashtag" in r.keys() else "",
        "invalid": 1 if ("invalid" in r.keys() and r["invalid"]) else 0,
        "grouped_id": r["grouped_id"] if "grouped_id" in r.keys() else 0,
    } for r in rows]
    # 类型计数（全部/图片/视频/音乐/文档/其它/#标签），供筛选胶囊显示真实数字
    stats = {"all": 0, "photo": 0, "video": 0, "audio": 0, "document": 0, "other": 0, "tag": 0}
    try:
        r = query_cache_db("SELECT COUNT(*) AS c FROM group_files WHERE chat_id=?", (chat_id,))
        stats["all"] = r[0]["c"] if r else 0
        rows = query_cache_db(
            "SELECT media_type, COUNT(*) AS c FROM group_files WHERE chat_id=? GROUP BY media_type",
            (chat_id,))
        for r in rows:
            mt = r["media_type"] or ""
            if mt in stats:
                stats[mt] = r["c"]
            else:
                stats["other"] += r["c"]
        r = query_cache_db(
            "SELECT COUNT(*) AS c FROM group_files WHERE chat_id=? AND hashtag IS NOT NULL AND hashtag != ''",
            (chat_id,))
        stats["tag"] = r[0]["c"] if r else 0
        r = query_cache_db(
            "SELECT COUNT(*) AS c FROM group_files WHERE chat_id=? AND invalid=1",
            (chat_id,))
        stats["invalid"] = r[0]["c"] if r else 0
    except Exception:
        pass
    return {"files": files, "total": total, "page": page, "page_size": page_size, "stats": stats}


def _group_scanned(chat_id):
    """该群是否已有扫描缓存"""
    try:
        r = query_cache_db("SELECT COUNT(*) AS c FROM group_files WHERE chat_id=?", (chat_id,))
        return r[0]["c"] > 0
    except Exception:
        return False


def _ext_cond(ext_list):
    """扩展名列表 → SQL LIKE 条件片段（与分类统计同一口径）"""
    return " OR ".join("LOWER(file_name) LIKE '%" + e + "'" for e in ext_list.split())


_IMG_EXT = ".jpg .jpeg .png .gif .webp .bmp .svg .heic .ico .tiff"
_VID_EXT = ".mp4 .mkv .avi .mov .wmv .flv .webm .m4v .mpg .mpeg .3gp .ts .rmvb"
_AUD_EXT = ".mp3 .flac .wav .m4a .aac .ogg .opus .wma .ape .mid"


def _load_group_files(chat_id, page=1, page_size=100, q="", kind="all", sort="", order="desc"):
    """读取某群文件清单（分页 + 搜索 + 类别 + 排序）+ 重复计数/出现序号 + 分类统计 + 请求状态。
    q=搜索关键词（文件名/文件ID/消息ID/对话ID）；kind=all/photo/video/audio/other；
    sort=size/dup（dup=重复多的排前面）；order=asc/desc。"""
    page = max(1, int(page or 1))
    page_size = min(500, max(10, int(page_size or 100)))
    offset = (page - 1) * page_size
    q = (q or "").strip()
    kind = (kind or "all").strip() or "all"
    order = "ASC" if str(order).lower() == "asc" else "DESC"

    # 1. 搜索 + 类别 → WHERE 片段（v1.2 分类统一用 media_type；旧库降级扩展名）
    conds = ["chat_id=?"]
    params = [chat_id]
    use_mt = _has_media_type_col()
    if q:
        like = "%%%s%%" % q
        conds.append("(file_name LIKE ? OR document_id LIKE ? OR CAST(msg_id AS TEXT) LIKE ? OR CAST(chat_id AS TEXT) LIKE ?)")
        params.extend([like, like, like, like])
    if kind == "photo":
        conds.append("media_type='photo'" if use_mt else "(%s)" % _ext_cond(_IMG_EXT))
    elif kind == "video":
        conds.append("media_type='video'" if use_mt else "(%s)" % _ext_cond(_VID_EXT))
    elif kind == "audio":
        conds.append("media_type='audio'" if use_mt else "(%s)" % _ext_cond(_AUD_EXT))
    elif kind == "other":
        conds.append("invalid=0 AND (media_type IN ('document',''))" if use_mt else
                     "invalid=0 AND (NOT (%s) AND NOT (%s) AND NOT (%s))" % (_ext_cond(_IMG_EXT), _ext_cond(_VID_EXT), _ext_cond(_AUD_EXT)))
    elif kind == "invalid":
        conds.append("invalid=1")
    where = " AND ".join(conds)

    # 2. 排序（dup 需要全表 GROUP BY 一次算重复数）
    dup_select, dup_join = "", ""
    if sort == "size":
        order_by = "file_size %s, msg_id DESC" % order
    elif sort == "dup":
        dup_select = ", COALESCE(d.c, 0) AS dc"
        dup_join = " LEFT JOIN (SELECT document_id AS did, COUNT(*) AS c FROM group_files WHERE document_id != '' GROUP BY document_id) d ON group_files.document_id = d.did"
        order_by = "dc DESC, msg_id DESC"
    else:
        order_by = "msg_id DESC"

    # 3. 总数（一次 COUNT，不加载数据）
    total = 0
    try:
        r = query_cache_db("SELECT COUNT(*) AS c FROM group_files WHERE %s" % where, params)
        total = r[0]["c"] if r else 0
    except Exception:
        pass

    # 4. 当前页文件（LIMIT/OFFSET，只加载 page_size 条）
    try:
        rows = query_cache_db("""
            SELECT file_name, document_id, file_size, msg_id, scanned_at, chat_name, chat_id, invalid%s
            FROM group_files%s
            WHERE %s
            ORDER BY %s
            LIMIT ? OFFSET ?
        """ % (dup_select, dup_join, where, order_by), params + [page_size, offset])
    except Exception:
        rows = []

    # 5. 只查当前页文件的重复计数（不再全表 GROUP BY，10万条也快）
    id_counts = defaultdict(int)
    occ_map = {}
    doc_ids = [r["document_id"] for r in rows if r["document_id"]]
    if doc_ids:
        try:
            placeholders = ",".join("?" * len(doc_ids))
            for r in query_cache_db(
                "SELECT document_id, COUNT(*) AS c FROM group_files WHERE document_id IN (%s) AND document_id != '' GROUP BY document_id" % placeholders,
                doc_ids,
            ):
                id_counts[r["document_id"]] = r["c"]
        except Exception:
            pass
        # 出现序号：窗口函数按 msg_id 升序编号，最早一条 = 第一次出现
        try:
            for r in query_cache_db(
                "SELECT document_id, msg_id, ROW_NUMBER() OVER (PARTITION BY document_id ORDER BY msg_id) AS ord "
                "FROM group_files WHERE document_id IN (%s) AND document_id != ''" % placeholders,
                doc_ids,
            ):
                occ_map["%s|%s" % (r["document_id"], r["msg_id"])] = r["ord"]
        except Exception:
            pass

    files = [{
        "name": r["file_name"] or "",
        "doc_id": r["document_id"] or "",
        "size": r["file_size"] or 0,
        "msg_id": r["msg_id"],
        "count": id_counts.get(r["document_id"] or "", 0),
        "ord": occ_map.get("%s|%s" % (r["document_id"] or "", r["msg_id"]), 1),
        "chat_id": r["chat_id"],
        "chat_name": r["chat_name"] or "",
        "invalid": r["invalid"] or 0,
    } for r in rows]

    # 6. 全量分类统计（一次 SQL CASE WHEN，不加载数据到 Python；v1.2 用 media_type 口径）
    stats = {"photo": 0, "video": 0, "audio": 0, "other": 0, "invalid": 0}
    try:
        if use_mt:
            _stats_sql = """
                SELECT
                  SUM(CASE WHEN media_type='photo' THEN 1 ELSE 0 END) AS photo,
                  SUM(CASE WHEN media_type='video' THEN 1 ELSE 0 END) AS video,
                  SUM(CASE WHEN media_type='audio' THEN 1 ELSE 0 END) AS audio,
                  COUNT(*) AS total,
                  SUM(CASE WHEN invalid=1 THEN 1 ELSE 0 END) AS invalid
                FROM group_files WHERE chat_id=? AND invalid=0
            """
        else:
            _stats_sql = """
                SELECT
                  SUM(CASE WHEN %s THEN 1 ELSE 0 END) AS photo,
                  SUM(CASE WHEN %s THEN 1 ELSE 0 END) AS video,
                  SUM(CASE WHEN %s THEN 1 ELSE 0 END) AS audio,
                  COUNT(*) AS total,
                  SUM(CASE WHEN invalid=1 THEN 1 ELSE 0 END) AS invalid
                FROM group_files WHERE chat_id=? AND invalid=0
            """ % (_ext_cond(_IMG_EXT), _ext_cond(_VID_EXT), _ext_cond(_AUD_EXT))
        r = query_cache_db(_stats_sql, (chat_id,))
        if r:
            stats["photo"] = r[0]["photo"] or 0
            stats["video"] = r[0]["video"] or 0
            stats["audio"] = r[0]["audio"] or 0
            stats["other"] = (r[0]["total"] or 0) - stats["photo"] - stats["video"] - stats["audio"]
    except Exception:
        pass
    # 失效（源频道不可用）单独计数（在 invalid=0 统计之外再查一次）
    try:
        r = query_cache_db("SELECT COUNT(*) AS c FROM group_files WHERE chat_id=? AND invalid=1", (chat_id,))
        if r:
            stats["invalid"] = r[0]["c"] or 0
    except Exception:
        pass

    # 7. 最近请求状态
    req_status = "none"
    req_result = ""
    req_id = 0
    try:
        r = query_db("SELECT id, status, result FROM web_requests WHERE chat_id=? ORDER BY id DESC LIMIT 1", (chat_id,))
        if r:
            req_id = r[0]["id"]
            req_status = {0: "pending", 1: "running", 2: "done", 3: "failed"}.get(r[0]["status"], "none")
            req_result = r[0]["result"] or ""
    except Exception:
        pass
    return {
        "files": files, "total": total, "page": page, "page_size": page_size,
        "stats": stats, "req_status": req_status, "req_result": req_result, "req_id": req_id,
    }


PAGE_HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<title>OpenBot 群文件查看器</title>
<style>
  * { box-sizing: border-box; }
  body { font-family: "Microsoft YaHei", "PingFang SC", sans-serif; margin: 0; background: #f5f6fa; color: #2c3e50; }
  .wrap { max-width: 1500px; margin: 0 auto; padding: 20px; }
  h1 { font-size: 22px; margin: 0 0 4px; }
  .meta { color: #7f8c8d; font-size: 13px; margin-bottom: 12px; }
  .toolbar { display: flex; gap: 10px; flex-wrap: wrap; align-items: center; background: #fff; padding: 12px 14px; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,.08); margin-bottom: 12px; }
  .toolbar input, .toolbar select { padding: 7px 10px; border: 1px solid #d5dbe3; border-radius: 6px; font-size: 13px; background: #fff; color: #2c3e50; }
  .toolbar input:focus, .toolbar select:focus { outline: none; border-color: #3498db; }
  #q { flex: 1; min-width: 160px; }
  #fsrc { min-width: 180px; }
  .toolbar label { font-size: 12px; color: #7f8c8d; }
  .clear-btn { background: #ecf0f1; border: 1px solid #d5dbe3; border-radius: 6px; padding: 7px 12px; cursor: pointer; font-size: 13px; color: #2c3e50; }
  .clear-btn:hover { background: #dfe6e9; }
  .stats { display: flex; gap: 10px; margin-bottom: 12px; flex-wrap: wrap; }
  .stat { background: #fff; padding: 8px 16px; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,.08); display: flex; flex-direction: column; align-items: center; min-width: 84px; }
  .stat span { font-size: 12px; color: #7f8c8d; }
  .stat b { font-size: 19px; }
  .stat.total b { color: #2c3e50; } .stat.unique b { color: #27ae60; } .stat.dup b { color: #e67e22; } .stat.noid b { color: #95a5a6; } .stat.failed b { color: #c0392b; }
  .tbl-wrap { background: #fff; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,.08); overflow: auto; max-height: calc(100vh - 300px); }
  table { width: 100%; border-collapse: collapse; }
  th { background: #2c3e50; color: #fff; padding: 10px 12px; text-align: left; font-size: 13px; position: sticky; top: 0; cursor: pointer; user-select: none; white-space: nowrap; }
  th:hover { background: #34495e; }
  th .arrow { font-size: 11px; opacity: .8; }
  td { padding: 8px 12px; border-bottom: 1px solid #eee; font-size: 13px; vertical-align: middle; }
  tbody tr:hover { background: #f0f4ff; }
  tbody tr.dup { background: #fff3e0; }
  tbody tr.dup td:first-child { border-left: 4px solid #e67e22; }
  .fname { max-width: 340px; word-break: break-all; }
  .failed { color: #c0392b; font-weight: bold; }
  .done { color: #27ae60; }
  .downloading { color: #2980b9; }
  .pending { color: #8e44ad; }
  .noid { color: #95a5a6; }
  .src { color: #2980b9; font-size: 12px; max-width: 220px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; cursor: pointer; text-decoration: underline dotted; }
  .src:hover { color: #1a5276; }
  .badge { display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 12px; }
  .b-dup { background: #e67e22; color: #fff; }
  .b-unique { background: #27ae60; color: #fff; }
.b-invalid { background: #95a5a6; color: #fff; }
.fname.invalid { color: #95a5a6; font-style: italic; }
.noid { color: #bdc3c7; }
tr.inv { background: #fafafa; }
  .b-noid { background: #bdc3c7; color: #333; }
  code { background: #f0f0f0; padding: 1px 5px; border-radius: 3px; font-size: 12px; }
  .empty { text-align: center; color: #95a5a6; padding: 40px 0; }
  /* 群详情卡片 */
  .gcard { background: #fff; border: 2px solid #3498db; border-radius: 10px; box-shadow: 0 4px 12px rgba(0,0,0,.12); margin-bottom: 12px; overflow: hidden; }
  .gcard-head { background: #3498db; color: #fff; padding: 10px 14px; display: flex; justify-content: space-between; align-items: center; font-size: 15px; }
  .gcard-head button { background: rgba(255,255,255,.25); color: #fff; border: none; border-radius: 6px; padding: 4px 12px; cursor: pointer; font-size: 13px; }
  .gcard-head button:hover { background: rgba(255,255,255,.4); }
  .ginfo { width: 100%; border-collapse: collapse; }
  .ginfo td { padding: 7px 14px; border-bottom: 1px solid #f0f0f0; font-size: 13px; }
  .ginfo td:first-child { width: 90px; color: #7f8c8d; font-weight: bold; background: #fafbfc; }
  .ginfo .big { font-size: 16px; font-weight: bold; }
  .g-badge { display: inline-block; padding: 2px 10px; border-radius: 10px; margin-right: 6px; font-size: 12px; color: #fff; }
  .g-b-blue { background: #3498db; } .g-b-green { background: #27ae60; } .g-b-orange { background: #e67e22; } .g-b-red { background: #c0392b; } .g-b-gray { background: #95a5a6; }
  /* 群列表操作按钮 */
  .g-btn { display: inline-block; padding: 3px 10px; border-radius: 6px; margin-right: 6px; font-size: 12px; color: #fff; border: none; cursor: pointer; }
  .g-btn-open { background: #3498db; } .g-btn-open:hover { background: #2980b9; }
  .g-btn-inc { background: #27ae60; } .g-btn-inc:hover { background: #1e8449; }
  .g-btn-full { background: #e67e22; } .g-btn-full:hover { background: #d35400; }
  .g-btn-scan { background: #9b59b6; } .g-btn-scan:hover { background: #8e44ad; }
  .g-btn-stat { background: #1abc9c; } .g-btn-stat:hover { background: #16a085; }
  /* Tab 切换 */
  .tabs { display: flex; gap: 8px; margin: 10px 0 12px; }
  .tab { padding: 8px 20px; border: 1px solid #d5dbe3; border-radius: 8px; background: #fff; cursor: pointer; font-size: 14px; color: #2c3e50; }
  .tab:hover { background: #f0f4ff; }
  .tab.active { background: #3498db; color: #fff; border-color: #3498db; font-weight: bold; }
  /* 群列表（视图2） */
  .glist { display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); gap: 10px; }
  .gitem { background: #fff; border: 1px solid #e3e8ee; border-radius: 8px; padding: 10px 14px; cursor: pointer; transition: border-color .15s, box-shadow .15s; }
  .gitem:hover { border-color: #3498db; box-shadow: 0 2px 8px rgba(52,152,219,.15); }
  .gitem .gn { font-size: 14px; font-weight: bold; word-break: break-all; }
  .gitem .gm { font-size: 12px; color: #7f8c8d; margin-top: 4px; }
  .gitem .badges { margin-top: 6px; }
  .gitem .gfile { font-size: 12px; color: #27ae60; margin-top: 4px; font-weight: bold; }
  .gitem .gn { cursor: pointer; }
  .gitem .gn:hover { color: #3498db; text-decoration: underline; }
  .gitem .grow { display: flex; justify-content: space-between; align-items: center; margin-top: 6px; }
  .gitem .gstat { font-size: 13px; color: #27ae60; font-weight: bold; white-space: nowrap; }
  .gitem .gstat-pending { color: #e67e22; font-weight: normal; animation: pulse 1.5s infinite; }
  .gitem .gstat-none { color: #bdc3c7; font-weight: normal; }
  .gitem .gscan { font-size: 12px; color: #e67e22; font-weight: bold; white-space: nowrap; animation: pulse 1.5s infinite; }
  .gitem .gprog { font-size: 13px; color: #2980b9; font-weight: bold; white-space: nowrap; }
  .gitem .gprog-none { color: #95a5a6; font-weight: normal; }
  @keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.5; } }
  .btn-dl { background: #27ae60; color: #fff; border: none; border-radius: 6px; padding: 5px 12px; cursor: pointer; font-size: 12px; }
  .btn-dl:hover { background: #1e8449; }
  .btn-dl:disabled { background: #95a5a6; cursor: not-allowed; }
  .btn-rescan { background: #3498db; color: #fff; border: none; border-radius: 6px; padding: 4px 12px; cursor: pointer; font-size: 12px; margin-left: 8px; }
  .scanning { color: #2980b9; font-weight: bold; }
  .scan-done { color: #27ae60; }
  .scan-failed { color: #c0392b; }
  .pager-row td { text-align: center; padding: 12px 8px; background: #f8f9fa; border-top: 2px solid #dee2e6; }
  .btn-page { background: #3498db; color: #fff; border: none; border-radius: 6px; padding: 6px 16px; cursor: pointer; font-size: 13px; margin: 0 8px; }
  .btn-page:hover { background: #2980b9; }
  .btn-page:disabled { background: #bdc3c7; cursor: not-allowed; }
.btn-page-mini { background: #fff; color: #3498db; border: 1px solid #c5d9ea; border-radius: 6px; padding: 5px 9px; cursor: pointer; font-size: 13px; margin: 0 2px; }
.btn-page-mini:hover { background: #eef5fb; }
.btn-page-mini:disabled { background: #3498db; color: #fff; border-color: #3498db; cursor: default; }
  .page-info { font-size: 13px; color: #555; margin: 0 12px; }
  /* 筛选标签 */
  .filter-tabs { display: flex; gap: 6px; margin: 10px 0 12px; flex-wrap: wrap; }
  .filter-tab { padding: 5px 16px; border: 1px solid #d0d7de; border-radius: 20px; background: #fff; color: #57606a; cursor: pointer; font-size: 13px; transition: all 0.15s; user-select: none; }
  .filter-tab:hover { background: #f6f8fa; border-color: #8c959f; }
  .filter-tab.active { background: #0969da; color: #fff; border-color: #0969da; font-weight: 600; }
  .filter-tab .cnt { font-size: 11px; opacity: 0.7; margin-left: 4px; }
  /* 详情面板：搜索 + 类别标签 + 排序（V2） */
  .gf-toolbar { display: flex; gap: 8px; padding: 10px 14px 0; flex-wrap: wrap; align-items: center; }
  .gf-toolbar input { flex: 1; min-width: 200px; padding: 7px 10px; border: 1px solid #d5dbe3; border-radius: 6px; font-size: 13px; background: #fff; color: #2c3e50; }
  .gf-toolbar input:focus { outline: none; border-color: #3498db; }
  .gf-toolbar .hint { font-size: 12px; color: #7f8c8d; white-space: nowrap; }
  .gf-tabs { display: flex; gap: 6px; padding: 8px 14px 10px; flex-wrap: wrap; }
  .gf-tab { padding: 4px 14px; border: 1px solid #d0d7de; border-radius: 16px; background: #fff; color: #57606a; cursor: pointer; font-size: 13px; user-select: none; transition: all .15s; }
  .gf-tab:hover { background: #f0f4ff; border-color: #8c959f; }
  .gf-tab.active { background: #3498db; color: #fff; border-color: #3498db; font-weight: 600; }
  .gf-tab .cnt { font-size: 11px; opacity: .75; margin-left: 4px; }
  .b-dup { cursor: pointer; }
  .b-dup:hover { background: #d35400; }
  #mtStatusBar { display: none; background: #fff0f0; border: 1px solid #e0a0a0; color: #b03030;
    padding: 10px 14px; border-radius: 8px; margin-bottom: 12px; font-size: 14px; line-height: 1.6; }
  #mtStatusBar b { font-weight: 700; }
</style>
</head>
<body>
<div class="wrap">
<div id="mtStatusBar">⚠️ <b>MTProto 未登录</b>：请先在 Telegram 私聊机器人发送 <b>/mtlogin</b> 完成登录，才能扫描群文件。登录后刷新此页即可。</div>
<h1>📁 OpenBot 群文件查看器</h1>
<div class="tabs">
  <button class="tab" id="tab1" onclick="switchView(1)">📥 下载任务</button>
  <button class="tab active" id="tab2" onclick="switchView(2)">🔍 群文件查询</button>
  <button class="tab" id="tab3" onclick="switchView(3)">📤 推广转发</button>
</div>

<div id="view1" style="display:none">
<p class="meta" id="meta">加载中...</p>

<div class="toolbar">
  <input id="q" type="text" placeholder="🔍 搜索文件名 / 文件ID..." oninput="render()">
  <label>状态</label>
  <select id="fst" onchange="render()">
    <option value="">全部</option>
    <option value="2">✅ 已下载</option>
    <option value="0">⏳ 待下载</option>
    <option value="1">⬇️ 下载中</option>
    <option value="3">❌ 失败</option>
    <option value="4">⚠️ 已删除</option>
  </select>
  <label>重复</label>
  <select id="fdup" onchange="render()">
    <option value="">全部</option>
    <option value="dup">🔁 重复</option>
    <option value="unique">✅ 唯一</option>
    <option value="noid">⚠️ 无ID</option>
  </select>
  <label>来源群</label>
  <input id="fsrc" list="grouplist" placeholder="全部来源（输入搜索群）" onchange="onGroupPick()">
  <datalist id="grouplist"></datalist>
</div>

<div class="stats" id="stats"></div>

<div id="groupCard" style="display:none"></div>

<div class="tbl-wrap">
<table>
<thead><tr>
  <th onclick="setSort('idx')"># <span class="arrow"></span></th>
  <th onclick="setSort('f')">文件名 <span class="arrow"></span></th>
  <th onclick="setSort('id')">文件ID <span class="arrow"></span></th>
  <th>消息ID</th>
  <th onclick="setSort('s')">大小 <span class="arrow"></span></th>
  <th onclick="setSort('cnt')">是否重复 <span class="arrow"></span></th>
  <th onclick="setSort('st')">状态 <span class="arrow"></span></th>
  <th>来源（点击看群详情）</th>
</tr></thead>
<tbody id="tbody"></tbody>
</table>
</div>
</div><!-- /view1 -->

<div id="view2">
<div class="toolbar">
  <input id="gq" type="text" placeholder="🔍 搜索群聊..." oninput="renderGroups()">
  <button class="clear-btn" onclick="syncAndRefreshGroups()">🔄 刷新频道列表</button>
</div>
<div class="filter-tabs" id="filterTabs">
  <span class="filter-tab active" data-filter="all" onclick="setFilter('all',this)">全部 <span class="cnt" id="fc_all">0</span></span>
  <span class="filter-tab" data-filter="group" onclick="setFilter('group',this)">👥 群聊 <span class="cnt" id="fc_group">0</span></span>
  <span class="filter-tab" data-filter="channel" onclick="setFilter('channel',this)">📢 频道 <span class="cnt" id="fc_channel">0</span></span>
  <span class="filter-tab" data-filter="user" onclick="setFilter('user',this)">💬 私聊 <span class="cnt" id="fc_user">0</span></span>
  <span class="filter-tab" data-filter="saved" onclick="setFilter('saved',this)">⭐ 收藏夹 <span class="cnt" id="fc_saved">0</span></span>
</div>
<p class="meta" id="gstatus">加载群列表...</p>
<div id="glist"></div>
<div id="gfWrap" style="display:none">
  <div id="gfHead"></div>
  <div class="tbl-wrap">
  <table>
  <thead><tr>
    <th>#</th><th>文件名</th><th>文件ID</th><th>消息ID</th><th onclick="setGSort('size')">大小 <span class="arrow" id="gar_size"></span></th><th onclick="setGSort('dup')">状态 <span class="arrow" id="gar_dup"></span></th><th>对话ID</th><th>操作</th>
  </tr></thead>
  <tbody id="gftbody"></tbody>
  </table>
  </div>
</div>
<!-- 群详情弹窗（视图2） -->
<div id="gdetail2" style="display:none; position:fixed; top:0; left:0; width:100%; height:100%; background:rgba(0,0,0,.5); z-index:999; display:none; align-items:center; justify-content:center;">
  <div class="gcard" style="max-width:520px; width:90%;">
    <div class="gcard-head"><span id="gd2_title">群详情</span><button onclick="closeGroupDetail2()">✕ 关闭</button></div>
    <div id="gd2_body" style="padding:14px;"></div>
  </div>
</div>
</div><!-- /view2 -->

<div id="view3" style="display:none">
  <iframe id="promoteFrame" src="" style="width:100%;min-height:calc(100vh - 150px);border:0;background:#f5f6fa;"></iframe>
</div><!-- /view3 -->
</div>

<script>
var FILES = [], GROUPS = [], DIALOGS = [];
var STATUS = {
  0: ["⏳ 待下载", "pending"],
  1: ["⬇️ 下载中", "downloading"],
  2: ["✅ 已下载", "done"],
  3: ["❌ 下载失败·可能失效", "failed"],
  4: ["⚠️ 已删除（源文件已失效）", "skipped"]
};
var sortKey = "idx", sortDir = 1;

function esc(s) { return String(s == null ? "" : s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;"); }
function fmtSize(b) {
  if (!b) return "0 B";
  var u = ["B", "KB", "MB", "GB", "TB"], i = 0;
  while (b >= 1024 && i < u.length - 1) { b /= 1024; i++; }
  return (b >= 100 ? b.toFixed(0) : b.toFixed(1)) + " " + u[i];
}

fetch("/api/data").then(function (r) { return r.json(); }).then(function (d) {
  if (d.error) { document.getElementById("meta").textContent = "❌ " + d.error; return; }
  FILES = d.files; GROUPS = d.groups; DIALOGS = d.dialogs || [];
  document.getElementById("meta").textContent =
    "共 " + FILES.length + " 个文件 / " + GROUPS.length + " 个群有任务" +
    (DIALOGS.length ? " / 全部对话 " + DIALOGS.length + " 个（来源下拉可搜索任意群）" : "");
  buildGroupList();
  render();
}).catch(function (e) {
  document.getElementById("meta").textContent = "❌ 数据加载失败: " + e;
});

function buildGroupList() {
  var dl = document.getElementById("grouplist");
  var src = DIALOGS.length ? DIALOGS : GROUPS;
  dl.innerHTML = src.map(function (g) {
    return '<option value="' + esc(g.name) + '">';
  }).join("");
}

function onGroupPick() {
  var name = document.getElementById("fsrc").value.trim();
  if (!name) { hideGroup(); render(); return; }
  showDialog(name, true);
}

function showDialog(name, doFilter) {
  // 💡 按群名（chat_name）匹配：转发任务的 chat_id 是 bot 私聊 ID（存储逻辑不变），
  //    群区分靠 chat_name，来源类型看 jobs.source
  //    doFilter=true（来源下拉选定）时表格筛选到该群；点击来源单元格（false）只显示详情，表格保持全部
  var g = GROUPS.find(function (x) { return x.name === name; });
  var d = DIALOGS.find(function (x) { return x.name === name; });
  var display = name || ((g && g.name) || (d && d.name) || "未知群");
  document.getElementById("fsrc").value = display;
  var card = document.getElementById("groupCard");
  card.style.display = "block";
  var typeRow = (d && d.type) ? '<tr><td>类型</td><td>' + esc(d.type) + (d.username ? ' · <code>@' + esc(d.username) + '</code>' : '') + '</td></tr>' : '';
  var syncRow = (d && d.updated_at) ? '<tr><td>列表同步</td><td>' + esc(d.updated_at) + '</td></tr>' : '';
  if (g) {
    var srcIdRow = (g.source_ids && g.source_ids.length) ? '<tr><td>来源群ID</td><td><code>' + esc(g.source_ids.join("</code> <code>")) + '</code></td></tr>' : '';
    card.innerHTML =
      '<div class="gcard"><div class="gcard-head"><span>📌 群详情：' + esc(g.name) + '</span><button onclick="closeGroup()">✕ 关闭</button></div>' +
      '<table class="ginfo">' +
      '<tr><td>群名</td><td class="big">' + esc(g.name) + '</td></tr>' +
      '<tr><td>chat_id</td><td><code>' + esc(g.id) + '</code></td></tr>' +
      srcIdRow +
      typeRow +
      '<tr><td>来源类型</td><td>' + (g.sources.length ? g.sources.map(function (s) { return "<span class='g-badge g-b-blue'>" + esc(s) + "</span>"; }).join(" ") : '<span class="noid">无记录</span>') + '</td></tr>' +
      '<tr><td>任务数</td><td><span class="g-badge g-b-blue">' + g.jobs + ' 个任务</span></td></tr>' +
      '<tr><td>文件统计</td><td><span class="g-badge g-b-blue">' + g.files + ' 个文件</span> <span class="g-badge g-b-gray">共 ' + fmtSize(g.size) + '</span></td></tr>' +
      '<tr><td>下载情况</td><td><span class="g-badge g-b-green">✅ ' + g.done + '</span> <span class="g-badge g-b-orange">🔁 重复 ' + g.dup + '</span> <span class="g-badge g-b-red">❌ ' + g.failed + '</span> <span class="g-badge g-b-gray">⚠️ 无ID ' + g.noid + '</span></td></tr>' +
      '<tr><td>最近下载</td><td>' + esc(g.last || "暂无") + '</td></tr>' +
      syncRow +
      '</table></div>';
  } else if (d) {
    card.innerHTML =
      '<div class="gcard"><div class="gcard-head"><span>📌 群详情：' + esc(d.name) + '</span><button onclick="closeGroup()">✕ 关闭</button></div>' +
      '<table class="ginfo">' +
      '<tr><td>群名</td><td class="big">' + esc(d.name) + '</td></tr>' +
      '<tr><td>chat_id</td><td><code>' + esc(d.id) + '</code></td></tr>' +
      typeRow +
      '<tr><td>下载记录</td><td><span class="noid">📭 该群暂无下载记录</span></td></tr>' +
      syncRow +
      '</table></div>';
  } else {
    hideGroup();
    return;
  }
  if (doFilter) {
    document.getElementById("fsrc").value = display;
    render(); // 来源下拉选定 → 表格筛选到该群（无任务群自然显示空）
  }
}

function hideGroup() {
  document.getElementById("groupCard").style.display = "none";
}
function closeGroup() { hideGroup(); render(); }

function setSort(k) {
  if (sortKey === k) { sortDir = -sortDir; } else { sortKey = k; sortDir = (k === "f" || k === "src") ? 1 : -1; }
  render();
}

function resetAll() {
  document.getElementById("q").value = "";
  document.getElementById("fst").value = "";
  document.getElementById("fdup").value = "";
  document.getElementById("fsrc").value = "";
  sortKey = "idx"; sortDir = 1;
  hideGroup();
  render();
}

function render() {
  var q = document.getElementById("q").value.trim().toLowerCase();
  var st = document.getElementById("fst").value;
  var dup = document.getElementById("fdup").value;
  var grp = document.getElementById("fsrc").value.trim();

  // 文件行: [0]name [1]doc_id [2]size [3]status [4]jid [5]chat_id [6]chat_name [7]msg_id [8]source [9]source_id [10]cnt [11]updated_at
  var rows = FILES.filter(function (r) {
    if (q && r[0].toLowerCase().indexOf(q) < 0 && r[1].toLowerCase().indexOf(q) < 0) return false;
    if (st && String(r[3]) !== st) return false;
    if (dup === "dup" && !(r[1] && r[10] > 1)) return false;
    if (dup === "unique" && !(r[1] && r[10] === 1)) return false;
    if (dup === "noid" && r[1]) return false;
    if (grp && r[6] !== grp) return false;
    return true;
  });

  if (sortKey !== "idx") {
    rows.sort(function (a, b) {
      var va, vb;
      if (sortKey === "f") { va = a[0].toLowerCase(); vb = b[0].toLowerCase(); }
      else if (sortKey === "id") { va = a[1]; vb = b[1]; }
      else if (sortKey === "s") { va = a[2]; vb = b[2]; }
      else if (sortKey === "st") { va = a[3]; vb = b[3]; }
      else if (sortKey === "cnt") { va = a[10]; vb = b[10]; }
      else { va = a[6].toLowerCase(); vb = b[6].toLowerCase(); }
      if (va < vb) return -1 * sortDir;
      if (va > vb) return 1 * sortDir;
      return 0;
    });
  }

  var unique = 0, dupn = 0, noid = 0, failed = 0, skipped = 0;
  rows.forEach(function (r) {
    if (r[1]) { if (r[10] > 1) dupn++; else unique++; } else noid++;
    if (r[3] === 3) failed++;
    if (r[3] === 4) skipped++;
  });
  document.getElementById("stats").innerHTML =
    '<div class="stat total"><span>总记录</span><b>' + rows.length + '</b></div>' +
    '<div class="stat unique"><span>✅ 唯一</span><b>' + unique + '</b></div>' +
    '<div class="stat dup"><span>🔁 重复</span><b>' + dupn + '</b></div>' +
    '<div class="stat noid"><span>⚠️ 无ID</span><b>' + noid + '</b></div>' +
    '<div class="stat failed"><span>❌ 失败</span><b>' + failed + '</b></div>' +
    '<div class="stat skipped"><span>⚠️ 已删除</span><b>' + skipped + '</b></div>';

  var heads = document.querySelectorAll("th .arrow");
  heads.forEach(function (a) { a.textContent = ""; });
  var idxMap = { idx: 0, f: 1, id: 2, s: 3, cnt: 4, st: 5 };
  if (sortKey !== "idx") {
    heads[idxMap[sortKey]].textContent = sortDir > 0 ? "▲" : "▼";
  }

  var tbody = document.getElementById("tbody");
  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="8" class="empty">📭 没有匹配的记录，试试调整筛选条件</td></tr>';
    return;
  }
  var html = "";
  rows.forEach(function (r, i) {
    var stInfo = STATUS[r[3]] || ["❓ 未知", "pending"];
    var dupHtml, rowCls = "", idHtml;
    if (r[1]) {
      dupHtml = r[10] > 1 ? '<span class="badge b-dup">🔁 重复</span>' : '<span class="badge b-unique">✅ 唯一</span>';
      if (r[10] > 1) rowCls = ' class="dup"';
      idHtml = '<code>' + esc(r[1]) + '</code>';
    } else {
      dupHtml = '<span class="badge b-noid">⚠️ 无ID</span>';
      idHtml = '<span class="noid">未记录</span>';
    }
    // 💡 来源显示：群名 + 真实来源ID（r[9]=source_id，转发任务的真实来源群；无则用 r[5]=chat_id）+ 来源类型
    //    显示 ID 是为了用户能用 /dl 命令指定群下载
    var srcIdShow = r[9] || r[5];
    var srcTxt = (r[6] || ("群 " + r[5])) + (srcIdShow ? " [" + srcIdShow + "]" : "") + (r[8] ? "（" + r[8] + "）" : "");
    html += '<tr' + rowCls + '>' +
      '<td>' + (i + 1) + '</td>' +
      '<td class="fname" title="' + esc(r[0]) + '">' + esc(r[0]) + '</td>' +
      '<td>' + idHtml + '</td>' +
      '<td><code>' + esc(r[7] || "") + '</code></td>' +
      '<td>' + fmtSize(r[2]) + '</td>' +
      '<td>' + dupHtml + '</td>' +
      '<td class="' + stInfo[1] + '">' + stInfo[0] + '</td>' +
      '<td class="src" onclick="showDialog(' + JSON.stringify(r[6] || ("群 " + r[5])).replace(/"/g, "&quot;") + ', false)" title="点击查看群详情">' + esc(srcTxt) + '</td>' +
      '</tr>';
  });
  tbody.innerHTML = html;
}

/* ===================== 视图2：群文件查询 ===================== */
var GROUPS_ALL = [], GFFILES = [], GF_REQ = null, GF_CHAT_ID = null;
var GF_PAGE = 1, GF_PAGE_SIZE = 100, GF_TOTAL = 0, GF_STATS = {photo:0,video:0,audio:0,other:0};
var GF_FILTER = "all";
var GF_Q = "", GF_KIND = "all", GF_SORT = { key: "", dir: 1 };

var _groupRefreshTimer = null;
function switchView(n) {
  document.getElementById("view1").style.display = n === 1 ? "block" : "none";
  document.getElementById("view2").style.display = n === 2 ? "block" : "none";
  document.getElementById("view3").style.display = n === 3 ? "block" : "none";
  document.getElementById("tab1").className = "tab" + (n === 1 ? " active" : "");
  document.getElementById("tab2").className = "tab" + (n === 2 ? " active" : "");
  document.getElementById("tab3").className = "tab" + (n === 3 ? " active" : "");
  if (n === 2) {
    loadGroups();
    if (_groupRefreshTimer) clearInterval(_groupRefreshTimer);
    _groupRefreshTimer = setInterval(function () {
      if (document.getElementById("view2").style.display !== "none") loadGroups(true);
    }, 5000);
  } else {
    if (_groupRefreshTimer) { clearInterval(_groupRefreshTimer); _groupRefreshTimer = null; }
  }
  if (n === 3) {
    var _f = document.getElementById("promoteFrame");
    if (_f && !_f.getAttribute("src")) _f.setAttribute("src", "/promote.html?embed=1");
  }
}

function loadGroups(silent) {
  // silent=true：5 秒自动轮询的静默刷新——不闪状态文字（群数量只在“刷新频道列表”同步后变化），
  // 但群行的扫描状态/统计数照常更新
  if (!silent) document.getElementById("gstatus").textContent = "🔄 正在刷新群列表...";
  fetch("/api/groups").then(function (r) { return r.json(); }).then(function (d) {
    if (d.error) { if (!silent) document.getElementById("gstatus").textContent = "❌ " + d.error; return; }
    GROUPS_ALL = d.groups;
    if (!silent) document.getElementById("gstatus").textContent =
      "共 " + GROUPS_ALL.length + " 个群聊（点击群 → 扫描并查看该群文件）";
    // 更新筛选标签各类型数量
    var cnt = { all: GROUPS_ALL.length, group: 0, channel: 0, user: 0, saved: 0 };
    GROUPS_ALL.forEach(function (g) {
      var t = g.type || "";
      if (t === "群") cnt.group++;
      else if (t === "频道") cnt.channel++;
      else if (t === "用户") cnt.user++;
      else if (t === "收藏夹") cnt.saved++;
    });
    document.getElementById("fc_all").textContent = cnt.all;
    document.getElementById("fc_group").textContent = cnt.group;
    document.getElementById("fc_channel").textContent = cnt.channel;
    document.getElementById("fc_user").textContent = cnt.user;
    document.getElementById("fc_saved").textContent = cnt.saved;
    renderGroups();
  }).catch(function (e) {
    if (!silent) document.getElementById("gstatus").textContent = "❌ 群列表加载失败: " + e;
  });
}

function syncAndRefreshGroups() {
  // 点"刷新频道列表"：先写同步请求让 bot 从 Telegram 拉最新对话，完成后再重读库显示
  var statusEl = document.getElementById("gstatus");
  statusEl.textContent = "🔄 正在从 Telegram 同步对话列表（bot 需在运行）...";
  fetch("/api/sync_dialogs").then(function (r) { return r.json(); }).then(function (d) {
    if (d.error) { statusEl.textContent = "❌ " + d.error; return; }
    var tries = 0;
    var t = setInterval(function () {
      tries++;
      fetch("/api/requests?id=" + d.id).then(function (r) { return r.json(); }).then(function (s) {
        if (s.status === 2) {
          clearInterval(t);
          statusEl.textContent = "✅ " + (s.result || "同步完成");
          loadGroups();
        } else if (s.status === 3) {
          clearInterval(t);
          statusEl.textContent = "❌ " + (s.result || "同步失败");
        } else if (tries > 90) {
          clearInterval(t);
          statusEl.textContent = "⚠️ 同步超时（3分钟），请确认 bot 正在运行后重试";
        }
      }).catch(function () {});
    }, 2000);
  }).catch(function (e) {
    statusEl.textContent = "❌ 发起同步失败: " + e;
  });
}

function renderGroups() {
  var q = document.getElementById("gq").value.trim().toLowerCase();
  // 保留原始索引：过滤后 clickGroup(i) 仍取 GROUPS_ALL 正确条目
  var list = [];
  GROUPS_ALL.forEach(function (g, idx) {
    if (q && (g.name || "").toLowerCase().indexOf(q) < 0) return;
    // 类型筛选（全部/群聊/频道/私聊/收藏夹）
    if (GF_FILTER !== "all") {
      var t = g.type || "";
      if (GF_FILTER === "group" && t !== "群") return;
      if (GF_FILTER === "channel" && t !== "频道") return;
      if (GF_FILTER === "user" && t !== "用户") return;
      if (GF_FILTER === "saved" && t !== "收藏夹") return;
    }
    list.push({ g: g, idx: idx });
  });
  var el = document.getElementById("glist");
  if (!list.length) {
    el.innerHTML = '<div class="empty">📭 没有匹配的群聊</div>';
    return;
  }
  var html = "";
  list.forEach(function (item) {
    var g = item.g, i = item.idx;
    var isScanning = (g.req_status === 0 || g.req_status === 1);
    // 按钮（data-action 用于事件委托，避免5秒刷新重建DOM后内联onclick失效）
    var openBtn = '<button class="g-btn g-btn-open" data-action="open" data-index="' + i + '">打开</button>';
    var scanBtns;
    if (g.scanned) {
      scanBtns = '<button class="g-btn g-btn-inc" data-action="scan_inc" data-index="' + i + '">增量扫描</button>'
        + '<button class="g-btn g-btn-full" data-action="scan_full" data-index="' + i + '">重新扫描</button>';
    } else {
      scanBtns = '<button class="g-btn g-btn-scan" data-action="scan" data-index="' + i + '">扫描</button>';
    }
    // 中间：已读取文件数（group_files 表实际扫描缓存条数）
    var middle;
    if (isScanning) {
      var progText = g.req_result && g.req_result !== "处理中" ? g.req_result : "排队中...";
      // id 供扫描进度轮询（pollRequest）高频更新该行进度，与文件面板进度保持一致
      middle = '<span class="gscan" id="gscan_' + String(g.id) + '">⏳ ' + esc(progText) + '</span>';
    } else if (g.req_result) {
      // 扫描完成/失败：显示后端结果文本（含"其中 N 条源频道不可用"对账），悬停可看全文
      middle = '<span class="gprog" title="' + esc(g.req_result) + '">' + esc(g.req_result) + '</span>';
    } else if (g.scanned_count > 0) {
      middle = '<span class="gprog">已读取 ' + Number(g.scanned_count).toLocaleString() + '</span>';
    } else {
      middle = '<span class="gprog gprog-none">未扫描</span>';
    }
    // 右侧：Telegram 分类统计（自动获取，无需手动点）
    var statRight;
    if (g.has_stats && g.tg_total > 0) {
      var parts = [];
      if (g.tg_photo > 0) parts.push("🖼️图" + Number(g.tg_photo).toLocaleString());
      if (g.tg_video > 0) parts.push("🎬视" + Number(g.tg_video).toLocaleString());
      if (g.tg_audio > 0) parts.push("🎵音" + Number(g.tg_audio).toLocaleString());
      if (g.tg_file > 0) parts.push("📁文" + Number(g.tg_file).toLocaleString());
      statRight = '<span class="gstat" title="图片/视频/音频/文件（Telegram统计）">' + parts.join(" ") + '</span>';
    } else if (g.scanned && g.scanned_count > 0) {
      var sparts = [];
      if (g.scanned_photo > 0) sparts.push("🖼️图" + Number(g.scanned_photo).toLocaleString());
      if (g.scanned_video > 0) sparts.push("🎬视" + Number(g.scanned_video).toLocaleString());
      if (g.scanned_audio > 0) sparts.push("🎵音" + Number(g.scanned_audio).toLocaleString());
      if (g.scanned_other > 0) sparts.push("📁文" + Number(g.scanned_other).toLocaleString());
      statRight = '<span class="gstat" title="已扫描文件分类">' + sparts.join(" ") + '</span>';
    } else if (g.count_status === 0 || g.count_status === 1) {
      statRight = '<span class="gstat gstat-pending">⏳统计获取中...</span>';
    } else {
      statRight = '<span class="gstat gstat-none">—</span>';
    }
    html += '<div class="gitem" data-action="detail" data-index="' + i + '">' +
      '<div class="gn" title="点击查看群详情">' + esc(g.name) + '</div>' +
      '<div class="gm">chat_id: <code>' + esc(g.id) + '</code></div>' +
      '<div class="grow"><div class="badges">' + openBtn + scanBtns + '</div>' + middle + statRight + '</div>' +
      '</div>';
  });
  el.innerHTML = html;
}

// 事件委托：所有按钮点击统一处理（5秒刷新重建DOM不影响）
document.getElementById("glist").addEventListener("click", function (e) {
  var target = e.target.closest("[data-action]");
  if (!target) return;
  var action = target.dataset.action;
  var idx = parseInt(target.dataset.index);
  if (action === "open") { clickGroup(idx); }
  else if (action === "scan_inc") { scanGroup(e, idx, 1); }
  else if (action === "scan_full") { scanGroup(e, idx, 0); }
  else if (action === "scan") { scanGroup(e, idx, 0); }
  else if (action === "detail") { showGroupDetail2(idx); }
});

function setFilter(f, el) {
  GF_FILTER = f;
  document.querySelectorAll(".filter-tab").forEach(function (t) { t.classList.remove("active"); });
  if (el) el.classList.add("active");
  renderGroups();
}

function resetGroupView() {
  document.getElementById("gq").value = "";
  renderGroups();
}

function closeGroupDetail2() {
  document.getElementById("gdetail2").style.display = "none";
}

function showGroupDetail2(i) {
  // 点击群名称 → 显示群详情（不直接打开文件列表）
  var g = GROUPS_ALL[i];
  var dlg = document.getElementById("gdetail2");
  dlg.style.display = "flex";
  document.getElementById("gd2_title").innerHTML = "📌 " + esc(g.name);
  document.getElementById("gd2_body").innerHTML =
    '<table class="ginfo">' +
    '<tr><td>群名</td><td class="big">' + esc(g.name) + '</td></tr>' +
    '<tr><td>chat_id</td><td><code>' + esc(g.id) + '</code></td></tr>' +
    '<tr><td>类型</td><td>' + esc(g.type || "未知") + '</td></tr>' +
    '<tr><td>扫描状态</td><td>' + (g.scanned ? '<span class="g-badge g-b-green">已扫描</span>' : '<span class="g-badge g-b-orange">未扫描</span>') + '</td></tr>' +
    '<tr><td>已缓存文件</td><td class="big">' + (g.file_count || 0) + ' 个</td></tr>' +
    '<tr><td colspan="2" style="color:#7f8c8d;font-size:12px;">正在读取分类统计...</td></tr>' +
    '</table>';
  // 读取该群详细统计（图片/视频/其它/最近扫描时间）
  fetch("/api/group_files?chat_id=" + encodeURIComponent(g.id))
    .then(function (r) { return r.json(); })
    .then(function (d) {
      if (d.error || !d.files) {
        document.getElementById("gd2_body").innerHTML +=
          '<div style="color:#c0392b;margin-top:8px;">读取失败: ' + esc(d.error || "无数据") + '</div>';
        return;
      }
      var st = d.stats || { photo: 0, video: 0, other: 0 };
      var lastScan = "";
      if (d.files && d.files.length) {
        lastScan = d.files[0].scanned_at || "";
      }
      document.getElementById("gd2_body").innerHTML =
        '<table class="ginfo">' +
        '<tr><td>群名</td><td class="big">' + esc(g.name) + '</td></tr>' +
        '<tr><td>chat_id</td><td><code>' + esc(g.id) + '</code></td></tr>' +
        '<tr><td>类型</td><td>' + esc(g.type || "未知") + '</td></tr>' +
        '<tr><td>扫描状态</td><td>' + (g.scanned ? '<span class="g-badge g-b-green">已扫描</span>' : '<span class="g-badge g-b-orange">未扫描</span>') + '</td></tr>' +
        '<tr><td>文件总数</td><td class="big">' + (d.count || 0) + ' 个</td></tr>' +
        '<tr><td>🖼️ 图片</td><td class="big">' + st.photo + '</td><td>🎬 视频</td><td class="big">' + st.video + '</td><td>📁 其它</td><td class="big">' + st.other + '</td></tr>' +
        (lastScan ? '<tr><td>最近扫描</td><td>' + esc(lastScan) + '</td></tr>' : '') +
        '</table>' +
        '<div style="margin-top:10px; display:flex; gap:8px;">' +
        '<button class="g-btn g-btn-open" onclick="closeGroupDetail2();clickGroup(' + i + ')">📂 打开文件列表</button>' +
        (g.scanned
          ? '<button class="g-btn g-btn-inc" onclick="closeGroupDetail2();scanGroup(event,' + i + ',1)">🔄 增量扫描</button>'
            + '<button class="g-btn g-btn-full" onclick="closeGroupDetail2();scanGroup(event,' + i + ',0)">🔄 重新扫描</button>'
          : '<button class="g-btn g-btn-scan" onclick="closeGroupDetail2();scanGroup(event,' + i + ',0)">🔍 开始扫描</button>') +
        '</div>';
    }).catch(function (e) {
      document.getElementById("gd2_body").innerHTML +=
        '<div style="color:#c0392b;margin-top:8px;">读取失败: ' + esc(e) + '</div>';
    });
}

function scanGroup(e, i, modeNum) {
  // modeNum: 0=重新扫描(full), 1=增量扫描(incremental)
  var mode = modeNum ? "incremental" : "full";
  var modeLabel = modeNum ? "增量扫描" : "重新扫描";
  var g = GROUPS_ALL[i];
  // 即时反馈：按钮变"扫描中..."并禁用，避免重复点击
  if (e && e.target) {
    e.target.innerHTML = "⏳ 扫描中...";
    e.target.disabled = true;
    e.target.style.opacity = "0.6";
  }
  var wrap = document.getElementById("gfWrap");
  wrap.style.display = "block";
  document.getElementById("gfHead").innerHTML =
    '<div class="gcard"><div class="gcard-head"><span>📌 ' + esc(g.name) +
    (g.id ? ' <code>(' + esc(g.id) + ')</code>' : '') +
    '</span><button onclick="closeGf()">✕ 关闭</button></div></div>';
  document.getElementById("gftbody").innerHTML =
    '<tr><td colspan="8" class="empty scanning">🔍 正在' + modeLabel + '该群文件（大群可能需要几十秒）...</td></tr>';
  GFFILES = [];
  GF_REQ = null;
  GF_CHAT_ID = g.id;
  // 滚动到文件查看区域，确保用户看到反馈
  wrap.scrollIntoView({ behavior: "smooth", block: "start" });
  fetch("/api/scan?chat_id=" + encodeURIComponent(g.id) + "&chat_name=" + encodeURIComponent(g.name) + "&mode=" + mode)
    .then(function (r) { return r.json(); })
    .then(function (s) {
      if (s.error) { showGfError(s.error); return; }
      GF_REQ = s.id;
      pollRequest(s.id, g);
    }).catch(function (err) { showGfError("发起扫描失败: " + err); });
}

function getGroupCount(i) {
  // 获取群媒体分类统计（Telegram count 接口，不遍历消息，几秒完成）
  var g = GROUPS_ALL[i];
  fetch("/api/get_count?chat_id=" + encodeURIComponent(g.id) + "&chat_name=" + encodeURIComponent(g.name))
    .then(function (r) { return r.json(); })
    .then(function (s) {
      if (s.error) { alert("获取统计失败: " + s.error); return; }
      // 轮询请求状态，完成后刷新群列表
      var t = setInterval(function () {
        fetch("/api/requests?id=" + s.id).then(function (r) { return r.json(); }).then(function (d) {
          if (d.status === 2) {
            clearInterval(t);
            loadGroups(true);  // 静默刷新显示统计，不闪文字
          } else if (d.status === 3) {
            clearInterval(t);
            alert("统计失败: " + (d.result || ""));
          }
        });
      }, 2000);
    }).catch(function (e) { alert("获取统计失败: " + e); });
}

function clickGroup(i) {
  var g = GROUPS_ALL[i];
  GF_CHAT_ID = g.id;
  GF_PAGE = 1;
  GF_Q = ""; GF_KIND = "all"; GF_SORT = { key: "", dir: 1 };
  var wrap = document.getElementById("gfWrap");
  wrap.style.display = "block";
  document.getElementById("gfHead").innerHTML =
    '<div class="gcard"><div class="gcard-head"><span>📌 ' + esc(g.name) +
    '</span><button onclick="closeGf()">✕ 关闭</button></div></div>';
  document.getElementById("gftbody").innerHTML =
    '<tr><td colspan="8" class="empty scanning">⏳ 正在读取该群文件...</td></tr>';
  GFFILES = [];
  GF_REQ = null;
  loadGroupPage(1, g);
}

function loadGroupPage(page, g) {
  if (!g) { g = GROUPS_ALL.find(function (x) { return String(x.id) === String(GF_CHAT_ID); }); }
  if (!g) return;
  GF_PAGE = page;
  fetch("/api/group_files?chat_id=" + encodeURIComponent(g.id) + "&page=" + page + "&page_size=" + GF_PAGE_SIZE
    + "&q=" + encodeURIComponent(GF_Q) + "&kind=" + encodeURIComponent(GF_KIND)
    + "&sort=" + encodeURIComponent(GF_SORT.key) + "&order=" + (GF_SORT.dir > 0 ? "asc" : "desc"))
    .then(function (r) { return r.json(); }).then(function (d) {
    if (d.error) { showGfError(d.error); return; }
    GFFILES = d.files || [];
    GF_TOTAL = d.total || 0;
    GF_STATS = d.stats || {photo:0, video:0, audio:0, other:0};
    if (GFFILES.length) { renderGroupFiles(); return; }
    // 空结果也更新提示行（显示 0 / 筛选后总数），避免残留旧值
    var h0 = document.getElementById("gfhint");
    if (h0) h0.textContent = "显示 0 / " + Number(d.total || 0).toLocaleString() + " 条";
    if (d.req_status === "done") {
      showGfEmpty("该群扫描完成但没有找到文件");
      return;
    }
    if (d.req_status === "failed") {
      showGfError("上次扫描失败: " + d.req_result);
      return;
    }
    if (d.req_status === "pending" || d.req_status === "running") {
      pollRequest(d.req_id, g);
      return;
    }
    // 未扫描过 → 发起扫描
    document.getElementById("gftbody").innerHTML =
      '<tr><td colspan="8" class="empty scanning">🔍 正在扫描该群文件（大群可能需要几十秒）...</td></tr>';
    fetch("/api/scan?chat_id=" + encodeURIComponent(g.id) + "&chat_name=" + encodeURIComponent(g.name))
      .then(function (r) { return r.json(); }).then(function (s) {
        if (s.error) { showGfError(s.error); return; }
        GF_REQ = s.id;
        pollRequest(s.id, g);
      }).catch(function (e) { showGfError("发起扫描失败: " + e); });
  }).catch(function (e) { showGfError("读取失败: " + e); });
}

function pollRequest(reqId, g) {
  var tries = 0;
  var t = setInterval(function () {
    tries++;
    fetch("/api/requests?id=" + reqId).then(function (r) { return r.json(); }).then(function (d) {
      if (d.status === 2) {
        clearInterval(t);
        document.getElementById("gftbody").innerHTML =
          '<tr><td colspan="8" class="empty scan-done">✅ ' + esc(d.result || "完成") + '</td></tr>';
        // 刷新群列表，让该群"已扫描"标记立即更新
        loadGroups(true);  // 静默刷新群列表，不闪文字
        // 扫描完成后加载第1页
        GF_PAGE = 1;
        loadGroupPage(1, g);
      } else if (d.status === 3) {
        clearInterval(t);
        showGfError("❌ " + esc(d.result || "失败"));
      } else {
        // 处理中：实时显示扫描进度（识别到多少 / 总数 百分比）
        var prog = d.result && d.result !== "处理中" ? " " + esc(d.result) : "";
        document.getElementById("gftbody").innerHTML =
          '<tr><td colspan="8" class="empty scanning">⏳ 扫描进行中' + prog + '...</td></tr>';
        // 同步更新群列表该行进度（与底部一致，不动其他行）
        var rowEl = document.getElementById("gscan_" + String(g.id));
        if (rowEl) {
          rowEl.textContent = "⏳ " + (d.result && d.result !== "处理中" ? d.result : "排队中...");
        }
        if (tries > 90) {
          clearInterval(t);
          showGfError("⏱️ 请求超时，请稍后重试");
        }
      }
    }).catch(function () {
      if (tries > 90) { clearInterval(t); showGfError("⏱️ 轮询失败，请重试"); }
    });
  }, 2000);
}

function renderGroupFiles() {
  var head = document.getElementById("gfHead");
  var gf = GFFILES[0] || {};
  var gObj = GROUPS_ALL.find(function (x) { return String(x.id) === String(GF_CHAT_ID); });
  var chatName = (gf.chat_name || (gObj && gObj.name) || "");
  var st = GF_STATS || { photo: 0, video: 0, audio: 0, other: 0 };
  head.innerHTML =
    '<div class="gcard"><div class="gcard-head"><span>📌 ' +
    esc(chatName) +
    (gf.chat_id ? ' <code>(' + esc(gf.chat_id) + ')</code>' : '') +
    '</span><button onclick="closeGf()">✕ 关闭</button></div>' +
    '<div class="gf-toolbar">' +
    '<input id="gfq" type="text" placeholder="🔍 搜索本群文件（文件名 / 文件ID / 消息ID / 对话ID）" value="' + esc(GF_Q) + '">' +
    '<span class="hint" id="gfhint"></span>' +
    '</div>' +
    '<div class="gf-tabs">' +
    '<span class="gf-tab' + (GF_KIND === "all" ? " active" : "") + '" onclick="setGKind(\\'all\\')">全部 <span class="cnt" id="gk_all">0</span></span>' +
    '<span class="gf-tab' + (GF_KIND === "photo" ? " active" : "") + '" onclick="setGKind(\\'photo\\')">🖼️ 图片 <span class="cnt" id="gk_photo">0</span></span>' +
    '<span class="gf-tab' + (GF_KIND === "video" ? " active" : "") + '" onclick="setGKind(\\'video\\')">🎬 视频 <span class="cnt" id="gk_video">0</span></span>' +
    '<span class="gf-tab' + (GF_KIND === "audio" ? " active" : "") + '" onclick="setGKind(\\'audio\\')">🎵 音乐 <span class="cnt" id="gk_audio">0</span></span>' +
    '<span class="gf-tab' + (GF_KIND === "other" ? " active" : "") + '" onclick="setGKind(\\'other\\')">📁 其它 <span class="cnt" id="gk_other">0</span></span>' +
    '<span class="gf-tab' + (GF_KIND === "invalid" ? " active" : "") + '" onclick="setGKind(\\'invalid\\')">⚠️ 失效 <span class="cnt" id="gk_invalid">0</span></span>' +
    '</div></div>';
  document.getElementById("gk_all").textContent = Number((st.photo || 0) + (st.video || 0) + (st.audio || 0) + (st.other || 0)).toLocaleString();
  document.getElementById("gk_photo").textContent = Number(st.photo || 0).toLocaleString();
  document.getElementById("gk_video").textContent = Number(st.video || 0).toLocaleString();
  document.getElementById("gk_audio").textContent = Number(st.audio || 0).toLocaleString();
  document.getElementById("gk_other").textContent = Number(st.other || 0).toLocaleString();
  document.getElementById("gk_invalid").textContent = Number(st.invalid || 0).toLocaleString();
  // 搜索框监听（每次重建后重新绑定）
  document.getElementById("gfq").addEventListener("input", function () {
    GF_Q = this.value.trim().toLowerCase();
    GF_PAGE = 1;
    loadGroupPage(1);
  });
  // 排序箭头 + 提示
  document.getElementById("gar_size").textContent = GF_SORT.key === "size" ? (GF_SORT.dir > 0 ? "▲" : "▼") : "";
  document.getElementById("gar_dup").textContent = GF_SORT.key === "dup" ? (GF_SORT.dir > 0 ? "▲" : "▼") : "";
  document.getElementById("gfhint").textContent =
    "显示 " + GFFILES.length + " / " + Number(GF_TOTAL || 0).toLocaleString() + " 条";
  var html = "";
  var baseIdx = (GF_PAGE - 1) * GF_PAGE_SIZE;
  GFFILES.forEach(function (f, i) {
    var dupHtml, rowCls = "", idHtml, nameTitle, nameCls, nameContent, sizeHtml, dlHtml;
    if (f.invalid) {
      // 源频道不可用空壳：仅消息ID可识别，无文件名/文件ID/大小，不可下载
      dupHtml = '<span class="badge b-invalid">⚠️ 失效</span>';
      idHtml = '<span class="noid">—</span>';
      rowCls = ' class="inv"';
      nameTitle = '源频道不可用，无文件名（转发自已被移出的频道）';
      nameCls = 'fname invalid';
      nameContent = '[源频道不可用] 已失效，无文件名';
      sizeHtml = '<span class="noid">—</span>';
      dlHtml = '<button class="btn-dl" disabled style="background:#bdc3c7;cursor:not-allowed;">无法下载</button>';
    } else {
      if (f.doc_id) {
        if (f.count > 1) {
          dupHtml = '<span class="badge b-dup" title="点击查看该文件ID的全部重复条目" onclick="filterDup(\\'' + esc(f.doc_id) + '\\')">🔁 重复 ' + f.count + ' 次（第' + cnOrd(f.ord) + '次出现）</span>';
          rowCls = ' class="dup"';
        } else {
          dupHtml = '<span class="badge b-unique">✅ 唯一</span>';
        }
        idHtml = '<code>' + esc(f.doc_id) + '</code>';
      } else {
        dupHtml = '<span class="badge b-noid">⚠️ 无ID</span>';
        idHtml = '<span class="noid">未记录</span>';
      }
      nameTitle = f.name;
      nameCls = 'fname';
      nameContent = esc(f.name);
      sizeHtml = fmtSize(f.size);
      dlHtml = '<button class="btn-dl" onclick="dlFile(' + i + ')" id="dlbtn_' + i + '">⬇️ 下载</button>';
    }
    html += '<tr' + rowCls + '>' +
      '<td>' + (baseIdx + i + 1) + '</td>' +
      '<td class="' + nameCls + '" title="' + esc(nameTitle) + '">' + nameContent + '</td>' +
      '<td>' + idHtml + '</td>' +
      '<td><code>' + esc(f.msg_id) + '</code></td>' +
      '<td>' + sizeHtml + '</td>' +
      '<td>' + dupHtml + '</td>' +
      '<td><code>' + esc(f.chat_id) + '</code></td>' +
      '<td>' + dlHtml + '</td>' +
      '</tr>';
  });
  // 分页：上一页/下一页 + 页码窗口（当前页±3，首尾页+省略号）+ 输入页码跳转（1000+ 页直接跳）
  var totalPages = Math.max(1, Math.ceil(GF_TOTAL / GF_PAGE_SIZE));
  var prevDisabled = GF_PAGE <= 1 ? ' disabled' : '';
  var nextDisabled = GF_PAGE >= totalPages ? ' disabled' : '';
  var win = pageWindow(GF_PAGE, totalPages), winHtml = '';
  for (var w = 0; w < win.length; w++) {
    if (win[w] === '...') { winHtml += '<span class="page-info">…</span>'; continue; }
    var act = (win[w] === GF_PAGE) ? ' disabled' : '';
    var st = (win[w] === GF_PAGE) ? ' style="background:#1f6fae;color:#fff;border-color:#1f6fae;"' : '';
    winHtml += '<button class="btn-page-mini" onclick="loadGroupPage(' + win[w] + ')"' + act + st + '>' + win[w] + '</button>';
  }
  html += '<tr class="pager-row"><td colspan="8">' +
    '<button class="btn-page" onclick="loadGroupPage(' + (GF_PAGE - 1) + ')"' + prevDisabled + '>◀ 上一页</button>' +
    winHtml +
    '<button class="btn-page" onclick="loadGroupPage(' + (GF_PAGE + 1) + ')"' + nextDisabled + '>下一页 ▶</button>' +
    '<span class="page-info">第 ' + GF_PAGE + ' / ' + totalPages + ' 页（共 ' + Number(GF_TOTAL || 0).toLocaleString() + ' 个文件，每页 ' + GF_PAGE_SIZE + '）</span>' +
    '<input id="gfJump" type="number" min="1" max="' + totalPages + '" placeholder="页码" style="width:70px;padding:5px 8px;border:1px solid #ccc;border-radius:6px;font-size:13px;margin:0 8px;">' +
    '<button class="btn-page" onclick="jumpPage()">跳转</button>' +
    '</td></tr>';
  document.getElementById("gftbody").innerHTML = html;
}

// 页码窗口：当前页±3，首尾页 + 省略号（1000+ 页也能直接点跳）
function pageWindow(cur, total) {
  var start = Math.max(1, cur - 3), end = Math.min(total, cur + 3);
  var parts = [];
  if (start > 1) parts.push(1);
  if (start > 2) parts.push('...');
  for (var i = start; i <= end; i++) parts.push(i);
  if (end < total - 1) parts.push('...');
  if (end < total) parts.push(total);
  return parts;
}

function jumpPage() {
  var el = document.getElementById("gfJump");
  if (!el) return;
  var tp = Math.max(1, Math.ceil(GF_TOTAL / GF_PAGE_SIZE));
  var v = parseInt(el.value, 10);
  if (!v || isNaN(v) || v < 1) v = 1;
  if (v > tp) v = tp;
  loadGroupPage(v);
}

var GF_CN = ["", "一", "二", "三", "四", "五", "六", "七", "八", "九", "十"];
function cnOrd(n) {
  n = parseInt(n, 10) || 1;
  return n <= 10 ? GF_CN[n] : String(n);
}

function setGKind(k) {
  GF_KIND = k;
  GF_PAGE = 1;
  loadGroupPage(1);
}

function setGSort(k) {
  if (GF_SORT.key === k) { GF_SORT.dir = -GF_SORT.dir; } else { GF_SORT.key = k; GF_SORT.dir = 1; }
  GF_PAGE = 1;
  loadGroupPage(1);
}

function filterDup(docId) {
  // 点击重复徽章：列出该文件ID的全部重复条目
  GF_Q = String(docId).toLowerCase();
  GF_KIND = "all";
  GF_PAGE = 1;
  loadGroupPage(1);
}

function dlFile(i) {
  var f = GFFILES[i];
  var btn = document.getElementById("dlbtn_" + i);
  btn.disabled = true;
  btn.textContent = "⏳ 排队中";
  fetch("/api/download?chat_id=" + encodeURIComponent(GF_CHAT_ID) +
    "&chat_name=" + encodeURIComponent(f.chat_name || "") +
    "&msg_id=" + encodeURIComponent(f.msg_id) +
    "&file_name=" + encodeURIComponent(f.name) +
    "&document_id=" + encodeURIComponent(f.doc_id || "") +
    "&file_size=" + encodeURIComponent(f.size))
    .then(function (r) { return r.json(); }).then(function (d) {
      if (d.error) { btn.textContent = "❌ " + d.error; return; }
      btn.textContent = "✅ 已排队";
      pollDownload(d.id, btn);
    }).catch(function (e) { btn.textContent = "❌ 失败"; });
}

function pollDownload(reqId, btn) {
  var tries = 0;
  var t = setInterval(function () {
    tries++;
    fetch("/api/requests?id=" + reqId).then(function (r) { return r.json(); }).then(function (d) {
      if (d.status === 2) { clearInterval(t); btn.textContent = "✅ 已入队"; }
      else if (d.status === 3) { clearInterval(t); btn.textContent = "❌ " + (d.result || "失败"); }
      else if (tries > 90) { clearInterval(t); btn.textContent = "⏱️ 超时"; }
    }).catch(function () {});
  }, 2000);
}

function closeGf() {
  document.getElementById("gfWrap").style.display = "none";
}
function showGfError(msg) {
  document.getElementById("gftbody").innerHTML =
    '<tr><td colspan="8" class="empty scan-failed">' + msg + '</td></tr>';
}
function showGfEmpty(msg) {
  document.getElementById("gftbody").innerHTML =
    '<tr><td colspan="8" class="empty">📭 ' + msg + '</td></tr>';
}

// MTProto 登录状态检查：未登录时显示顶部提示横幅（每 5 秒轮询，登录后自动消失）
function checkMtStatus(){
  fetch("/api/status").then(function (r) { return r.json(); }).then(function (d) {
    if (d && d.mtproto === "not_logged_in") {
      document.getElementById("mtStatusBar").style.display = "block";
    } else {
      document.getElementById("mtStatusBar").style.display = "none";
    }
  }).catch(function () {});
}
checkMtStatus();
setInterval(checkMtStatus, 5000);

// 页面初始化：默认群文件查询视图
switchView(2);
</script>
</body>
</html>
"""


def start_server(host=HOST, port=PORT):
    # 独立进程启动：确保两个数据库的表都存在
    try:
        core_init_db()  # jobs / tasks（下载任务视图用，download_tasks.db）
    except Exception as e:
        print(f"⚠️ 初始化下载任务表失败: {e}")
    try:
        look_init_db()  # web_requests / dialogs（download/look.db）
    except Exception as e:
        print(f"⚠️ 初始化 look 表失败: {e}")
    # 群文件扫描缓存 + 群统计表（download/media_cache.db），查询前确保表存在
    try:
        from core.media_cache import init_db as cache_init_db
        cache_init_db()
    except Exception as e:
        print(f"⚠️ 初始化媒体缓存表失败: {e}")

    server = ThreadingHTTPServer((host, port), Handler)
    print(f"📁 OpenBot 群文件查看器已启动: http://127.0.0.1:{port}")
    print("   (Ctrl+C 停止)")
    server.serve_forever()


if __name__ == "__main__":
    start_server()
