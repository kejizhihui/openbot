# -*- coding: utf-8 -*-
"""
【插件】/look - 群文件查看器（HTML 表格导出）
- 参数格式与 /dl 下载命令一致：
    /look                 → 全部文件
    /look 链接            → 只看该群/频道
    /look 链接 关键字     → 按文件名关键字筛选
    /look 链接 latest:N   → 只看最近 N 个文件
    /look 链接 min_id:N max_id:N → 按消息ID范围
- 输出 HTML 表格到 download/ 目录，浏览器打开
- 判重口径：document_id 相同 = 内容重复（同一视频/文件只算一份）
- 失效提示：下载失败(status=FAILED)的记录红色标注"可能失效"
"""
import logging
import os
import re
import json
import sqlite3
import html as html_mod
from collections import Counter
from datetime import datetime
from telegram import Update
from telegram.ext import ContextTypes, CommandHandler
from core.command_registry import register_handler
from core.utils import is_admin
from core.database import (
    DB_PATH, TASK_PENDING, TASK_DOWNLOADING, TASK_DONE, TASK_FAILED, format_size, _db_conn
)

logger = logging.getLogger(__name__)

__MODULE_NAME__ = "群文件查看器"

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DOWNLOAD_DIR = os.path.join(BASE_DIR, "download")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# 状态 → (文案, css类)
STATUS_MAP = {
    TASK_PENDING: ("⏳ 待下载", "pending"),
    TASK_DOWNLOADING: ("⬇️ 下载中", "downloading"),
    TASK_DONE: ("✅ 已下载", "done"),
    TASK_FAILED: ("❌ 下载失败·可能失效", "failed"),
}


def _build_html(rows, id_counts, title_info):
    """构建可筛选的交互式 HTML 表格页面
    数据以 JSON 嵌入页面，浏览器端实时渲染：搜索框 / 状态下拉 / 重复下拉 / 来源下拉 / 表头排序
    """
    items = []
    for file_name, doc_id, file_size, status, jid, source, tag, msg_id in rows:
        cnt = id_counts.get(doc_id, 0) if doc_id else 0
        src_txt = f"{source} {tag}".strip() or f"任务#{jid}"
        items.append([file_name or "", doc_id or "", file_size or 0, status, jid, src_txt[:60], msg_id, cnt])
    data_json = json.dumps(items, ensure_ascii=False).replace("</", "<\\/")

    return _HTML_TEMPLATE.replace("__TITLE__", html_mod.escape(title_info)).replace("__DATA__", data_json)


_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<title>OpenBot 群文件清单</title>
<style>
  * { box-sizing: border-box; }
  body { font-family: "Microsoft YaHei", "PingFang SC", sans-serif; margin: 0; background: #f5f6fa; color: #2c3e50; }
  .wrap { max-width: 1400px; margin: 0 auto; padding: 20px; }
  h1 { font-size: 22px; margin: 0 0 4px; }
  .meta { color: #7f8c8d; font-size: 13px; margin-bottom: 12px; }
  /* 筛选工具条 */
  .toolbar { display: flex; gap: 10px; flex-wrap: wrap; align-items: center; background: #fff; padding: 12px 14px; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,.08); margin-bottom: 12px; }
  .toolbar input, .toolbar select { padding: 7px 10px; border: 1px solid #d5dbe3; border-radius: 6px; font-size: 13px; background: #fff; color: #2c3e50; }
  .toolbar input:focus, .toolbar select:focus { outline: none; border-color: #3498db; }
  #q { flex: 1; min-width: 180px; }
  .toolbar label { font-size: 12px; color: #7f8c8d; }
  .clear-btn { background: #ecf0f1; border: 1px solid #d5dbe3; border-radius: 6px; padding: 7px 12px; cursor: pointer; font-size: 13px; color: #2c3e50; }
  .clear-btn:hover { background: #dfe6e9; }
  /* 统计条 */
  .stats { display: flex; gap: 10px; margin-bottom: 12px; flex-wrap: wrap; }
  .stat { background: #fff; padding: 8px 16px; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,.08); display: flex; flex-direction: column; align-items: center; min-width: 84px; }
  .stat span { font-size: 12px; color: #7f8c8d; }
  .stat b { font-size: 19px; }
  .stat.total b { color: #2c3e50; } .stat.unique b { color: #27ae60; } .stat.dup b { color: #e67e22; } .stat.noid b { color: #95a5a6; } .stat.failed b { color: #c0392b; }
  /* 表格 */
  .tbl-wrap { background: #fff; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,.08); overflow: auto; max-height: calc(100vh - 260px); }
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
  .src { color: #7f8c8d; font-size: 12px; max-width: 220px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .badge { display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 12px; }
  .b-dup { background: #e67e22; color: #fff; }
  .b-unique { background: #27ae60; color: #fff; }
  .b-noid { background: #bdc3c7; color: #333; }
  code { background: #f0f0f0; padding: 1px 5px; border-radius: 3px; font-size: 12px; }
  .empty { text-align: center; color: #95a5a6; padding: 40px 0; }
</style>
</head>
<body>
<div class="wrap">
<h1>📁 OpenBot 群文件清单</h1>
<p class="meta">__TITLE__</p>

<div class="toolbar">
  <input id="q" type="text" placeholder="🔍 搜索文件名 / 文件ID..." oninput="render()">
  <label>状态</label>
  <select id="fst" onchange="render()">
    <option value="">全部</option>
    <option value="2">✅ 已下载</option>
    <option value="0">⏳ 待下载</option>
    <option value="1">⬇️ 下载中</option>
    <option value="3">❌ 失败</option>
  </select>
  <label>重复</label>
  <select id="fdup" onchange="render()">
    <option value="">全部</option>
    <option value="dup">🔁 重复</option>
    <option value="unique">✅ 唯一</option>
    <option value="noid">⚠️ 无ID</option>
  </select>
  <label>来源</label>
  <select id="fsrc" onchange="render()"><option value="">全部来源</option></select>
  <button class="clear-btn" onclick="resetAll()">↺ 重置</button>
</div>

<div class="stats" id="stats"></div>

<div class="tbl-wrap">
<table>
<thead><tr>
  <th onclick="setSort('idx')"># <span class="arrow"></span></th>
  <th onclick="setSort('f')">文件名 <span class="arrow"></span></th>
  <th onclick="setSort('id')">文件ID <span class="arrow"></span></th>
  <th onclick="setSort('s')">大小 <span class="arrow"></span></th>
  <th onclick="setSort('cnt')">是否重复 <span class="arrow"></span></th>
  <th onclick="setSort('st')">状态 <span class="arrow"></span></th>
  <th onclick="setSort('src')">来源 <span class="arrow"></span></th>
</tr></thead>
<tbody id="tbody"></tbody>
</table>
</div>
</div>

<script>
var DATA = __DATA__;
var STATUS = {
  0: ["⏳ 待下载", "pending"],
  1: ["⬇️ 下载中", "downloading"],
  2: ["✅ 已下载", "done"],
  3: ["❌ 下载失败·可能失效", "failed"]
};
var sortKey = "idx", sortDir = 1; // idx: 原始顺序

function esc(s) {
  return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}
function fmtSize(b) {
  if (!b) return "0 B";
  var u = ["B", "KB", "MB", "GB", "TB"], i = 0;
  while (b >= 1024 && i < u.length - 1) { b /= 1024; i++; }
  return (b >= 100 ? b.toFixed(0) : b.toFixed(1)) + " " + u[i];
}

// 来源下拉去重
(function () {
  var seen = {}, sel = document.getElementById("fsrc");
  DATA.forEach(function (r) { if (r[5] && !seen[r[5]]) { seen[r[5]] = 1; var o = document.createElement("option"); o.value = r[5]; o.textContent = r[5]; sel.appendChild(o); } });
})();

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
  render();
}

function render() {
  var q = document.getElementById("q").value.trim().toLowerCase();
  var st = document.getElementById("fst").value;
  var dup = document.getElementById("fdup").value;
  var src = document.getElementById("fsrc").value;

  var rows = DATA.filter(function (r) {
    if (q && r[0].toLowerCase().indexOf(q) < 0 && r[1].toLowerCase().indexOf(q) < 0) return false;
    if (st && String(r[3]) !== st) return false;
    if (dup === "dup" && !(r[1] && r[7] > 1)) return false;
    if (dup === "unique" && !(r[1] && r[7] === 1)) return false;
    if (dup === "noid" && r[1]) return false;
    if (src && r[5] !== src) return false;
    return true;
  });

  // 排序
  if (sortKey === "idx") {
    // 原始顺序（保持倒序：新文件在前）
  } else {
    rows.sort(function (a, b) {
      var va, vb;
      if (sortKey === "f") { va = a[0].toLowerCase(); vb = b[0].toLowerCase(); }
      else if (sortKey === "id") { va = a[1]; vb = b[1]; }
      else if (sortKey === "s") { va = a[2]; vb = b[2]; }
      else if (sortKey === "st") { va = a[3]; vb = b[3]; }
      else if (sortKey === "cnt") { va = a[7]; vb = b[7]; }
      else { va = a[5]; vb = b[5]; }
      if (va < vb) return -1 * sortDir;
      if (va > vb) return 1 * sortDir;
      return 0;
    });
  }

  // 统计
  var unique = 0, dupn = 0, noid = 0, failed = 0;
  rows.forEach(function (r) {
    if (r[1]) { if (r[7] > 1) dupn++; else unique++; } else noid++;
    if (r[3] === 3) failed++;
  });
  document.getElementById("stats").innerHTML =
    '<div class="stat total"><span>总记录</span><b>' + rows.length + '</b></div>' +
    '<div class="stat unique"><span>✅ 唯一</span><b>' + unique + '</b></div>' +
    '<div class="stat dup"><span>🔁 重复</span><b>' + dupn + '</b></div>' +
    '<div class="stat noid"><span>⚠️ 无ID</span><b>' + noid + '</b></div>' +
    '<div class="stat failed"><span>❌ 失败</span><b>' + failed + '</b></div>';

  // 表头箭头
  var heads = document.querySelectorAll("th .arrow");
  heads.forEach(function (a) { a.textContent = ""; });
  var idxMap = { idx: 0, f: 1, id: 2, s: 3, cnt: 4, st: 5, src: 6 };
  if (sortKey !== "idx") {
    heads[idxMap[sortKey]].textContent = sortDir > 0 ? "▲" : "▼";
  }

  // 渲染行
  var tbody = document.getElementById("tbody");
  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="7" class="empty">📭 没有匹配的记录，试试调整筛选条件</td></tr>';
    return;
  }
  var html = "";
  rows.forEach(function (r, i) {
    var stInfo = STATUS[r[3]] || ["❓ 未知", "pending"];
    var dupHtml, rowCls = "";
    if (r[1]) {
      dupHtml = r[7] > 1 ? '<span class="badge b-dup">🔁 重复</span>' : '<span class="badge b-unique">✅ 唯一</span>';
      if (r[7] > 1) rowCls = ' class="dup"';
      var idHtml = '<code>' + esc(r[1]) + '</code>';
    } else {
      dupHtml = '<span class="badge b-noid">⚠️ 无ID</span>';
      idHtml = '<span class="noid">未记录</span>';
    }
    html += '<tr' + rowCls + '>' +
      '<td>' + (i + 1) + '</td>' +
      '<td class="fname" title="' + esc(r[0]) + '">' + esc(r[0]) + '</td>' +
      '<td>' + idHtml + '</td>' +
      '<td>' + fmtSize(r[2]) + '</td>' +
      '<td>' + dupHtml + '</td>' +
      '<td class="' + stInfo[1] + '">' + stInfo[0] + '</td>' +
      '<td class="src" title="' + esc(r[5]) + '">' + esc(r[5]) + '</td>' +
      '</tr>';
  });
  tbody.innerHTML = html;
}

render();
</script>
</body>
</html>"""


async def handle_look(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/look [链接] [筛选变量] - 查看群文件清单，导出 HTML 表格到 download/ 目录"""
    # 权限校验：仅管理员
    manager = getattr(handle_look, "manager", None) or context.bot_data.get('manager')
    if not manager or not is_admin(update.effective_user.id, manager.config):
        return

    args = context.args or []

    # 用法提示
    if args and args[0] in ("-h", "--help", "help"):
        return await update.message.reply_text(
            "💡 <b>用法:</b> /look [链接] [筛选变量]\n"
            "━━━━━━━━━━━━━━━\n"
            "  /look               → 全部文件\n"
            "  /look 链接          → 只看该群/频道\n"
            "  /look 链接 关键字   → 按文件名筛选\n"
            "  /look 链接 latest:N → 只看最近 N 个文件\n"
            "  /look 链接 min_id:N max_id:N → 按消息ID范围\n"
            "  筛选可组合: /look 链接 视频 latest:50",
            parse_mode="HTML"
        )

    # 解析参数：第一个参数若是链接 → 来源筛选，其余为筛选变量
    source_kw = None
    filters = []
    if args:
        first = args[0].strip()
        if re.search(r'(t\.me|https?://|@\w+)', first):
            source_kw = first
            filters = args[1:]
        else:
            filters = args

    # 解析筛选变量（延用 /dl 格式：latest:N / min_id:N / max_id:N / 关键字）
    keyword = None
    latest_n = None
    min_id = None
    max_id = None
    for f in filters:
        f = f.strip()
        m = re.match(r'^latest:(\d+)$', f, re.I)
        if m:
            latest_n = int(m.group(1))
            continue
        m = re.match(r'^min_id:(\d+)$', f, re.I)
        if m:
            min_id = int(m.group(1))
            continue
        m = re.match(r'^max_id:(\d+)$', f, re.I)
        if m:
            max_id = int(m.group(1))
            continue
        if re.match(r'^(?:from|to):', f, re.I):
            continue  # 日期类筛选对文件清单无意义，忽略
        keyword = f  # 兜底作为关键字

    # 查询文件记录（join jobs 拿来源）
    sql = """SELECT t.file_name, t.document_id, t.file_size, t.status, t.jid,
                    COALESCE(j.source, ''), COALESCE(j.tag, ''), t.msg_id
             FROM tasks t LEFT JOIN jobs j ON t.jid = j.jid
             WHERE 1=1"""
    params = []
    if source_kw:
        sql += " AND (j.source LIKE ? OR j.tag LIKE ?)"
        like = f"%{source_kw}%"
        params += [like, like]
    if keyword:
        sql += " AND t.file_name LIKE ?"
        params.append(f"%{keyword}%")
    if min_id is not None:
        sql += " AND t.msg_id >= ?"
        params.append(min_id)
    if max_id is not None:
        sql += " AND t.msg_id <= ?"
        params.append(max_id)
    sql += " ORDER BY t.tid DESC"
    if latest_n:
        sql += " LIMIT ?"
        params.append(latest_n)

    try:
        conn = _db_conn()
        rows = conn.execute(sql, params).fetchall()
        conn.close()
    except Exception as e:
        logger.error(f"查询文件记录失败: {e}")
        return await update.message.reply_text(f"❌ 查询失败: {e}")

    if not rows:
        return await update.message.reply_text("📭 没有符合筛选条件的文件记录")

    # 全局判重：document_id 出现次数
    id_counts = Counter(r[1] for r in rows if r[1])

    # 标题信息
    title_parts = [f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"]
    if source_kw:
        title_parts.append(f"筛选来源: {source_kw}")
    if keyword:
        title_parts.append(f"关键字: {keyword}")
    if latest_n:
        title_parts.append(f"最近 {latest_n} 条")
    if min_id is not None or max_id is not None:
        title_parts.append(f"msg_id {min_id or 0}~{max_id or '∞'}")
    title_info = " | ".join(title_parts)

    # 生成 HTML
    html_content = _build_html(rows, id_counts, title_info)

    # 写入 download/ 目录（UTF-8 带 BOM）
    fname = f"look_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
    fpath = os.path.join(DOWNLOAD_DIR, fname)
    try:
        with open(fpath, "w", encoding="utf-8-sig") as f:
            f.write(html_content)
    except Exception as e:
        logger.error(f"写入 HTML 失败: {e}")
        return await update.message.reply_text(f"❌ 写入文件失败: {e}")

    failed_cnt = sum(1 for r in rows if r[3] == TASK_FAILED)
    logger.info(f"📁 群文件清单已导出: {fpath}（{len(rows)} 条，失败 {failed_cnt}）")
    await update.message.reply_text(
        f"📁 <b>群文件清单已生成</b>\n"
        f"━━━━━━━━━━━━━━━\n"
        f"📄 文件名: <code>{fname}</code>\n"
        f"📂 完整路径: <code>{fpath}</code>\n"
        f"📊 记录: {len(rows)} 条"
        + (f"（❌ 失败 {failed_cnt} 条）" if failed_cnt else "")
        + "\n"
        f"━━━━━━━━━━━━━━━\n"
        f"💡 用浏览器打开即可查看表格",
        parse_mode="HTML"
    )


# ===================== 统一注册入口 =====================

def register(manager):
    """符合 ClientManager 调用的统一注册入口"""
    handle_look.manager = manager
    register_handler(CommandHandler("look", handle_look), __name__)

    # 🌐 注册 Web 服务子进程（look_server 独立进程，由 core 统一拉起/看护/停机）
    try:
        from core.child_services import register_web_service
        register_web_service(
            "群文件查看器",
            os.path.join("features", "look", "look_server.py"),
            host="127.0.0.1",
            port=7777,
        )
    except Exception as e:
        logger.warning(f"⚠️ Web 服务注册失败（不影响 /look）: {e}")

    # 🗓️ 对话列表自动同步（7777 Web 查看器显示全部群聊的数据源）
    try:
        from features.look.dialog_sync import register_dialog_sync
        register_dialog_sync(manager)
    except Exception as e:
        logger.warning(f"⚠️ 对话列表同步挂载失败（不影响 /look）: {e}")

    # 🔁 Web 请求处理循环（群文件扫描 / 点击下载中转，每 5 秒轮询）
    try:
        from features.look.group_browse import register_worker
        register_worker(manager)
    except Exception as e:
        logger.warning(f"⚠️ Web 请求处理循环挂载失败（不影响 /look）: {e}")

    logger.info(f"✅ [{__MODULE_NAME__}] V1.0 已就绪")
