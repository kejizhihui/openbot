#openbot\core\command_registry.py
import logging
import sys
from typing import List, Any, Dict

logger = logging.getLogger(__name__)

# --- 全局存储容器 ---
# 使用全局变量确保热重载时数据能被清空和重建
GLOBAL_HANDLERS: List[Any] = []
# 💡 关键修改：PLUGIN_MAP 现在存储结构化字典，不再是简单的 set
PLUGIN_MAP: Dict[str, Dict[str, Any]] = {}
# 系统级持久 handler（跨热重载保留，如无效命令兜底），不受 clear_handlers 影响
SYSTEM_HANDLERS: List[Any] = []

def get_handlers(): return GLOBAL_HANDLERS
def get_plugin_map(): return PLUGIN_MAP
def get_system_handlers(): return SYSTEM_HANDLERS

def clear_handlers():
    global GLOBAL_HANDLERS, PLUGIN_MAP
    GLOBAL_HANDLERS.clear()
    PLUGIN_MAP.clear()
    # 注意：SYSTEM_HANDLERS 故意不清空，系统级 handler 必须跨热重载保留
    logger.debug("🧹 注册器账本已清空（系统级 handler 保留）")

def register_system_handler(handler: Any):
    """
    注册系统级 handler（如无效命令兜底）。
    与 register_handler 的区别：加入 SYSTEM_HANDLERS，clear_handlers() 不会清除，
    热重载(load_plugins) 重挂时会与插件 handler 一起重新挂载到 Bot。
    """
    if handler and handler not in SYSTEM_HANDLERS:
        SYSTEM_HANDLERS.append(handler)
    return handler

def _ensure_plugin_entry(plugin_key: str, module_name: str, cmds_init):
    """
    公共函数：确保 PLUGIN_MAP 中存在该插件的结构化条目。
    统一处理中文别名读取与默认字段初始化，避免多处重复实现。
    """
    if plugin_key not in PLUGIN_MAP:
        module_obj = sys.modules.get(module_name)
        alias = getattr(module_obj, "__MODULE_NAME__", plugin_key.replace('_', ' ').title())
        PLUGIN_MAP[plugin_key] = {
            "alias": alias,               # 中文名称
            "file": f"{plugin_key.replace('.', '/')}.py",  # 相对路径文件名（如 admin/help_manager.py）
            "cmds": cmds_init             # 指令集（集合）
        }
    return PLUGIN_MAP[plugin_key]

def register_handler(handler: Any, module_name: str = None):
    """
    插件注册核心函数
    handler: Bot API 的 Handler 对象
    module_name: 插件的模块路径
    """
    # 1. 存入待加载列表（用于 Bot 挂载）
    if handler and handler not in GLOBAL_HANDLERS:
        GLOBAL_HANDLERS.append(handler)
    
    # 2. 提取插件唯一标识
    # 💡 修复命名碰撞：原用模块名最后一段（如 help_manager），
    #    admin/help_manager 与 help_auto/help_manager 会共用同一 key 导致 UI 条目合并。
    #    现改为去 features. 前缀的完整模块路径（如 admin.help_manager），全局唯一。
    plugin_key = module_name
    if plugin_key and plugin_key.startswith("features."):
        plugin_key = plugin_key[len("features."):]
    if not plugin_key:
        plugin_key = "未分类"
    
    # 3. 初始化结构化字典（公共函数）
    entry = _ensure_plugin_entry(plugin_key, module_name, set())
    
    # 4. 自动提取 / 指令
    if handler:
        cmd_attr = getattr(handler, 'commands', getattr(handler, 'command', None))
        if cmd_attr:
            if isinstance(cmd_attr, (list, tuple, set, frozenset)):
                for c in cmd_attr:
                    entry["cmds"].add(f"/{str(c).lstrip('/')}")
            else:
                entry["cmds"].add(f"/{str(cmd_attr).lstrip('/')}")
        
    return handler


# ===================== 命令菜单同步 =====================
import os as _os
import re as _re

_PROJECT_ROOT = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))


def _build_command_menu():
    """从各插件 help.txt 解析生成命令菜单 [(command, description), ...]（按插件目录排序、去重、截断）"""
    from telegram import BotCommand
    cmd_map = {}  # cmd -> desc，首次出现优先，后续同命令跳过（如 /dl_saved 出现多次）
    help_files = set()
    for key, data in PLUGIN_MAP.items():
        if not isinstance(data, dict):
            continue
        file_rel = data.get("file", "")
        if not file_rel:
            continue
        # data["file"] 形如 "admin/help_manager.py" → help 文件 "features/admin/help.txt"
        d = _os.path.dirname(file_rel)
        if d:
            help_files.add(_os.path.join(_PROJECT_ROOT, "features", d, "help.txt"))
    for hp in sorted(help_files):
        try:
            with open(hp, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    # 支持 "/命令 - 描述" 与 "/命令 参数 - 描述"（只取第一个词作命令名）
                    m = _re.match(r"^/([A-Za-z0-9_]+)\b(?:\s*[^\s-][^-]*?)?\s*-\s*(.+)$", line)
                    if not m:
                        continue
                    cmd, desc = m.group(1), m.group(2).strip()
                    if len(desc) > 256:  # Telegram 描述上限 256 字符
                        desc = desc[:253] + "..."
                    if cmd not in cmd_map:
                        cmd_map[cmd] = desc
        except Exception:
            continue
    return [BotCommand(c, d) for c, d in cmd_map.items()]


async def sync_command_menu(app) -> int:
    """同步命令菜单到 Telegram（启动/热重载后调用）。返回同步的命令数量。"""
    if app is None:
        return 0
    try:
        menu = _build_command_menu()
        if not menu:
            logger.warning("📋 命令菜单为空，跳过同步")
            return 0
        await app.bot.set_my_commands(menu)
        logger.info(f"📋 命令菜单已同步：{len(menu)} 个命令")
        return len(menu)
    except Exception as e:
        logger.warning(f"⚠️ 命令菜单同步失败: {e}")
        return 0
