#openbot\core\plugin_scanner.py
import os
import importlib
import sys
import textwrap
import logging
import inspect
import ast  # 新增：用于静态语法分析
from core.command_registry import get_handlers, clear_handlers, get_plugin_map, get_system_handlers, sync_command_menu
from core.logger import get_plugin_logger

logger = logging.getLogger(__name__)

def verify_syntax(file_path):
    """
    【沙盒第一层：静态语法校验】
    在不运行代码的情况下，检查 Python 文件是否存在语法错误。
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            source = f.read()
        ast.parse(source)
        return True
    except SyntaxError as e:
        logger.error(f"🚫 插件语法错误 [{os.path.basename(file_path)}]: 第 {e.lineno} 行 - {e.msg}")
        return False
    except Exception as e:
        logger.error(f"读取插件文件失败: {e}")
        return False

def load_plugins(manager=None):
    """
    V2.6 工业级注入扫描器
    特性：AST语法沙盒、动态日志分流、热重载支持、依赖缺失识别
    """
    app = manager.bot_app if manager else None
    if app: 
        clear_handlers()
    
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    plugins_dir = os.path.join(project_root, "features")
    results = []

    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    for root, _, files in os.walk(plugins_dir):
        if "__pycache__" in root: continue
        
        for file in files:
            if file.endswith(".py") and not file.startswith("__"):
                full_path = os.path.join(root, file)
                
                # 1. 静态预检
                if not verify_syntax(full_path):
                    results.append({"name": file, "status": "🚫 语法错误"})
                    continue

                try:
                    path_display = os.path.relpath(full_path, plugins_dir).replace(".py", "").replace(os.path.sep, "/")
                    rel_mod_path = os.path.relpath(full_path, project_root).replace(".py", "").replace(os.path.sep, ".")
                    
                    # 2. 尝试导入
                    if rel_mod_path in sys.modules:
                        module = importlib.reload(sys.modules[rel_mod_path])
                    else:
                        module = importlib.import_module(rel_mod_path)
                    
                    # 3. 动态日志注入
                    plugin_logger = get_plugin_logger(rel_mod_path)
                    setattr(module, "logger", plugin_logger)
                    
                    for _, obj in inspect.getmembers(module):
                        if inspect.isclass(obj) and obj.__module__ == rel_mod_path:
                            setattr(obj, "logger", plugin_logger)

                    # 4. 注册与入口校验
                    if hasattr(module, "register"):
                        sig = inspect.signature(module.register)
                        if len(sig.parameters) > 0:
                            module.register(manager)
                        else:
                            module.register()
                        
                        results.append({"name": path_display, "status": "✅ 成功"})
                    else:
                        results.append({"name": path_display, "status": "⚠️ 无入口"})

                except Exception as e:
                    logger.error(f"❌ 运行期异常 [{file}]: {e}", exc_info=True)
                    results.append({"name": path_display, "status": "❌ 崩溃"})

    # 5. 处理器重新挂载（插件 handler + 系统级 handler 一起挂，保证兜底等跨热重载不丢失）
    if app:
        # 清理旧的 handlers
        for group in list(app.handlers.keys()):
            app.handlers[group].clear()
        
        all_h = get_handlers() + get_system_handlers()
        for h in all_h:
            app.add_handler(h)
        logger.info(f"✅ 核心引擎同步完成：共激活 {len(all_h)} 个逻辑单元")

        # 热重载后同步命令菜单（updater 运行中才同步；启动阶段由 launcher 统一同步，避免重复）
        try:
            import asyncio
            if getattr(app, "updater", None) and app.updater.running:
                asyncio.get_running_loop().create_task(sync_command_menu(app))
                logger.info("📋 命令菜单同步已排队（热重载）")
        except Exception:
            pass

    _print_pretty_summary(results)
    return get_handlers()

def _get_visual_length(text):
    return sum(2 if ord(c) > 127 else 1 for c in text)

def _print_pretty_summary(results):
    """严格保持你的 V2.6 极简风格，修复数据解析 Bug"""
    width = 65
    print("\n" + "═" * width)
    print(f"║ {'插件路径 (安全扫描)' :<41} ║ {'加载状态' :<10} ║")
    print("╟" + "─" * 45 + "╫" + "─" * 16 + "╢")
    
    for res in results:
        name = res['name']
        status = res['status']
        padding = 43 - _get_visual_length(name)
        print(f"║ {name}{' ' * max(0, padding)} ║ {status :<9} ║")

    print("╟" + "─" * 63 + "╢")
    
    p_map = get_plugin_map()
    for key_name, data in sorted(p_map.items()):
        # --- 核心适配：判断 data 是字典还是集合 ---
        if isinstance(data, dict):
            # 如果你改了 registry，这里要从字典取 cmds
            actual_cmds = data.get("cmds", set())
            display_name = data.get("alias", key_name.split('.')[-1])
        else:
            # 如果你没改 registry，data 就是原有的 set
            actual_cmds = data
            display_name = key_name.split('.')[-1]
            # 尝试找模块别名
            for m_name, m_obj in sys.modules.items():
                if m_name == key_name or m_name.endswith(key_name):
                    display_name = getattr(m_obj, "__MODULE_NAME__", display_name)
                    break
        
        # 组装指令字符串：由于 registry 里存的是带 / 的，直接 join
        cmd_str = ' '.join(list(actual_cmds)) if actual_cmds else "[监听模式]"
        
        # 💡 这里严格遵循你要求的 line 样式，一个字符都不多加
        line = f"● {display_name}: {cmd_str}"
        
        for w in textwrap.wrap(line, width=60):
            pad = 61 - _get_visual_length(w)
            print(f"║ {w}{' ' * max(0, pad)} ║")
            
    print("═" * width + "\n")