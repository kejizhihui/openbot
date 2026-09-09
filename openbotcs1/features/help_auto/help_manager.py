import logging
import time
import asyncio
from pathlib import Path
from telegram import Update
from telegram.ext import ContextTypes, CommandHandler
from core.command_registry import register_handler

logger = logging.getLogger(__name__)

# 插件名称
__MODULE_NAME__ = "帮助中心"

async def handle_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """【指令 /help】自动扫描全量插件目录下的 help.txt 并聚合"""
    
    # 1. 精准定位 features 目录
    # resolve() 获取绝对路径，parent.parent 回退到 features 根目录
    features_dir = Path(__file__).resolve().parent.parent
    
    help_parts = [
        "📖 <b>OpenBot 系统指令手册</b>",
        "━━━━━━━━━━━━━━━━━━"
    ]

    found_content = False

    try:
        # 2. 获取所有子文件夹并进行优先级排序
        if not features_dir.exists():
            raise FileNotFoundError(f"目录不存在: {features_dir}")
            
        all_dirs = [d for d in features_dir.iterdir() if d.is_dir()]
        
        def sort_logic(path_obj):
            name = path_obj.name.lower()
            if name == "basic": return (0, name)    # 最优先
            if name == "custom": return (2, name)   # 最后
            return (1, name)                        # 其他按字母排
            
        sorted_dirs = sorted(all_dirs, key=sort_logic)

        # 3. 扫描每个模块的 help.txt
        for folder in sorted_dirs:
            txt_path = folder / "help.txt"
            
            if txt_path.exists():
                try:
                    # 增加 errors='ignore' 防止编码异常
                    content = txt_path.read_text(encoding="utf-8", errors="ignore").strip()
                    if content:
                        help_parts.append(f"📦 <b>{folder.name.upper()} 模块</b>")
                        help_parts.append(content)
                        help_parts.append("──────────────────")
                        found_content = True
                except Exception as e:
                    logger.error(f"读取文档 {txt_path} 异常: {e}")

    except Exception as e:
        logger.error(f"扫描帮助目录失败: {e}")
        await update.effective_message.reply_text(f"❌ 目录扫描异常: {e}")
        return

    # 4. 最终渲染
    if not found_content:
        await update.effective_message.reply_text(
            f"📂 <b>扫描完成</b>\n未发现任何有效的 help.txt", 
            parse_mode="HTML"
        )
        return

    help_parts.append(f"🕒 <i>数据更新时间：{time.strftime('%H:%M:%S')}</i>")

    # 拼装最终消息内容（超过 4096 字符自动分条发送，避免 Telegram 超长报错）
    MAX_LEN = 4000  # 留一点余量给 HTML 标签
    chunks = []
    current_chunk = []
    current_len = 0

    for part in help_parts:
        part_len = len(part) + 1  # +1 for newline
        if current_len + part_len > MAX_LEN and current_chunk:
            chunks.append("\n".join(current_chunk))
            current_chunk = [part]
            current_len = part_len
        else:
            current_chunk.append(part)
            current_len += part_len

    if current_chunk:
        chunks.append("\n".join(current_chunk))

    for i, chunk in enumerate(chunks):
        try:
            # 先按 HTML 发送，保留官方 help.txt 里精心写的格式
            await update.effective_message.reply_text(chunk, parse_mode="HTML")
        except Exception:
            # 若内容含非法 HTML 标签解析失败，自动降级为纯文本发送，保证不崩
            await update.effective_message.reply_text(chunk)
        if i < len(chunks) - 1:
            await asyncio.sleep(0.5)  # 避免触发频率限制

# ===================== 统一注册入口 =====================

def register(manager):
    """
    符合 ClientManager 调用的统一注册入口
    """
    register_handler(CommandHandler("help", handle_help), __name__)
    # 修复处的 logger 移入函数内或完全顶格
    logger.info(f"✅ [{__MODULE_NAME__}] V1.0 已就绪")