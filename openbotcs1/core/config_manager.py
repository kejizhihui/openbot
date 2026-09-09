#openbot\core\config_manager.py
import os
import shutil
import logging
from typing import Optional, Any

logger = logging.getLogger(__name__)

class ConfigManager:
    def __init__(self, env_path: str = ".env"):
        self.env_path = env_path
        # 初始化配置字典（从.env读取）
        self.config = self._load_env()

    def _load_env(self) -> dict:
        """使用 python-dotenv 读取 .env（支持引号、注释、转义等标准格式）"""
        from dotenv import load_dotenv, dotenv_values
        # 如果 .env 文件不存在，创建空文件（只读环境如 Docker :ro 挂载写失败则警告，不崩溃）
        if not os.path.exists(self.env_path):
            try:
                with open(self.env_path, "w", encoding="utf-8") as f:
                    f.write("")
            except OSError as e:
                logger.warning(f"⚠️ 无法创建 {self.env_path}（{e}），请通过环境变量或挂载 .env 提供配置")
            return {}
        # 判断 .env 是否"实际为空"：0 字节 / 只有 BOM / 只有空白（如记事本另存留下的 3 字节 BOM 壳）
        env_empty = False
        if os.path.getsize(self.env_path) == 0:
            env_empty = True
        else:
            try:
                with open(self.env_path, "r", encoding="utf-8-sig") as f:
                    env_empty = f.read().strip() == ""
            except Exception:
                env_empty = True
        # 实际为空：尝试自动写入模板（.env.example）；只读环境写失败则把模板内容打到日志，提示手动填写
        # 只允许 .env.example 作为模板源（无密钥）；含真实密钥的 .env.docker 不参与
        if env_empty:
            if os.path.exists(".env.example"):
                try:
                    with open(".env.example", "r", encoding="utf-8") as f:
                        template = f.read()
                    if template:
                        try:
                            with open(self.env_path, "w", encoding="utf-8") as f:
                                f.write(template)
                            logger.warning(
                                f"⚠️ {self.env_path} 为空，已自动写入模板 .env.example，请填写配置后重启"
                            )
                        except OSError as e:
                            logger.warning(
                                f"⚠️ {self.env_path} 为空且无法自动写入模板（{e}），请手动填写 .env 后重启。模板内容如下：\n{template}"
                            )
                except Exception as e:
                    logger.warning(f"⚠️ {self.env_path} 为空且无法读取模板 .env.example（{e}）")
        # override=True 保持原语义：.env 的配置优先于系统环境变量
        # load_dotenv 负责把键值同步进 os.environ（供 get() 优先读取）
        load_dotenv(self.env_path, override=True)
        # dotenv_values 负责解析（支持引号包裹、# 注释、多行值）
        return dict(dotenv_values(self.env_path))

    def reset_to_template(self) -> bool:
        """配置错误时重置：把现有 .env 备份为 .env.bak，再用 .env.example 模板覆盖 .env。

        只读环境（如 Docker :ro 挂载）写失败时把模板全文打到日志并返回 False。
        返回 True = 已成功重置为模板。"""
        template = ""
        try:
            if os.path.exists(self.env_path) and os.path.getsize(self.env_path) > 0:
                shutil.copy2(self.env_path, self.env_path + ".bak")
                logger.warning(f"⚠️ 旧配置已备份为 {self.env_path}.bak")
            if not os.path.exists(".env.example"):
                logger.error(f"❌ 缺少模板文件 .env.example，无法重置 {self.env_path}")
                return False
            with open(".env.example", "r", encoding="utf-8") as f:
                template = f.read()
            if not template.strip():
                logger.error(f"❌ 模板 .env.example 为空，无法重置 {self.env_path}")
                return False
            with open(self.env_path, "w", encoding="utf-8") as f:
                f.write(template)
            logger.warning(
                f"⚠️ {self.env_path} 配置错误，已自动重置为模板 .env.example（旧配置备份为 {self.env_path}.bak），请填写后重启"
            )
            return True
        except OSError as e:
            logger.warning(
                f"⚠️ 无法重置 {self.env_path}（{e}），请手动填写 .env 后重启。模板内容如下：\n{template}"
            )
            return False

    def get(self, key: str, default: Optional[Any] = None) -> Any:
        """获取配置项（优先系统环境变量，其次.env）"""
        return os.getenv(key, self.config.get(key, default))
    
    def set(self, key: str, value: str) -> None:
        """设置配置项并写入.env（修复换行贴合问题）"""
        self.config[key] = value
        os.environ[key] = value

        lines = []
        if os.path.exists(self.env_path):
            with open(self.env_path, "r", encoding="utf-8") as f:
                lines = f.readlines()
        
        new_lines = []
        key_found = False
        
        for line in lines:
            line_strip = line.strip()
            # 保持空行和注释
            if not line_strip or line_strip.startswith("#"):
                new_lines.append(line)
                continue
            
            # 找到现有的 key 则替换
            if line_strip.startswith(f"{key}="):
                new_lines.append(f"{key}={value}\n")
                key_found = True
            else:
                new_lines.append(line)
        
        # 如果是新 key，执行追加逻辑
        if not key_found:
            # 【核心修复】如果最后一行缺换行符，先补一个
            if new_lines and not new_lines[-1].endswith("\n"):
                new_lines[-1] = new_lines[-1] + "\n"
            
            new_lines.append(f"{key}={value}\n")
        
        # 写入文件
        with open(self.env_path, "w", encoding="utf-8") as f:
            f.writelines(new_lines)
        
        # 只记录 key，不记录 value（防止 token/密码等敏感配置写入日志）
        logger.info(f"Config updated: {key}")
