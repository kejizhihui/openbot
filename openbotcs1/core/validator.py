#openbot\core\validator.py
import logging

logger = logging.getLogger(__name__)

class ConfigValidator:
    def __init__(self, config):
        self.config = config

    def validate_all(self):
        """校验所有必要配置项，返回 (是否通过, 错误明细列表)。
        错误明细为 [{"key": 配置键, "reason": 原因}, ...]（键级，供启动流程汇总并重置 .env）。"""
        errors = []
        errors.extend(self._validate_bot_token())
        errors.extend(self._validate_api_id_hash())
        errors.extend(self._validate_admin_id())
        return (len(errors) == 0, errors)

    def _validate_bot_token(self):
        errors = []
        token = self.config.get("BOT_TOKEN")
        if not token:
            logger.error("❌ 配置错误: 未配置 BOT_TOKEN")
            errors.append({"key": "BOT_TOKEN", "reason": "未配置"})
        elif ":" not in token:
            logger.error("❌ 配置错误: BOT_TOKEN 格式不正确（应为 数字ID:密钥 形式）")
            errors.append({"key": "BOT_TOKEN", "reason": "格式不正确（应为 数字ID:密钥 形式）"})
        return errors

    def _validate_api_id_hash(self):
        errors = []
        api_id = self.config.get("API_ID")
        api_hash = self.config.get("API_HASH")
        if not api_id:
            logger.error("❌ 配置错误: 未配置 API_ID")
            errors.append({"key": "API_ID", "reason": "未配置"})
        elif not api_id.isdigit():
            logger.error("❌ 配置错误: API_ID 格式不正确（必须是纯数字）")
            errors.append({"key": "API_ID", "reason": "格式不正确（必须是纯数字）"})
        if not api_hash:
            logger.error("❌ 配置错误: 未配置 API_HASH")
            errors.append({"key": "API_HASH", "reason": "未配置"})
        elif len(api_hash) != 32:
            logger.error("❌ 配置错误: API_HASH 格式不正确（应为 32 位字符串）")
            errors.append({"key": "API_HASH", "reason": "格式不正确（应为 32 位字符串）"})
        return errors

    def _validate_admin_id(self):
        """确保配置了管理员（ADMIN_ID 或 ADMIN_LIST 至少其一），否则管理功能形同虚设"""
        errors = []
        admin_id = self.config.get("ADMIN_ID")
        admin_list = self.config.get("ADMIN_LIST")
        if not admin_id and not admin_list:
            logger.error("❌ 配置错误: 未配置 ADMIN_ID 或 ADMIN_LIST，系统必须至少一个管理员，启动终止")
            errors.append({"key": "ADMIN_ID/ADMIN_LIST", "reason": "至少需要一个管理员"})
        return errors
