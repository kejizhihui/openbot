# openbot\core\utils.py
import re
import logging

logger = logging.getLogger(__name__)

def is_valid_phone(phone: str) -> bool:
    """校验手机号格式（E.164 标准）"""
    pattern = r"^\+[1-9]\d{1,14}$"
    return re.match(pattern, phone) is not None

def is_admin(user_id: int, config) -> bool:
    """
    核心权限校验：判断用户是否为管理员
    支持单个 ADMIN_ID 和逗号分隔的 ADMIN_LIST（如 123,456,789）
    """
    user_id_str = str(user_id).strip()

    # 1. 检查单个 ADMIN_ID
    admin_id = config.get("ADMIN_ID")
    if admin_id and str(admin_id).strip() == user_id_str:
        return True

    # 2. 检查 ADMIN_LIST（逗号分隔的多个 ID）
    admin_list_str = config.get("ADMIN_LIST")
    if admin_list_str:
        admin_list = [x.strip() for x in str(admin_list_str).split(",") if x.strip()]
        if user_id_str in admin_list:
            return True

    # 都不匹配
    if not admin_id and not admin_list_str:
        logger.warning(f"⚠️ 未配置 ADMIN_ID/ADMIN_LIST，拦截来自 {user_id} 的访问")
    return False
