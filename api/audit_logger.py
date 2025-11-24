# api/audit_logger.py
"""
Audit logging utilities cho notification preferences
"""
import logging
from typing import Dict, Any, Optional
from django.db import transaction
from .models import PreferenceAuditLog, User, Location

logger = logging.getLogger(__name__)


class PreferenceAuditLogger:
    """
    Class để log các thay đổi notification preferences
    """
    
    @staticmethod
    def log_preference_change(
        user_id: int,
        field_name: str,
        old_value: Any,
        new_value: Any,
        preference_type: str = 'global',
        location_id: Optional[int] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None
    ) -> Optional[PreferenceAuditLog]:
        """
        Log một thay đổi preference
        
        Args:
            user_id: ID của user
            field_name: Tên field được thay đổi
            old_value: Giá trị cũ
            new_value: Giá trị mới
            preference_type: Loại preference ('global' hoặc 'location')
            location_id: ID của location (nếu là location preference)
            ip_address: IP address của request
            user_agent: User agent của request
            
        Returns:
            PreferenceAuditLog object hoặc None nếu có lỗi
        """
        try:
            # Chỉ log nếu giá trị thực sự thay đổi
            if old_value == new_value:
                return None
            
            with transaction.atomic():
                # Lấy user object
                try:
                    user = User.objects.get(user_id=user_id)
                except User.DoesNotExist:
                    logger.error(f"[AUDIT LOG] User {user_id} not found")
                    return None
                
                # Lấy location object nếu có
                location = None
                if location_id:
                    try:
                        location = Location.objects.get(location_id=location_id)
                    except Location.DoesNotExist:
                        logger.warning(f"[AUDIT LOG] Location {location_id} not found")
                
                # Tạo audit log entry
                audit_log = PreferenceAuditLog.objects.create(
                    user=user,
                    preference_type=preference_type,
                    location=location,
                    field_name=field_name,
                    old_value=old_value,
                    new_value=new_value,
                    ip_address=ip_address,
                    user_agent=user_agent
                )
                
                logger.info(
                    f"[AUDIT LOG] User {user_id} changed {field_name} "
                    f"from {old_value} to {new_value} "
                    f"(type: {preference_type}, location: {location_id})"
                )
                
                return audit_log
                
        except Exception as e:
            logger.error(f"[AUDIT LOG] Error logging preference change: {e}", exc_info=True)
            return None
    
    @staticmethod
    def log_multiple_changes(
        user_id: int,
        changes: Dict[str, tuple],
        preference_type: str = 'global',
        location_id: Optional[int] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None
    ) -> int:
        """
        Log nhiều thay đổi preferences cùng lúc
        
        Args:
            user_id: ID của user
            changes: Dict với key là field_name, value là tuple (old_value, new_value)
            preference_type: Loại preference ('global' hoặc 'location')
            location_id: ID của location (nếu là location preference)
            ip_address: IP address của request
            user_agent: User agent của request
            
        Returns:
            Số lượng audit logs được tạo
        """
        count = 0
        for field_name, (old_value, new_value) in changes.items():
            result = PreferenceAuditLogger.log_preference_change(
                user_id=user_id,
                field_name=field_name,
                old_value=old_value,
                new_value=new_value,
                preference_type=preference_type,
                location_id=location_id,
                ip_address=ip_address,
                user_agent=user_agent
            )
            if result:
                count += 1
        
        return count
    
    @staticmethod
    def get_client_ip(request) -> Optional[str]:
        """
        Lấy IP address từ request
        
        Args:
            request: Django request object
            
        Returns:
            IP address string hoặc None
        """
        x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
        if x_forwarded_for:
            ip = x_forwarded_for.split(',')[0]
        else:
            ip = request.META.get('REMOTE_ADDR')
        return ip
    
    @staticmethod
    def get_user_agent(request) -> Optional[str]:
        """
        Lấy User-Agent từ request
        
        Args:
            request: Django request object
            
        Returns:
            User-Agent string hoặc None
        """
        return request.META.get('HTTP_USER_AGENT')
