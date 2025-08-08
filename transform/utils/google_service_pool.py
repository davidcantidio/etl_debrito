"""
Centralized Google API service pool to prevent multiple connection creation.
"""

import logging
import threading
from typing import Optional, Dict, Any
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from transform.utils.safe_connection_pool import get_safe_pooled_session

log = logging.getLogger(__name__)

# Global service cache
_service_cache: Dict[str, Any] = {}
_cache_lock = threading.Lock()

# Scopes mapping
SCOPE_MAPPINGS = {
    'readonly': ["https://www.googleapis.com/auth/spreadsheets.readonly"],
    'readwrite': ["https://www.googleapis.com/auth/spreadsheets"],
}


def get_cached_service(
    creds_path: str,
    service_type: str = 'sheets',
    version: str = 'v4',
    scope_type: str = 'readwrite'
) -> Any:
    """Get cached Google API service to reuse connections."""
    
    cache_key = f"{creds_path}:{service_type}:{version}:{scope_type}"
    
    with _cache_lock:
        # Return cached service if available
        if cache_key in _service_cache:
            log.debug(f"🔗 Using cached {service_type} service - avoided new connection")
            return _service_cache[cache_key]
        
        try:
            # Create new service 
            scopes = SCOPE_MAPPINGS.get(scope_type, SCOPE_MAPPINGS['readwrite'])
            creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
            
            service = build(
                service_type, 
                version, 
                credentials=creds,
                cache_discovery=False
            )
            
            # Cache the service for reuse
            _service_cache[cache_key] = service
            log.debug(f"🔗 Created and cached new {service_type} service")
            
            return service
            
        except Exception as e:
            log.error(f"Failed to create cached service: {e}")
            raise


def clear_service_cache():
    """Clear all cached services."""
    with _cache_lock:
        _service_cache.clear()
        log.debug("🗑️ Service cache cleared")


def get_service_cache_stats() -> Dict[str, int]:
    """Get service cache statistics."""
    with _cache_lock:
        return {"cached_services": len(_service_cache)}


# Convenience functions for common services
def get_sheets_service(creds_path: str, readonly: bool = False) -> Any:
    """Get Sheets API service with connection pooling."""
    scope_type = 'readonly' if readonly else 'readwrite'
    return get_cached_service(creds_path, 'sheets', 'v4', scope_type)


def get_sheets_readonly_service(creds_path: str) -> Any:
    """Get read-only Sheets API service."""
    return get_sheets_service(creds_path, readonly=True)