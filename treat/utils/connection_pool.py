"""
HTTP connection pooling utilities for Google APIs.
Reduces redundant connection establishment overhead.
"""

import logging
import threading
from typing import Optional, Dict
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from google.auth.transport.requests import Request
import google.auth.transport.requests

log = logging.getLogger(__name__)


class PooledSession:
    """Thread-safe singleton session with connection pooling for Google APIs."""
    
    _instance: Optional['PooledSession'] = None
    _session: Optional[requests.Session] = None
    _lock = threading.Lock()
    
    def __new__(cls) -> 'PooledSession':
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
        return cls._instance
    
    def get_session(self) -> requests.Session:
        """Get or create a pooled session in thread-safe manner."""
        if self._session is None:
            with self._lock:
                # Double-check locking pattern
                if self._session is None:
                    try:
                        self._session = self._create_session()
                        log.debug("🔗 Created new pooled HTTP session for Google APIs")
                    except Exception as e:
                        # 🚨 SAFETY: Never let session creation crash the pipeline
                        log.warning(f"Failed to create pooled session, using basic session: {e}")
                        self._session = requests.Session()
        return self._session
    
    def _create_session(self) -> requests.Session:
        """Create session with optimized connection pooling."""
        session = requests.Session()
        
        # Retry strategy for transient failures
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE", "POST"]
        )
        
        # HTTP adapter with connection pooling
        adapter = HTTPAdapter(
            pool_connections=2,  # Number of pools to cache (googleapis.com, oauth2.googleapis.com)
            pool_maxsize=10,     # Max connections per pool
            max_retries=retry_strategy,
            pool_block=False
        )
        
        # Mount adapter for Google domains
        session.mount("https://sheets.googleapis.com", adapter)
        session.mount("https://oauth2.googleapis.com", adapter)
        session.mount("https://googleapis.com", adapter)
        
        # Set timeouts
        session.timeout = 30
        
        return session
    
    def close(self):
        """Close the pooled session."""
        if self._session:
            self._session.close()
            self._session = None
            log.debug("🔗 Closed pooled HTTP session")


# Safe monkey patch approach - avoid threading issues
_original_request_init = google.auth.transport.requests.Request.__init__
_patched = False

def _safe_patched_request_init(self, session=None):
    """Thread-safe patched Request init to use pooled session."""
    try:
        if session is None:
            # Get pooled session in a thread-safe way
            pooled_instance = PooledSession()
            session = pooled_instance.get_session()
        _original_request_init(self, session)
    except Exception as e:
        # Fallback to original behavior if pooling fails
        log.debug(f"Connection pooling fallback: {e}")
        _original_request_init(self, session)

def apply_connection_pooling():
    """Safely apply connection pooling patch."""
    global _patched
    if not _patched:
        google.auth.transport.requests.Request.__init__ = _safe_patched_request_init
        _patched = True
        log.debug("🔗 Connection pooling patch applied safely")

def remove_connection_pooling():
    """Remove connection pooling patch."""
    global _patched
    if _patched:
        google.auth.transport.requests.Request.__init__ = _original_request_init
        _patched = False
        log.debug("🔗 Connection pooling patch removed")


def get_pooled_session() -> requests.Session:
    """Get the global pooled session instance."""
    return PooledSession().get_session()


def close_connections():
    """Close all pooled connections."""
    PooledSession().close()


# Connection pool statistics
def get_connection_stats() -> Dict[str, int]:
    """Get connection pool statistics (DISABLED to prevent threading issues)."""
    # 🚨 DISABLED: The original implementation caused "unlikely to be threadsafe" errors
    # These lines caused the pipeline to crash:
    #   stats["pools"] += len(adapter.poolmanager.pools)  <-- THREADING ERROR
    #   for pool in adapter.poolmanager.pools.values():   <-- THREADING ERROR
    
    # Return safe dummy stats instead
    return {"pools": "disabled", "connections": "disabled"}