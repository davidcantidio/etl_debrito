"""
Production quiet mode utilities to reduce verbose logging.
"""

import logging
import os
from typing import Dict, Set


class ProductionFilter(logging.Filter):
    """Filter to reduce verbose logging in production mode."""
    
    # Patterns to suppress in production
    SUPPRESS_PATTERNS = {
        "Starting new HTTPS connection",
        "https://sheets.googleapis.com",  
        "https://oauth2.googleapis.com",
        "Converted retries value",
        "Making request: POST",
        "urllib3.connectionpool",
        "urllib3.util.retry",
        "google.auth.transport.requests",
        "googleapiclient.discovery_cache",
        "file_cache is only supported",
        # Schema validation details
        "Schema validation found",
        "Empty columns:",
        "Extra columns:",
        # Minor validation warnings
        "Ignorando colunas extras",
        "Colunas do header ausentes",
        # Date normalization debug
        "Normalized.*date inconsistencies",
        "Cross-platform normalization",
        # Connection pool details
        "Connection pool:",
        "Created new pooled HTTP session",
        "Closed pooled HTTP session",
    }
    
    # Loggers to quiet down in production
    QUIET_LOGGERS = {
        "urllib3.connectionpool",
        "urllib3.util.retry", 
        "google.auth.transport.requests",
        "googleapiclient.discovery_cache",
        "googleapiclient.discovery",
        "transform.utils.schema_validator",
        "transform.utils.date_normalizer",
        "transform.utils.connection_pool",
        "load.utils.column_mapper"
    }
    
    def filter(self, record: logging.LogRecord) -> bool:
        """Return True if record should be logged, False to suppress."""
        # Always log errors and warnings
        if record.levelno >= logging.WARNING:
            return True
            
        # Suppress debug messages from quiet loggers
        if record.name in self.QUIET_LOGGERS and record.levelno == logging.DEBUG:
            return False
        
        # Check if message matches suppress patterns
        message = record.getMessage()
        for pattern in self.SUPPRESS_PATTERNS:
            if pattern in message:
                return False
        
        return True


def enable_production_mode(quiet_level: str = "normal") -> None:
    """
    Enable production quiet mode.
    
    Args:
        quiet_level: "normal", "quiet", or "minimal"
    """
    root_logger = logging.getLogger()
    
    # Remove existing production filters first
    for handler in root_logger.handlers:
        handler.filters = [f for f in handler.filters if not isinstance(f, ProductionFilter)]
    
    if quiet_level == "minimal":
        # Only show warnings and errors
        root_logger.setLevel(logging.WARNING)
        
    elif quiet_level == "quiet":
        # Show info and above, but filter verbose messages
        root_logger.setLevel(logging.INFO)
        production_filter = ProductionFilter()
        
        # Add filter to all handlers
        for handler in root_logger.handlers:
            handler.addFilter(production_filter)
            
        # Set specific logger levels
        for logger_name in ProductionFilter.QUIET_LOGGERS:
            logger = logging.getLogger(logger_name)
            logger.setLevel(logging.WARNING)
            
    elif quiet_level == "normal":
        # Default production mode - filter some verbose messages but keep info
        production_filter = ProductionFilter()
        
        # Add filter to all handlers 
        for handler in root_logger.handlers:
            handler.addFilter(production_filter)
            
        # Set some loggers to INFO instead of DEBUG
        debug_to_info_loggers = [
            "urllib3.connectionpool",
            "google.auth.transport.requests", 
            "googleapiclient.discovery_cache"
        ]
        
        for logger_name in debug_to_info_loggers:
            logger = logging.getLogger(logger_name)
            logger.setLevel(logging.INFO)
    
    logging.getLogger(__name__).info(f"🔇 Production mode enabled: {quiet_level}")


def disable_production_mode() -> None:
    """Disable production quiet mode and restore normal logging."""
    root_logger = logging.getLogger()
    
    # Remove production filters
    for handler in root_logger.handlers:
        handler.filters = [f for f in handler.filters if not isinstance(f, ProductionFilter)]
    
    # Reset logger levels to DEBUG
    root_logger.setLevel(logging.DEBUG)
    
    for logger_name in ProductionFilter.QUIET_LOGGERS:
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.DEBUG)
    
    logging.getLogger(__name__).info("🔊 Production mode disabled - full logging restored")


def is_production_environment() -> bool:
    """Check if we're running in production environment."""
    # Check common production environment indicators
    env_indicators = [
        os.getenv("ENVIRONMENT") == "production",
        os.getenv("ENV") == "prod", 
        os.getenv("NODE_ENV") == "production",
        os.getenv("PRODUCTION") == "true",
        # Check if running in a container or cloud environment
        os.path.exists("/.dockerenv"),
        os.getenv("KUBERNETES_SERVICE_HOST") is not None,
        os.getenv("GOOGLE_CLOUD_PROJECT") is not None
    ]
    
    return any(env_indicators)


def auto_enable_production_mode() -> None:
    """Automatically enable production mode if running in production environment."""
    if is_production_environment():
        enable_production_mode("quiet")
        logging.getLogger(__name__).info("🤖 Auto-enabled production mode (detected production environment)")
    else:
        logging.getLogger(__name__).debug("🔧 Development environment detected - keeping full logging")