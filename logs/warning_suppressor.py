"""
Utilities to suppress common warnings in production environments.
"""

import warnings
import logging
import os


def suppress_common_warnings():
    """Suppress common warnings that don't affect functionality."""
    
    # Suppress TqdmWarning about IProgress not found
    warnings.filterwarnings("ignore", category=UserWarning, module="tqdm")
    warnings.filterwarnings("ignore", message=".*IProgress not found.*")
    
    # Suppress other common warnings
    warnings.filterwarnings("ignore", message=".*urllib3.*")
    warnings.filterwarnings("ignore", message=".*requests.*")
    warnings.filterwarnings("ignore", category=DeprecationWarning, module="google.*")
    
    logging.getLogger("py.warnings").setLevel(logging.ERROR)
    
    # Set specific loggers to reduce noise
    logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
    logging.getLogger("urllib3.util.retry").setLevel(logging.WARNING)


def setup_clean_tqdm():
    """Setup tqdm with clean imports and no warnings."""
    try:
        # Try to import specific tqdm version that won't warn
        from tqdm import tqdm as _tqdm
        
        # Create a clean wrapper
        def clean_tqdm(*args, **kwargs):
            # Suppress the specific warning during tqdm usage
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", message=".*IProgress not found.*")
                warnings.filterwarnings("ignore", category=UserWarning, module="tqdm")
                return _tqdm(*args, **kwargs)
        
        return clean_tqdm
        
    except ImportError:
        # Fallback to a simple progress indicator
        def fallback_tqdm(iterable, desc=None, **kwargs):
            total = len(iterable) if hasattr(iterable, '__len__') else None
            if desc and total:
                print(f"{desc}: processing {total} items...")
            for item in iterable:
                yield item
            if desc:
                print(f"{desc}: completed")
                
        return fallback_tqdm


def apply_warning_suppressions():
    """Apply all warning suppressions for cleaner logs."""
    suppress_common_warnings()
    
    # Log that suppressions are active
    logger = logging.getLogger(__name__)
    logger.debug("🔇 Warning suppressions applied for cleaner logs")


# Automatically apply suppressions when imported in production-like environments
if os.getenv("ETL_SUPPRESS_WARNINGS", "").lower() in ["true", "1", "yes"]:
    apply_warning_suppressions()