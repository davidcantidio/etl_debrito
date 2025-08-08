"""
Smart module reloading system that only reloads when files actually changed.
"""

import os
import sys
import importlib
import logging
from typing import Dict, Set, List, Any
from pathlib import Path
import time

log = logging.getLogger(__name__)

# Global state for tracking file changes
_file_timestamps: Dict[str, float] = {}
_loaded_modules: Set[str] = set()


def get_file_timestamp(module_path: str) -> float:
    """Get modification timestamp of a module file."""
    try:
        return os.path.getmtime(module_path)
    except (OSError, FileNotFoundError):
        return 0.0


def should_reload_module(module: Any) -> bool:
    """Check if module should be reloaded based on file timestamp."""
    if not hasattr(module, '__file__') or not module.__file__:
        return False
        
    module_file = module.__file__
    if module_file.endswith('.pyc'):
        module_file = module_file[:-1]  # .pyc -> .py
    
    if not os.path.exists(module_file):
        return False
    
    current_timestamp = get_file_timestamp(module_file)
    last_timestamp = _file_timestamps.get(module_file, 0)
    
    return current_timestamp > last_timestamp


def smart_reload_modules(modules_to_check: List[Any]) -> Dict[str, str]:
    """Smart reload that only reloads changed modules."""
    reloaded = {}
    skipped = 0
    
    for module in modules_to_check:
        module_name = getattr(module, '__name__', str(module))
        
        try:
            if should_reload_module(module):
                importlib.reload(module)
                if hasattr(module, '__file__'):
                    _file_timestamps[module.__file__] = get_file_timestamp(module.__file__)
                reloaded[module_name] = "reloaded"
                log.debug(f"🔄 Reloaded {module_name}")
            else:
                skipped += 1
                reloaded[module_name] = "skipped"
                
        except Exception as e:
            log.warning(f"⚠️ Failed to reload {module_name}: {e}")
            reloaded[module_name] = f"error: {e}"
    
    if reloaded:
        actual_reloads = sum(1 for status in reloaded.values() if status == "reloaded")
        if actual_reloads > 0:
            log.info(f"🔄 Smart reload: {actual_reloads} changed, {skipped} skipped")
        else:
            log.debug(f"⏭️ Smart reload: no changes detected ({skipped} modules checked)")
    
    return reloaded


def should_enable_hot_reload() -> bool:
    """Determine if hot reload should be enabled based on environment."""
    # Disable in production environments
    production_indicators = [
        os.getenv("ENVIRONMENT") == "production",
        os.getenv("ENV") == "prod", 
        os.getenv("NODE_ENV") == "production",
        os.getenv("PRODUCTION") == "true",
        os.path.exists("/.dockerenv"),
        os.getenv("KUBERNETES_SERVICE_HOST") is not None,
    ]
    
    if any(production_indicators):
        return False
    
    # Disable if explicitly set
    if os.getenv("DISABLE_HOT_RELOAD", "").lower() in ["true", "1", "yes"]:
        return False
    
    # Enable in development by default
    return True


def get_development_modules() -> List[Any]:
    """Get list of development modules that should be hot-reloaded."""
    modules_to_reload = []
    
    # Only import modules that are actually loaded
    for module_name, module in sys.modules.items():
        if not module_name.startswith(('extract.', 'transform.', 'load.')):
            continue
            
        # Skip builtin modules
        if not hasattr(module, '__file__') or not module.__file__:
            continue
            
        # Skip modules from site-packages (third party)
        if 'site-packages' in module.__file__:
            continue
            
        modules_to_reload.append(module)
    
    return modules_to_reload


def conditional_hot_reload() -> None:
    """Conditionally apply hot reload based on environment and file changes."""
    if not should_enable_hot_reload():
        log.debug("🚀 Hot-reload disabled (production mode)")
        return
    
    # Only check modules that are likely to change during development
    modules_to_check = get_development_modules()
    
    if not modules_to_check:
        log.debug("🚀 No development modules found for hot-reload")
        return
    
    # Smart reload only changed modules
    smart_reload_modules(modules_to_check)