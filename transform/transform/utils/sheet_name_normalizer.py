"""
Sheet name normalization utilities for handling special characters and encoding issues.
"""

import logging
import re
import unicodedata
from urllib.parse import quote, unquote

log = logging.getLogger(__name__)


def normalize_sheet_name(sheet_name: str) -> str:
    """
    Normalize sheet name to handle special characters safely.
    
    Args:
        sheet_name: Original sheet name that may contain special characters
        
    Returns:
        Normalized sheet name safe for API calls
    """
    if not sheet_name:
        return sheet_name
    
    try:
        # First, handle URL encoding issues
        if '%' in sheet_name:
            try:
                sheet_name = unquote(sheet_name)
            except Exception:
                pass  # Keep original if unquote fails
        
        # Normalize unicode characters
        normalized = unicodedata.normalize('NFKC', sheet_name)
        
        # Handle common problematic characters
        replacements = {
            "'": "'",  # Smart apostrophe
            '"': '"',  # Smart quote left
            '"': '"',  # Smart quote right
            '–': '-',  # En dash
            '—': '-',  # Em dash
            '…': '...',  # Ellipsis
        }
        
        for old, new in replacements.items():
            normalized = normalized.replace(old, new)
        
        # Remove or replace other control characters
        normalized = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', normalized)
        
        # Trim whitespace
        normalized = normalized.strip()
        
        if normalized != sheet_name:
            log.debug(f"Sheet name normalized: '{sheet_name}' -> '{normalized}'")
        
        return normalized
        
    except Exception as e:
        log.warning(f"Failed to normalize sheet name '{sheet_name}': {e}")
        return sheet_name


def safe_sheet_range(sheet_name: str, cell_range: str = "A:Z") -> str:
    """
    Create a safe sheet range string for API calls.
    
    Args:
        sheet_name: Sheet name (will be normalized)
        cell_range: Cell range (default A:Z)
        
    Returns:
        Safe range string like "'Normalized Sheet'!A:Z"
    """
    normalized_name = normalize_sheet_name(sheet_name)
    
    # Always quote sheet names for safety
    if "'" not in normalized_name:
        quoted_name = f"'{normalized_name}'"
    else:
        # Handle single quotes in sheet names
        escaped_name = normalized_name.replace("'", "''")
        quoted_name = f"'{escaped_name}'"
    
    return f"{quoted_name}!{cell_range}"


def batch_normalize_sheet_names(sheet_names: list[str]) -> dict[str, str]:
    """
    Batch normalize sheet names and return mapping.
    
    Args:
        sheet_names: List of original sheet names
        
    Returns:
        Dictionary mapping original -> normalized names
    """
    normalized_mapping = {}
    
    for original_name in sheet_names:
        normalized = normalize_sheet_name(original_name)
        normalized_mapping[original_name] = normalized
    
    # Log summary
    changed_count = sum(1 for orig, norm in normalized_mapping.items() if orig != norm)
    if changed_count > 0:
        log.info(f"📝 Normalized {changed_count}/{len(sheet_names)} sheet names with special characters")
    
    return normalized_mapping


def validate_sheet_name_encoding(sheet_name: str) -> bool:
    """
    Validate that a sheet name doesn't have encoding issues.
    
    Args:
        sheet_name: Sheet name to validate
        
    Returns:
        True if name is safe, False if it has encoding issues
    """
    try:
        # Try to encode/decode to detect issues
        encoded = sheet_name.encode('utf-8')
        decoded = encoded.decode('utf-8')
        
        # Check for problematic patterns
        problematic_patterns = [
            r'%[0-9A-Fa-f]{2}',  # URL encoding
            r'\\x[0-9A-Fa-f]{2}',  # Hex escape sequences
            r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]',  # Control characters
        ]
        
        for pattern in problematic_patterns:
            if re.search(pattern, sheet_name):
                return False
        
        return decoded == sheet_name
        
    except (UnicodeError, UnicodeDecodeError, UnicodeEncodeError):
        return False


def get_encoding_safe_names(sheet_names: list[str]) -> tuple[list[str], list[str]]:
    """
    Split sheet names into encoding-safe and problematic ones.
    
    Returns:
        (safe_names, problematic_names)
    """
    safe_names = []
    problematic_names = []
    
    for name in sheet_names:
        if validate_sheet_name_encoding(name):
            safe_names.append(name)
        else:
            problematic_names.append(name)
    
    if problematic_names:
        log.warning(f"Found {len(problematic_names)} sheets with encoding issues: {problematic_names[:3]}...")
    
    return safe_names, problematic_names