"""
Metadata optimization utilities to reduce redundant spreadsheet metadata calls.
"""

import logging
from typing import Dict, List, Optional, Any
import time
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

log = logging.getLogger(__name__)

# Global metadata cache
_METADATA_CACHE: Dict[str, tuple[float, Dict]] = {}
_CACHE_TTL = 300  # 5 minutes


def get_consolidated_metadata(
    spreadsheet_id: str,
    creds_path: str,
    include_sheets: bool = True,
    include_properties: bool = True
) -> Dict[str, Any]:
    """
    Get all spreadsheet metadata in a single API call.
    
    Returns:
        {
            'title': str,
            'sheets': List[Dict], # if include_sheets
            'properties': Dict,   # if include_properties  
        }
    """
    cache_key = f"{spreadsheet_id}_metadata"
    now = time.time()
    
    # Check cache first
    if cache_key in _METADATA_CACHE:
        cached_time, cached_data = _METADATA_CACHE[cache_key]
        if now - cached_time < _CACHE_TTL:
            log.debug("📥 Metadata cache hit - avoided 1+ API calls")
            return cached_data
    
    # Make single consolidated API call
    try:
        from transform.transform.utils.google_service_pool import get_sheets_readonly_service
        service = get_sheets_readonly_service(creds_path)
        
        # Single API call with all needed metadata
        fields = ["properties.title"]
        if include_sheets:
            fields.append("sheets(properties(sheetId,title,gridProperties))")
        if include_properties:
            fields.append("properties")
            
        result = service.spreadsheets().get(
            spreadsheetId=spreadsheet_id,
            fields=",".join(fields)
        ).execute()
        
        # Parse response
        metadata = {
            'title': result.get('properties', {}).get('title', ''),
            'properties': result.get('properties', {}) if include_properties else {},
            'sheets': []
        }
        
        if include_sheets:
            for sheet in result.get('sheets', []):
                props = sheet.get('properties', {})
                grid_props = props.get('gridProperties', {})
                metadata['sheets'].append({
                    'sheetId': props.get('sheetId'),
                    'title': props.get('title'),
                    'row_count': grid_props.get('rowCount', 1000),
                    'col_count': grid_props.get('columnCount', 26)
                })
        
        # Cache the result
        _METADATA_CACHE[cache_key] = (now, metadata)
        log.debug("📡 Consolidated metadata fetched and cached")
        
        return metadata
        
    except Exception as e:
        log.error(f"Failed to fetch consolidated metadata: {e}")
        # Return minimal fallback
        return {'title': 'Unknown', 'sheets': [], 'properties': {}}


def get_sheet_statistics_optimized(
    spreadsheet_id: str,
    creds_path: str,
    top_n: int = 10
) -> List[Dict]:
    """
    Get sheet statistics using consolidated metadata (1 API call instead of 2+).
    """
    metadata = get_consolidated_metadata(spreadsheet_id, creds_path)
    
    stats = []
    for sheet_info in metadata.get('sheets', []):
        cells = sheet_info['row_count'] * sheet_info['col_count']
        stats.append({
            'title': sheet_info['title'],
            'cells': cells,
            'rows': sheet_info['row_count'],
            'cols': sheet_info['col_count']
        })
    
    # Sort by cell count descending
    stats.sort(key=lambda x: x['cells'], reverse=True)
    return stats[:top_n]


def invalidate_metadata_cache(spreadsheet_id: str) -> None:
    """Invalidate metadata cache for a specific spreadsheet."""
    cache_key = f"{spreadsheet_id}_metadata"
    _METADATA_CACHE.pop(cache_key, None)
    log.debug("🗑️ Metadata cache invalidated")


def get_cache_stats() -> Dict[str, int]:
    """Get metadata cache statistics."""
    return {
        'cached_spreadsheets': len(_METADATA_CACHE),
        'total_cache_size': sum(len(str(data)) for _, data in _METADATA_CACHE.values())
    }