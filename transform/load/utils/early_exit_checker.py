"""
Early exit utilities to skip processing sheets with no new data.
"""

import logging
from typing import Optional, Set
import pandas as pd

from transform.load.dest_writer import _EXISTING_IDS, _infer_data_type, DESTINATION_SHEETS

log = logging.getLogger(__name__)


def check_has_new_data(
    df_raw: pd.DataFrame, 
    sheet_name: str,
    sample_size: int = 100
) -> tuple[bool, dict]:
    """
    Quick check if sheet has new data without full processing.
    
    Args:
        df_raw: Raw DataFrame
        sheet_name: Sheet name 
        sample_size: Number of rows to sample for quick check
        
    Returns:
        (has_new_data: bool, stats: dict)
    """
    if df_raw.empty:
        return False, {"reason": "empty_sheet", "total_rows": 0}
    
    # For sheets that don't write to destination, always process
    data_type = _infer_data_type(sheet_name)
    dest_sheet_name = DESTINATION_SHEETS.get(data_type)
    
    if not dest_sheet_name or dest_sheet_name not in _EXISTING_IDS:
        return True, {"reason": "no_dest_dedup", "total_rows": len(df_raw)}
    
    # Sample the data for quick ID generation and check
    sample_df = df_raw.head(sample_size) if len(df_raw) > sample_size else df_raw
    
    try:
        # Quick ID generation for sample
        from transform.transform.utils.campos_calculados import gerar_id
        
        # Simulate basic column mapping for ID generation
        id_sample = sample_df.apply(gerar_id, axis=1).astype(str).tolist()
        
        existing_ids = _EXISTING_IDS[dest_sheet_name]
        new_ids = [id for id in id_sample if id not in existing_ids]
        
        has_new = len(new_ids) > 0
        
        if not has_new and len(df_raw) > sample_size:
            # If sample shows no new data but sheet is large, do a more thorough check
            log.debug(f"🔍 {sheet_name}: Sample shows no new data, checking full sheet...")
            
            # Check a few more samples from different parts of the sheet
            mid_sample = df_raw.iloc[len(df_raw)//2:len(df_raw)//2 + sample_size//2]
            end_sample = df_raw.tail(sample_size//2)
            
            mid_ids = mid_sample.apply(gerar_id, axis=1).astype(str).tolist() 
            end_ids = end_sample.apply(gerar_id, axis=1).astype(str).tolist()
            
            new_mid = [id for id in mid_ids if id not in existing_ids]
            new_end = [id for id in end_ids if id not in existing_ids]
            
            has_new = len(new_mid) > 0 or len(new_end) > 0
            
        stats = {
            "reason": "has_new_data" if has_new else "all_duplicates",
            "total_rows": len(df_raw),
            "sample_size": len(id_sample),
            "new_in_sample": len(new_ids),
            "existing_ids_total": len(existing_ids)
        }
        
        return has_new, stats
        
    except Exception as e:
        log.debug(f"Early exit check failed for {sheet_name}: {e}")
        # If check fails, assume we have new data to be safe
        return True, {"reason": "check_failed", "error": str(e), "total_rows": len(df_raw)}


def should_skip_sheet(df_raw: pd.DataFrame, sheet_name: str) -> tuple[bool, dict]:
    """
    Determine if a sheet should be skipped entirely.
    
    Returns:
        (should_skip: bool, reason: dict)
    """
    # Never skip Pinterest dimension sheets (they have dependencies)
    pinterest_dims = {"pinterestgenero", "pinterestidade", "pinterestregiao", "pinterestgeral", "pinterestalcance"}
    if sheet_name.lower() in pinterest_dims:
        return False, {"reason": "pinterest_dependency", "total_rows": len(df_raw)}
    
    # Never skip Google Analytics (different write logic)
    if sheet_name.lower().startswith("ga"):
        return False, {"reason": "ga_special_case", "total_rows": len(df_raw)}
    
    has_new, stats = check_has_new_data(df_raw, sheet_name)
    should_skip = not has_new
    
    if should_skip:
        log.info(f"⏭️ {sheet_name}: Skipping - no new data detected "
                f"({stats['total_rows']} rows, {stats.get('new_in_sample', 0)} new in sample)")
    
    return should_skip, stats