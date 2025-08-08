"""
Dynamic column mapping utilities to reduce 'colunas extras' warnings.
"""

import logging
from typing import Set, List, Dict, Tuple
import pandas as pd

log = logging.getLogger(__name__)

# Common columns that should be kept for origin write-back
ORIGIN_WRITE_BACK_COLUMNS = {
    "date", "campaign_name", "ad_group_name", "ad_name", 
    "impressions", "link_clicks", "clicks", "cost", 
    "video_watched_100", "saves", "pin_clicks",
    "utm_content", "preview_link_fb", "preview_link_ig",
    "ad_preview_link", "preview_link", "pin_id",
    "post_comments", "post_reactions", "post_shares",
    "campaign_lifetime_budget", "campaign_daily_budget",
    "URL_do_Anuncio", "sessions", "users", "bounce_rate",
    "source", "medium", "campaign"
}

# Columns that are generated during processing and should be excluded from origin write-back
GENERATED_COLUMNS = {
    "Veiculo", "ID_Veiculo", "Campanha", "ID_Campanha",
    "Engajamento_Total", "ID", "URL_do_Anuncio_Generated"
}


def get_optimal_columns_for_origin(
    df_raw: pd.DataFrame,
    df_ok: pd.DataFrame,
    sheet_name: str
) -> Tuple[List[str], Set[str], Set[str]]:
    """
    Determine optimal columns for origin write-back to minimize warnings.
    
    Returns:
        - optimal_columns: List of columns to include in write-back
        - filtered_extras: Set of extra columns that were filtered out (won't warn)
        - remaining_extras: Set of extra columns that will still warn
    """
    original_cols = set(df_raw.columns)
    processed_cols = set(df_ok.columns)
    
    # Start with columns that exist in both raw and processed
    common_cols = original_cols.intersection(processed_cols)
    
    # Add any missing raw columns that should be preserved
    missing_from_processed = original_cols - processed_cols
    for col in missing_from_processed:
        if col.lower() in {c.lower() for c in ORIGIN_WRITE_BACK_COLUMNS}:
            common_cols.add(col)
    
    # Filter out generated columns
    filtered_extras = processed_cols.intersection(GENERATED_COLUMNS)
    
    # Determine what extra columns remain
    remaining_extras = processed_cols - original_cols - filtered_extras
    
    # Final optimal columns: prioritize raw columns order
    optimal_columns = []
    for col in df_raw.columns:
        if col in processed_cols and col not in filtered_extras:
            optimal_columns.append(col)
    
    # Add any additional processed columns that aren't generated
    for col in processed_cols:
        if col not in optimal_columns and col not in filtered_extras:
            optimal_columns.append(col)
    
    log.debug(f"[ColumnMapper] {sheet_name}: {len(optimal_columns)} optimal, "
              f"{len(filtered_extras)} filtered, {len(remaining_extras)} extra")
    
    return optimal_columns, filtered_extras, remaining_extras


def get_optimal_columns_for_dest(
    df_model: pd.DataFrame,
    sheet_name: str,
    dest_sheet_name: str
) -> Tuple[List[str], Set[str]]:
    """
    Determine optimal columns for destination write-back.
    
    Returns:
        - optimal_columns: List of columns to include
        - filtered_extras: Set of columns that were filtered out
    """
    all_cols = set(df_model.columns)
    
    # For destination, we generally want to keep all model columns
    # But we can filter out some debug/intermediate columns
    debug_columns = {
        col for col in all_cols 
        if col.lower().startswith('debug_') or 
           col.lower().startswith('temp_') or
           col.lower().endswith('_debug')
    }
    
    optimal_columns = [col for col in df_model.columns if col not in debug_columns]
    
    log.debug(f"[ColumnMapper] {dest_sheet_name}: {len(optimal_columns)} optimal, "
              f"{len(debug_columns)} debug filtered")
    
    return optimal_columns, debug_columns


def apply_smart_column_mapping(
    df_raw: pd.DataFrame,
    df_ok: pd.DataFrame,
    sheet_name: str,
    for_destination: bool = False
) -> pd.DataFrame:
    """
    Apply smart column mapping to reduce warnings.
    
    Args:
        df_raw: Original raw DataFrame
        df_ok: Processed DataFrame 
        sheet_name: Sheet name for logging
        for_destination: Whether this is for destination write-back
        
    Returns:
        DataFrame with optimally mapped columns
    """
    if for_destination:
        # For destination, use df_ok as the model DataFrame
        optimal_cols, filtered = get_optimal_columns_for_dest(df_ok, sheet_name, sheet_name)
        
        if filtered:
            log.debug(f"[SmartMapping] {sheet_name} dest: filtered {len(filtered)} debug columns")
            
    else:
        # For origin write-back
        optimal_cols, filtered_extras, remaining_extras = get_optimal_columns_for_origin(
            df_raw, df_ok, sheet_name
        )
        
        if filtered_extras:
            log.debug(f"[SmartMapping] {sheet_name} origin: intelligently filtered "
                      f"{len(filtered_extras)} generated columns: {sorted(list(filtered_extras)[:5])}")
        
        if remaining_extras and len(remaining_extras) <= 3:
            log.debug(f"[SmartMapping] {sheet_name} origin: remaining extras: {sorted(remaining_extras)}")
    
    # Return DataFrame with optimal columns
    available_cols = [col for col in optimal_cols if col in df_ok.columns]
    return df_ok[available_cols].copy()