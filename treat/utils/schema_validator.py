"""
Schema validation utilities for early detection of column mismatches.
"""

import logging
from typing import Dict, List, Set, Optional
import pandas as pd

log = logging.getLogger(__name__)


# Expected columns by sheet type
EXPECTED_SCHEMAS: Dict[str, Set[str]] = {
    # Meta (Facebook/Instagram)
    "meta": {
        "date", "campaign_name", "ad_group_name", "ad_name", 
        "impressions", "link_clicks", "cost", "video_watched_100",
        "post_comments", "post_reactions", "post_shares",
        "campaign_lifetime_budget", "campaign_daily_budget",
        "preview_link_fb", "preview_link_ig", "utm_content"
    },
    
    # TikTok
    "tiktok": {
        "date", "campaign_name", "ad_group_name", "ad_name",
        "impressions", "clicks", "cost", "video_watched_100",
        "ad_preview_link", "URL_do_Anuncio"
    },
    
    # Pinterest
    "pinterest": {
        "date", "campaign_name", "ad_group_name", "ad_name",
        "impressions", "pin_clicks", "cost", "saves",
        "preview_link", "pin_id"
    },
    
    # LinkedIn
    "linkedin": {
        "date", "campaign_name", "impressions", "clicks", 
        "cost", "URL_do_Anuncio", "utm_content"
    },
    
    # Google Analytics
    "ga": {
        "date", "campaign", "source", "medium",
        "sessions", "users", "bounce_rate"
    }
}

# Columns that are generated during processing
GENERATED_COLUMNS = {
    "Veiculo", "ID_Veiculo", "Campanha", "ID_Campanha",
    "Engajamento_Total", "ID", "URL_do_Anuncio"
}


def get_sheet_type(sheet_name: str) -> Optional[str]:
    """Determine sheet type from name."""
    sheet_lower = sheet_name.lower()
    
    if sheet_lower.startswith("meta"):
        return "meta"
    elif sheet_lower.startswith("tiktok"):
        return "tiktok"
    elif sheet_lower.startswith("pinterest"):
        return "pinterest"
    elif sheet_lower.startswith("linkedin"):
        return "linkedin"
    elif sheet_lower.startswith("ga"):
        return "ga"
    
    return None


def validate_schema_early(
    df: pd.DataFrame, 
    sheet_name: str,
    warn_only: bool = True
) -> Dict[str, List[str]]:
    """
    Validate DataFrame schema against expected columns.
    
    Returns dict with:
    - missing_expected: columns expected but not found
    - extra_columns: columns found but not expected
    - empty_columns: columns that exist but are completely empty
    """
    sheet_type = get_sheet_type(sheet_name)
    if not sheet_type:
        return {"missing_expected": [], "extra_columns": [], "empty_columns": []}
    
    expected = EXPECTED_SCHEMAS.get(sheet_type, set())
    actual = set(df.columns)
    
    # Find discrepancies
    missing_expected = list(expected - actual)
    extra_columns = list(actual - expected - GENERATED_COLUMNS)
    
    # Find empty columns
    empty_columns = []
    for col in df.columns:
        if df[col].isna().all() or (df[col] == "").all():
            empty_columns.append(col)
    
    # Log findings
    if missing_expected:
        msg = f"[Schema] {sheet_name}: Missing expected columns: {missing_expected}"
        if warn_only:
            log.warning(msg)
        else:
            log.info(msg)
    
    if extra_columns and not warn_only:
        log.debug(f"[Schema] {sheet_name}: Extra columns: {extra_columns}")
    
    if empty_columns and len(empty_columns) < 10:  # Only log if not too many
        log.debug(f"[Schema] {sheet_name}: Empty columns: {empty_columns}")
    
    return {
        "missing_expected": missing_expected,
        "extra_columns": extra_columns,
        "empty_columns": empty_columns
    }


def should_process_column(col: str, sheet_type: str) -> bool:
    """
    Determine if a column should be processed based on sheet type.
    This helps avoid warnings about missing columns that aren't relevant.
    """
    if sheet_type not in EXPECTED_SCHEMAS:
        return True
    
    expected = EXPECTED_SCHEMAS[sheet_type]
    return col.lower() in {c.lower() for c in expected} or col in GENERATED_COLUMNS