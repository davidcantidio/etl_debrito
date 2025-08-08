"""
Date normalization utilities for campaign consistency.
"""

import logging
from typing import Dict, List, Optional, Tuple
import pandas as pd
from datetime import datetime, date

log = logging.getLogger(__name__)


def normalize_campaign_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize campaign dates to ensure consistency.
    
    Rules:
    1. If multiple start dates for same campaign/vehicle, use the earliest
    2. If multiple end dates for same campaign/vehicle, use the latest
    3. Ensure end date is always >= start date
    4. Cross-platform consistency for same campaigns
    """
    campaign_col = None
    if 'campaign_name' in df.columns:
        campaign_col = 'campaign_name'
    elif 'Campanha' in df.columns:
        campaign_col = 'Campanha'
    
    if not campaign_col:
        return df
    
    # Identify date columns with various naming conventions
    date_cols = []
    start_date_col = None
    end_date_col = None
    
    for col in df.columns:
        col_lower = col.lower()
        if 'start' in col_lower or 'inicio' in col_lower:
            if 'date' in col_lower or 'data' in col_lower:
                date_cols.append(col)
                start_date_col = col
        elif 'end' in col_lower or 'fim' in col_lower:
            if 'date' in col_lower or 'data' in col_lower:
                date_cols.append(col)
                end_date_col = col
    
    if not date_cols:
        return df
    
    # Convert date columns to datetime
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Group by campaign and vehicle (if exists)
    group_cols = [campaign_col]
    vehicle_col = None
    if 'vehicle' in df.columns:
        vehicle_col = 'vehicle'
        group_cols.append(vehicle_col)
    elif 'Veiculo' in df.columns:
        vehicle_col = 'Veiculo'
        group_cols.append(vehicle_col)
    
    # Find inconsistencies and normalize
    inconsistencies = []
    
    for group_keys, group_df in df.groupby(group_cols):
        campaign = group_keys[0] if isinstance(group_keys, tuple) else group_keys
        vehicle = group_keys[1] if isinstance(group_keys, tuple) and len(group_keys) > 1 else 'N/A'
        
        # Normalize start dates
        if start_date_col and start_date_col in df.columns:
            unique_starts = group_df[start_date_col].dropna().unique()
            if len(unique_starts) > 1:
                inconsistencies.append({
                    'campaign': campaign,
                    'vehicle': vehicle,
                    'issue': 'multiple_start_dates',
                    'values': [d.isoformat() if hasattr(d, 'isoformat') else str(d) for d in unique_starts]
                })
                
                # Normalize to earliest date
                earliest = pd.Series(unique_starts).min()
                df.loc[group_df.index, start_date_col] = earliest
        
        # Normalize end dates
        if end_date_col and end_date_col in df.columns:
            unique_ends = group_df[end_date_col].dropna().unique()
            if len(unique_ends) > 1:
                inconsistencies.append({
                    'campaign': campaign,
                    'vehicle': vehicle,
                    'issue': 'multiple_end_dates',
                    'values': [d.isoformat() if hasattr(d, 'isoformat') else str(d) for d in unique_ends]
                })
                
                # Normalize to latest date
                latest = pd.Series(unique_ends).max()
                df.loc[group_df.index, end_date_col] = latest
    
    # Log inconsistencies found and fixed
    if inconsistencies:
        log.info(f"📅 Normalized {len(inconsistencies)} date inconsistencies:")
        for inc in inconsistencies[:5]:  # Show first 5
            log.debug(f"  • {inc['campaign']} ({inc['vehicle']}): {inc['issue']} - {inc['values']}")
    
    # Ensure end >= start
    if start_date_col and end_date_col and start_date_col in df.columns and end_date_col in df.columns:
        mask = df[end_date_col] < df[start_date_col]
        if mask.any():
            count = mask.sum()
            log.warning(f"📅 Fixed {count} cases where end_date < start_date")
            df.loc[mask, end_date_col] = df.loc[mask, start_date_col]
    
    return df


def normalize_dates_across_platforms() -> None:
    """
    Apply cross-platform date normalization using global cache.
    This ensures campaigns have consistent dates across Pinterest, TikTok, etc.
    """
    import builtins
    
    # Get all processed dataframes from global cache
    campaign_dates = {}  # {(campaign, vehicle): {start: set, end: set}}
    
    # Collect date info from all platforms
    for attr_name in dir(builtins):
        if not attr_name.startswith('_') or not attr_name.endswith('_tratado'):
            continue
            
        df = getattr(builtins, attr_name, None)
        if not isinstance(df, pd.DataFrame) or df.empty:
            continue
        
        platform = attr_name.replace('_tratado', '').replace('_', '')
        
        # Extract campaign dates from this platform's data
        campaign_col = 'campaign_name' if 'campaign_name' in df.columns else 'Campanha'
        vehicle_col = 'vehicle' if 'vehicle' in df.columns else 'Veiculo'
        
        if campaign_col not in df.columns:
            continue
            
        for col in df.columns:
            col_lower = col.lower()
            is_start = 'start' in col_lower or 'inicio' in col_lower
            is_end = 'end' in col_lower or 'fim' in col_lower
            is_date = 'date' in col_lower or 'data' in col_lower
            
            if not (is_date and (is_start or is_end)):
                continue
            
            for _, row in df.iterrows():
                campaign = row.get(campaign_col)
                vehicle = row.get(vehicle_col, platform.title())
                date_val = row.get(col)
                
                if pd.isna(campaign) or pd.isna(date_val):
                    continue
                
                key = (campaign, vehicle)
                if key not in campaign_dates:
                    campaign_dates[key] = {'start': set(), 'end': set()}
                
                date_type = 'start' if is_start else 'end'
                campaign_dates[key][date_type].add(pd.to_datetime(date_val))
    
    # Apply normalization rules across platforms
    normalized_dates = {}  # {(campaign, vehicle): {start: date, end: date}}
    
    for (campaign, vehicle), dates in campaign_dates.items():
        start_dates = dates['start']
        end_dates = dates['end']
        
        if len(start_dates) > 1 or len(end_dates) > 1:
            # Normalize: earliest start, latest end
            normalized_start = min(start_dates) if start_dates else None
            normalized_end = max(end_dates) if end_dates else None
            
            if normalized_end and normalized_start and normalized_end < normalized_start:
                normalized_end = normalized_start
                
            normalized_dates[(campaign, vehicle)] = {
                'start': normalized_start,
                'end': normalized_end
            }
            
            log.info(f"📅 Cross-platform normalization: {campaign} ({vehicle}) - "
                    f"start: {normalized_start}, end: {normalized_end}")
    
    # Update global cache with normalized dates
    for attr_name in dir(builtins):
        if not attr_name.startswith('_') or not attr_name.endswith('_tratado'):
            continue
            
        df = getattr(builtins, attr_name, None)
        if not isinstance(df, pd.DataFrame) or df.empty:
            continue
        
        # Apply normalized dates to this platform's data
        campaign_col = 'campaign_name' if 'campaign_name' in df.columns else 'Campanha'
        vehicle_col = 'vehicle' if 'vehicle' in df.columns else 'Veiculo'
        
        if campaign_col not in df.columns:
            continue
        
        # Find date columns and update them
        for col in df.columns:
            col_lower = col.lower()
            is_start = 'start' in col_lower or 'inicio' in col_lower
            is_end = 'end' in col_lower or 'fim' in col_lower
            is_date = 'date' in col_lower or 'data' in col_lower
            
            if not (is_date and (is_start or is_end)):
                continue
            
            date_type = 'start' if is_start else 'end'
            
            for idx, row in df.iterrows():
                campaign = row.get(campaign_col)
                vehicle = row.get(vehicle_col, attr_name.replace('_tratado', '').title())
                
                key = (campaign, vehicle)
                if key in normalized_dates:
                    new_date = normalized_dates[key][date_type]
                    if new_date:
                        df.loc[idx, col] = new_date


def find_date_inconsistencies_across_sheets(
    dataframes: Dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """
    Find date inconsistencies across multiple sheets for the same campaigns.
    
    Returns a DataFrame with inconsistencies found.
    """
    all_campaign_dates = []
    
    for sheet_name, df in dataframes.items():
        if df.empty:
            continue
            
        # Skip if no campaign info
        if 'campaign_name' not in df.columns and 'Campanha' not in df.columns:
            continue
        
        campaign_col = 'campaign_name' if 'campaign_name' in df.columns else 'Campanha'
        vehicle_col = None
        
        if 'vehicle' in df.columns:
            vehicle_col = 'vehicle'
        elif 'Veiculo' in df.columns:
            vehicle_col = 'Veiculo'
        
        # Collect date info
        for col_prefix in ['campaign_start_date', 'campaign_end_date', 'Data_Inicio', 'Data_Fim']:
            if col_prefix in df.columns:
                date_data = df.groupby([campaign_col] + ([vehicle_col] if vehicle_col else []))[col_prefix].agg(
                    lambda x: list(pd.to_datetime(x, errors='coerce').dropna().unique())
                ).reset_index()
                
                date_data['sheet'] = sheet_name
                date_data['date_type'] = 'start' if 'start' in col_prefix or 'Inicio' in col_prefix else 'end'
                date_data.columns = ['campaign', 'vehicle', 'dates', 'sheet', 'date_type'] if vehicle_col else ['campaign', 'dates', 'sheet', 'date_type']
                
                all_campaign_dates.append(date_data)
    
    if not all_campaign_dates:
        return pd.DataFrame()
    
    # Combine all date data
    combined = pd.concat(all_campaign_dates, ignore_index=True)
    
    # Find inconsistencies
    inconsistencies = []
    
    group_cols = ['campaign', 'vehicle', 'date_type'] if 'vehicle' in combined.columns else ['campaign', 'date_type']
    
    for group_keys, group_df in combined.groupby(group_cols):
        all_dates = []
        sheets = []
        
        for _, row in group_df.iterrows():
            all_dates.extend(row['dates'])
            sheets.append(row['sheet'])
        
        unique_dates = list(set(all_dates))
        
        if len(unique_dates) > 1:
            campaign = group_keys[0]
            vehicle = group_keys[1] if len(group_keys) > 2 else 'N/A'
            date_type = group_keys[-1]
            
            inconsistencies.append({
                'campaign': campaign,
                'vehicle': vehicle,
                'date_type': date_type,
                'unique_dates': len(unique_dates),
                'dates': [d.isoformat() if hasattr(d, 'isoformat') else str(d) for d in sorted(unique_dates)],
                'sheets': ','.join(sorted(set(sheets)))
            })
    
    if inconsistencies:
        return pd.DataFrame(inconsistencies)
    else:
        return pd.DataFrame()