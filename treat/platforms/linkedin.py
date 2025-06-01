import pandas as pd
from typing import Any, Dict, Optional

def _init_preview_column(df: pd.DataFrame) -> pd.DataFrame:
    df["URL_do_Anuncio"] = df.get("URL_do_Anuncio", "")
    return df

def _apply_preview_map(df: pd.DataFrame, preview_map: Dict[str, str]) -> pd.DataFrame:
    if preview_map and "utm_content" in df.columns:
        df["URL_do_Anuncio"] = df["URL_do_Anuncio"].where(
            df["URL_do_Anuncio"].str.strip() != "",
            df["utm_content"].astype(str).map(preview_map).fillna(""),
        )
    return df

def _override_ad_name(df: pd.DataFrame, ad_name_map: Dict[str, str]) -> pd.DataFrame:
    if ad_name_map and "utm_content" in df.columns:
        df["ad_name"] = (
            df["utm_content"]
            .astype(str).str.strip()
            .map(ad_name_map)
            .fillna(df.get("ad_name", ""))
        )
    return df

def _sync_ad_group(df: pd.DataFrame) -> pd.DataFrame:
    df["ad_group_name"] = df["ad_name"]
    return df


def transform_linkedin(df: pd.DataFrame, lookup: Optional[Any] = None) -> pd.DataFrame:
    from treat.utils.preview_links import generate_linkedin_ad_preview_link_from_lookup

    df = _init_preview_column(df)

    preview_map = (
        generate_linkedin_ad_preview_link_from_lookup(lookup.df())
        if lookup is not None
        else {}
    )
    df = _apply_preview_map(df, preview_map)

    ad_name_map = lookup.get_linkedin_ad_name_map() if lookup is not None else {}
    df = _override_ad_name(df, ad_name_map)

    df = _sync_ad_group(df)
    return df
