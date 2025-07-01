# treat/platforms/pinterest.py
import pandas as pd
from treat.utils.preview_links import build_pinterest_preview_link


def transform_pinterest(df: pd.DataFrame, lookup=None) -> pd.DataFrame:
    """
    Transforms the DataFrame for a Pinterest sheet:
    - Builds `URL_do_Anuncio` from `pin_id`, if empty.
    """
    if "pin_id" in df.columns:
        # ensure the preview column exists
        df["URL_do_Anuncio"] = df.get("URL_do_Anuncio", "")
        # fill only the empty entries
        mask = df["URL_do_Anuncio"].astype(str).str.strip() == ""
        df.loc[mask, "URL_do_Anuncio"] = df.loc[mask, "pin_id"].apply(
            build_pinterest_preview_link
        )
    return df
