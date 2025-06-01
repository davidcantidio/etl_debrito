# treat/platforms/meta.py
import pandas as pd
from treat.utils.preview_links import determine_meta_ad_preview_link

def transform_meta(df: pd.DataFrame, lookup=None) -> pd.DataFrame:
    """
    Aplica a geração de preview links para planilhas 'meta*'.
    """
    # só roda se encontrar colunas de preview no df
    if any(c.strip().lower() in ("preview_link_ig", "preview_link_fb") for c in df.columns):
        df = determine_meta_ad_preview_link(df)
    return df
