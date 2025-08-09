# File: treat/platforms/tiktok.py

import pandas as pd

from transform.transform.utils.preview_links import generate_tiktok_ad_preview_link


def transform_tiktok(df: pd.DataFrame, lookup=None) -> pd.DataFrame:
    """
    Aplica o preview de anúncios do TikTok:
    - Gera/atualiza a coluna URL_do_Anuncio usando generate_tiktok_ad_preview_link.
    """
    # A função original já retorna o df com a coluna de preview ajustada
    return generate_tiktok_ad_preview_link(df)
