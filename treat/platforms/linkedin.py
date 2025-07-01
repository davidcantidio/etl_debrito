# treat/platforms/linkedin.py

from typing import Any, Dict, Optional

import pandas as pd


def _init_preview_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Garante que exista a coluna 'URL_do_Anuncio' no DataFrame.
    """
    df["URL_do_Anuncio"] = df.get("URL_do_Anuncio", "")
    return df


def _apply_preview_map(df: pd.DataFrame, preview_map: Dict[str, str]) -> pd.DataFrame:
    """
    Preenche 'URL_do_Anuncio' usando o mapeamento de utm_content → preview,
    somente onde 'URL_do_Anuncio' esteja vazio.
    """
    if preview_map and "utm_content" in df.columns:
        df["URL_do_Anuncio"] = df["URL_do_Anuncio"].where(
            df["URL_do_Anuncio"].str.strip() != "",
            df["utm_content"].astype(str).map(preview_map).fillna(""),
        )
    return df


def _override_ad_name(df: pd.DataFrame, ad_name_map: Dict[str, str]) -> pd.DataFrame:
    """
    Substitui 'ad_name' com base em utm_content → ad_name, onde houver mapeamento.
    """
    if ad_name_map and "utm_content" in df.columns:
        df["ad_name"] = (
            df["utm_content"]
            .astype(str)
            .str.strip()
            .map(ad_name_map)
            .fillna(df.get("ad_name", ""))
        )
    return df


def _sync_ad_group(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ajusta 'ad_group_name' para ter sempre o mesmo valor que 'ad_name'.
    """
    df["ad_group_name"] = df["ad_name"]
    return df


def transform_linkedin(df: pd.DataFrame, lookup: Optional[Any] = None) -> pd.DataFrame:
    """
    Constrói as colunas de preview e sincroniza ad_name/ad_group_name para
    dados de LinkedIn, usando um BIParamLookup passado em `lookup`.

    - lookup.df() deve retornar o DataFrame da aba BI_PARAMETRIZAÇÃO.
    - lookup.get_linkedin_ad_name_map() fornece o mapeamento utm_content → novo ad_name.
    """
    # Importa dinamicamente para evitar dependência circular
    from treat.utils.preview_links import \
        generate_linkedin_ad_preview_link_from_lookup

    # 1) Cria/garante a coluna de preview
    df = _init_preview_column(df)

    # 2) Gera mapeamento de utm_content → preview_link (coluna 'preview' em BI_PARAMETRIZAÇÃO)
    preview_map = (
        generate_linkedin_ad_preview_link_from_lookup(lookup.df())
        if lookup is not None
        else {}
    )
    df = _apply_preview_map(df, preview_map)

    # 3) Gera mapeamento utm_content → ad_name social (taxonomy_ad_name_social)
    ad_name_map = lookup.get_linkedin_ad_name_map() if lookup is not None else {}
    df = _override_ad_name(df, ad_name_map)

    # 4) Sincroniza ad_group_name com o novo ad_name
    df = _sync_ad_group(df)

    return df
