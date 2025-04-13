import logging
import pandas as pd


def determine_meta_ad_preview_link(df: pd.DataFrame) -> pd.DataFrame:
    """
    Determina o link de visualização do anúncio para Meta Ads,
    usando 'Preview Link FB' como fallback para 'URL_do_Anuncio'.
    Renomeia a coluna se necessário e aplica a lógica de substituição.
    """
    if 'Preview Link FB' in df.columns:
        df = df.rename(columns={'Preview Link FB': 'Preview_Link_FB'})

    if 'URL_do_Anuncio' in df.columns and 'Preview_Link_FB' in df.columns:
        df['URL_do_Anuncio'] = df.apply(
            lambda row: row['Preview_Link_FB']
            if not row['URL_do_Anuncio'] or str(row['URL_do_Anuncio']).strip() == ""
            else row['URL_do_Anuncio'],
            axis=1
        )

    return df


def generate_linkedin_ad_preview_link_from_lookup(df_parametrizacao: pd.DataFrame) -> dict:
    """
    Gera um dicionário {ID_Content: Preview_Link} a partir da aba BI_PARAMETRIZAÇÃO.
    - ID_Content vem da coluna 'ID'
    - Preview vem da coluna 'PREVIEW'
    """
    logging.debug("Iniciando geração de mapping_preview para LinkedIn...")

    # Identifica as colunas certas sem alterar o df original
    colunas_normalizadas = [col.strip().upper() for col in df_parametrizacao.columns]
    coluna_id = next((col for col in df_parametrizacao.columns if col.strip().upper() == 'ID'), None)
    coluna_preview = next((col for col in df_parametrizacao.columns if col.strip().upper() == 'PREVIEW'), None)

    if not coluna_id or not coluna_preview:
        logging.warning("Colunas 'ID' e/ou 'PREVIEW' não encontradas na aba BI_PARAMETRIZAÇÃO.")
        return {}

    df = df_parametrizacao[[coluna_id, coluna_preview]].dropna()
    df[coluna_id] = df[coluna_id].astype(str).str.strip()
    df[coluna_preview] = df[coluna_preview].astype(str).str.strip()

    mapping = dict(zip(df[coluna_id], df[coluna_preview]))

    logging.debug(f"Exemplos de mapping_preview gerados: {list(mapping.items())[:5]}")
    return mapping


def build_pinterest_preview_link(id_pin: str) -> str:
    """
    Constrói a URL de preview pública de um Pin do Pinterest a partir do seu ID.

    Args:
        id_pin (str or int): ID do Pin (ex: "1234567890")

    Returns:
        str: URL completa (ex: "https://www.pinterest.com/pin/1234567890")
    """
    if not id_pin or str(id_pin).strip() == "":
        return ""
    return f"https://www.pinterest.com/pin/{str(id_pin).strip()}"


def generate_pinterest_ad_preview_link(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preenche a coluna 'URL_do_Anuncio' com base na coluna 'Preview Link'
    aplicando a função build_pinterest_preview_link().
    """
    match_col = [col for col in df.columns if col.strip().lower() == 'preview link']
    if match_col:
        df['URL_do_Anuncio'] = df[match_col[0]].apply(build_pinterest_preview_link)
    else:
        df['URL_do_Anuncio'] = ""
    return df

