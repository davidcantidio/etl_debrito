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
    Gera um dicionário {utm_content: preview} para ser usado no preenchimento de preview do LinkedIn.
    """
    COL_UTM = "utm_content"
    COL_PREVIEW = "preview"

    if COL_UTM not in df_parametrizacao.columns or COL_PREVIEW not in df_parametrizacao.columns:
        logging.warning("Colunas 'utm_content' ou 'preview' não encontradas em BI_PARAMETRIZAÇÃO.")
        return {}

    mapping = df_parametrizacao[[COL_UTM, COL_PREVIEW]].dropna()
    mapping = mapping.astype(str).drop_duplicates(subset=[COL_UTM])
    preview_dict = dict(zip(mapping[COL_UTM], mapping[COL_PREVIEW]))

    logging.debug("Exemplo de mapeamentos de preview gerados para LinkedIn:")
    for k, v in list(preview_dict.items())[:5]:
        logging.debug(f"{k} -> {v}")

    return preview_dict

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

