# utils/preview_links.py

def ajustar_preview_link(url_anuncio, preview_link_fb):
    """
    Se 'url_anuncio' estiver vazio, retorna o valor de 'preview_link_fb'.
    Caso contrário, retorna o valor original de 'url_anuncio'.
    """
    if not url_anuncio or str(url_anuncio).strip() == "":
        return preview_link_fb if preview_link_fb else ""
    return url_anuncio


def construir_mapping_preview_parametrizacao(df_parametrizacao):
    """
    Constrói um dicionário de mapeamento de preview link para LinkedIn a partir da aba BI_PARAMETRIZAÇÃO.

    Usa o valor da coluna 'utm_content' como chave e o valor da coluna 'PREVIEW' como destino.
    Se as colunas necessárias não existirem, retorna um dicionário vazio.

    Args:
        df_parametrizacao (pd.DataFrame): DataFrame da aba BI_PARAMETRIZAÇÃO.

    Returns:
        dict: dicionário {utm_content -> PREVIEW} ou vazio se as colunas não existirem.
    """
    mapping = {}
    if 'utm_content' not in df_parametrizacao.columns or 'PREVIEW' not in df_parametrizacao.columns:
        import logging
        logging.warning("Colunas 'utm_content' e/ou 'PREVIEW' não encontradas na aba BI_PARAMETRIZAÇÃO. Retornando mapping vazio.")
        return mapping

    for _, row in df_parametrizacao.iterrows():
        chave = str(row['utm_content']).strip()
        valor = str(row['PREVIEW']).strip() if row.get('PREVIEW') is not None else ""
        if chave and chave not in mapping:
            mapping[chave] = valor
    return mapping


def construir_preview_link_pinterest(id_pin):
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
