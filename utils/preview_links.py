# utils/preview_links.py

def select_meta_preview_link(url_anuncio, preview_link_fb):
    """
    Se 'url_anuncio' estiver vazio, retorna o valor de 'preview_link_fb'.
    Caso contrário, retorna o valor original de 'url_anuncio'.
    """
    if not url_anuncio or str(url_anuncio).strip() == "":
        return preview_link_fb if preview_link_fb else ""
    return url_anuncio


def generate_preview_link_from_lookup(df_parametrizacao):
    """
    Constrói um dicionário {Content (utm) → PREVIEW}, usando o campo 'ID' da BI_PARAMETRIZAÇÃO
    como ponto de correspondência com o valor vindo de 'Content (utm)' da aba de origem.

    Exemplo:
        - 'Content (utm)' na aba linkedinGeral = 'abc123'
        - Busca linha na BI_PARAMETRIZAÇÃO onde ID == 'abc123'
        - Pega o valor de PREVIEW nessa linha
        - Resultado: mapping['abc123'] = preview correspondente

    Args:
        df_parametrizacao (pd.DataFrame): DataFrame da aba BI_PARAMETRIZAÇÃO.

    Returns:
        dict: dicionário {Content (utm) → PREVIEW}
    """
    mapping = {}
    required_cols = {'ID', 'PREVIEW'}

    colunas_atuais = set(df_parametrizacao.columns)
    if not required_cols.issubset(colunas_atuais):
        import logging
        logging.warning("Colunas 'ID' e/ou 'PREVIEW' não encontradas na aba BI_PARAMETRIZAÇÃO.")
        return mapping

    for _, row in df_parametrizacao.iterrows():
        id_val = str(row.get("ID", "")).strip()
        preview = str(row.get("PREVIEW", "")).strip()
        if id_val:
            mapping[id_val] = preview
    return mapping


def build_pinterest_preview_link(id_pin):
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
