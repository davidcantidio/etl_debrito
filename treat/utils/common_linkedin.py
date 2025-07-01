# common_linkedin.py

import logging

logging.basicConfig(level=logging.DEBUG)

from utils.get_google_client import get_google_client
from utils.read_sheet_as_dataframe import read_sheet_as_dataframe_range
from utils.normalize import normalize_columns, normalize_parametrizacao_values
from utils.preview_links import generate_linkedin_ad_preview_link_from_lookup
from utils.creative_mapping import load_ad_name_mapping, get_ad_name_from_utm_content
from utils.google_sheets import CREDS_PATH, SPREADSHEET_ID


def carregar_mapeamentos_linkedin():
    """
    Carrega os mapeamentos de preview e criativo da aba BI_PARAMETRIZAÇÃO
    para uso nos ETLs do LinkedIn.

    Agora assume que os cabeçalhos estão na linha 2 da planilha (A2:ZZ)
    e, portanto, usamos header_row_index=0.
    """
    import logging
    from utils.get_google_client import get_google_client
    from utils.read_sheet_as_dataframe import read_sheet_as_dataframe_range
    from utils.google_sheets import CREDS_PATH, SPREADSHEET_ID
    from utils.normalize import normalize_columns, normalize_parametrizacao_values
    from utils.creative_mapping import load_ad_name_mapping

    logging.info("Carregando mapeamentos do LinkedIn (preview e criativo)...")

    client = get_google_client(CREDS_PATH)
    # Aqui usamos A2:ZZ e header_row_index=0 para indicar que a linha 2 é o cabeçalho.
    df_parametrizacao = read_sheet_as_dataframe_range(
        client,
        SPREADSHEET_ID,
        sheet_name="BI_PARAMETRIZAÇÃO",
        range_str="A2:ZZ",
        header_row_index=0,
    )

    # Normaliza os nomes das colunas (isso converte para minúsculo, remove quebras de linha e acentuação)
    df_parametrizacao.columns = normalize_columns(df_parametrizacao.columns)
    logging.debug(
        f"[DEBUG] Colunas após normalização: {df_parametrizacao.columns.tolist()}"
    )

    # Aplica normalização dos valores nos campos de parametrização
    df_parametrizacao = normalize_parametrizacao_values(df_parametrizacao)

    # Gerar o mapeamento preview: o lookup usará a coluna 'utm_content' e os valores deverão ser extraídos da coluna 'preview'
    mapping_preview = generate_linkedin_ad_preview_link_from_lookup(df_parametrizacao)
    mapping_criativo = load_ad_name_mapping(df_parametrizacao)

    return mapping_preview, mapping_criativo


def buscar_nome_criativo_com_log(utm_content, mapping_criativo):
    """
    Busca o nome do criativo com base no utm_content.
    Se não encontrar, loga uma mensagem de debug.
    """
    utm_str = str(utm_content).strip()
    nome = get_ad_name_from_utm_content(utm_str, mapping_criativo)
    if not nome:
        logging.debug(
            f"[LinkedIn] utm_content '{utm_str}' NÃO encontrado no mapping_criativo."
        )
    return nome


def preparar_kwargs_linkedin() -> dict:
    """
    Prepara os kwargs extras necessários para inicializar ETLs do LinkedIn,
    carregando os mapeamentos de preview e criativo.
    """
    mapping_preview, mapping_criativo = carregar_mapeamentos_linkedin()
    return {
        "mapping_preview": mapping_preview,
        "mapping_criativo": mapping_criativo,
    }


def preencher_nomes_anuncio_linkedin(df, mapping_criativo):
    """
    Preenche as colunas 'Nome_do_Anuncio' e 'Nome_do_Conjunto_de_Anuncio' no DataFrame de LinkedIn.
    - 'Nome_do_Anuncio' recebe o valor presente na coluna 'ID_Content'
    - 'Nome_do_Conjunto_de_Anuncio' é o resultado do lookup da coluna 'ID_Content'
      no mapeamento de criativo (mapping_criativo).

    Se algum valor em 'ID_Content' não tiver correspondência, é feito log de aviso.
    """

    df = df.copy()

    if "ID_Content" not in df.columns:
        raise KeyError(
            "A coluna 'ID_Content' não foi encontrada no DataFrame após renomeação."
        )

    # 'Nome_do_Anuncio' recebe diretamente o valor de 'ID_Content'
    df["Nome_do_Anuncio"] = df["ID_Content"]
    # 'Nome_do_Conjunto_de_Anuncio' é preenchido via mapeamento (ID_Content → nome amigável)
    df["Nome_do_Conjunto_de_Anuncio"] = df["ID_Content"].map(mapping_criativo)

    # Log de valores de ID_Content que não tiveram correspondência
    utms_nao_mapeados = (
        df[df["Nome_do_Conjunto_de_Anuncio"].isna()]["ID_Content"].dropna().unique()
    )
    if len(utms_nao_mapeados) > 0:
        logging.warning(
            "ID_Content sem correspondência em BI_PARAMETRIZAÇÃO (LinkedIn):"
        )
        for utm in utms_nao_mapeados[:10]:
            logging.warning(f"- {utm}")

    return df
