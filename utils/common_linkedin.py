import logging
from utils.get_google_client import get_google_client
from utils.read_sheet_as_dataframe import read_sheet_as_dataframe_range
from utils.normalize import normalize_columns, normalize_parametrizacao_values
from utils.preview_links import generate_linkedin_ad_preview_link_from_lookup
from utils.get_nome_campanha import carregar_mapeamento_nome_creativo, obter_nome_por_utm_content
from utils.google_sheets import CREDS_PATH, SPREADSHEET_ID

def carregar_mapeamentos_linkedin():
    """
    Carrega os mapeamentos de preview e criativo da aba BI_PARAMETRIZAÇÃO
    para uso nos ETLs do LinkedIn.
    """
    logging.info("Carregando mapeamentos do LinkedIn (preview e criativo)...")

    client = get_google_client(CREDS_PATH)

    df_parametrizacao = read_sheet_as_dataframe_range(
        client,
        SPREADSHEET_ID,
        sheet_name="BI_PARAMETRIZAÇÃO",
        range_str="A2:ZZ",
        header_row_index=0
    )

    df_parametrizacao.columns = normalize_columns(df_parametrizacao.columns)
    df_parametrizacao = normalize_parametrizacao_values(df_parametrizacao)

    mapping_preview = generate_linkedin_ad_preview_link_from_lookup(df_parametrizacao)
    mapping_criativo = carregar_mapeamento_nome_creativo(df_parametrizacao)

    return mapping_preview, mapping_criativo

def buscar_nome_criativo_com_log(utm_content, mapping_criativo):
    """
    Busca o nome do criativo com base no utm_content.
    Se não encontrar, loga uma mensagem de debug.
    """
    utm_str = str(utm_content).strip()
    nome = obter_nome_por_utm_content(utm_str, mapping_criativo)
    if not nome:
        logging.debug(f"utm_content '{utm_str}' NÃO encontrado no mapping_criativo.")
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
    Preenche as colunas 'Nome_do_Anuncio' e 'Nome_do_Conjunto_de_Anuncio' com base no ID_Content
    e mapeamento de criativo (utm_content → nome).
    """
    if 'ID_Content' not in df.columns:
        df['Nome_do_Anuncio'] = ""
        df['Nome_do_Conjunto_de_Anuncio'] = ""
        return df

    df['Nome_do_Anuncio'] = df['ID_Content'].apply(
        lambda utm: buscar_nome_criativo_com_log(utm, mapping_criativo)
    )
    df['Nome_do_Conjunto_de_Anuncio'] = df['Nome_do_Anuncio']
    return df