# utils/atribuicoes_via_lookup.py

import logging
from utils.google_sheets import carregar_aba_google_sheets, CREDS_PATH, SPREADSHEET_ID, SPREADSHEET_URL
from utils.normalize import inferir_veiculo_meta_por_placement
from utils.campanha_mapper import buscar_mapping

import pandas as pd

def atribuir_id_veiculo_generico(df: pd.DataFrame) -> pd.DataFrame:
    """
    Atribui 'ID_Veiculo' com base no valor da coluna 'Veiculo',
    buscando a correspondência na aba SOURCE.
    """
    logging.debug(">>> In atribuir_id_veiculo_generico")
    try:
        df_source = carregar_aba_google_sheets(
            CREDS_PATH,
            SPREADSHEET_URL,
            "SOURCE"
        )
        df_source['Descrição da Mídia'] = df_source['Descrição da Mídia'].str.strip().str.lower()
        mapping = dict(zip(df_source['Descrição da Mídia'], df_source['ID_Veiculo']))
        df['ID_Veiculo'] = df['Veiculo'].str.strip().str.lower().map(mapping).fillna("")
    except Exception as e:
        logging.warning(f"Falha ao mapear ID_Veiculo via SOURCE: {e}")
        df['ID_Veiculo'] = ""
    return df

def atribuir_veiculo_e_id_meta(df: pd.DataFrame) -> pd.DataFrame:
    """
    Inferência de Veiculo + ID_Veiculo para META com base em 'Placement' e aba SOURCE.
    """
    logging.debug(">>> In atribuir_veiculo_e_id_meta")
    df = inferir_veiculo_meta_por_placement(df)
    return atribuir_id_veiculo_generico(df)

def atribuir_veiculo_por_criativo(df: pd.DataFrame) -> pd.DataFrame:
    """
    Faz lookup do campo 'Ad name' (que representa o CRIATIVO) na aba BI_PARAMETRIZAÇÃO
    e preenche o campo 'Veiculo' com base na coluna 'VEÍCULOS'.
    """
    logging.debug(">>> In atribuir_veiculo_por_criativo (via Ad name → CRIATIVO)")

    df_param = carregar_aba_google_sheets(CREDS_PATH, SPREADSHEET_URL, "BI_PARAMETRIZAÇÃO", header_row_index=1)
    df_param.columns = [col.strip().upper() for col in df_param.columns]

    if 'CRIATIVO' not in df_param.columns or 'VEÍCULOS' not in df_param.columns:
        logging.warning("Colunas 'CRIATIVO' ou 'VEÍCULOS' não encontradas em BI_PARAMETRIZAÇÃO.")
        df['Veiculo'] = ""
        return df

    mapping = dict(zip(df_param['CRIATIVO'].astype(str).str.strip(), df_param['VEÍCULOS'].astype(str).str.strip()))
    df['Veiculo'] = df['Ad name'].astype(str).str.strip().map(mapping).fillna("")
    return df

def preencher_campos_com_campanha(df: pd.DataFrame) -> pd.DataFrame:
    """
    Para plataformas como Pinterest onde não há Ad Name,
    replica 'Campaign name' nos campos de destino:
    - Campanha
    - Nome_do_Anuncio
    - Nome_do_Conjunto_de_Anuncio
    """
    logging.debug(">>> In preencher_campos_com_campanha (Pinterest)")

    if 'Campaign name' not in df.columns:
        logging.warning("Coluna 'Campaign name' não encontrada. Não será possível preencher campos derivados.")
        df['Campanha'] = ""
        df['Nome_do_Anuncio'] = ""
        df['Nome_do_Conjunto_de_Anuncio'] = ""
        return df

    df['Campanha'] = df['Campaign name']
    df['Nome_do_Anuncio'] = df['Campaign name']
    df['Nome_do_Conjunto_de_Anuncio'] = df['Campaign name']
    return df


def aplicar_parametrizacao_campanha(df: pd.DataFrame, mapping_campanha: dict, mapping_sigla: dict) -> pd.DataFrame:
    """
    Preenche as colunas 'Campanha' e 'ID_Campanha' com base na coluna 'Campaign_name',
    utilizando dicionários de mapeamento externos.
    """
    if 'Campaign_name' not in df.columns:
        df['Campanha'] = ""
        df['ID_Campanha'] = ""
        return df

    df['Campanha'] = df['Campaign_name'].apply(
        lambda x: buscar_mapping(mapping_campanha, x) or x
    )
    df['ID_Campanha'] = df['Campaign_name'].apply(
        lambda x: buscar_mapping(mapping_sigla, x)
    )
    return df
