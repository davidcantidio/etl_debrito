import logging
from utils.google_sheets import carregar_aba_google_sheets
from utils.normalize import inferir_veiculo_meta_por_placement

SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1DazUQxspLgT0utOFHcTINbFngXw7Fq0LOq6v4lRGixg/edit"
CREDENTIALS_PATH = "creds.json"

def atribuir_veiculo_e_id_meta(df):
    """
    Inferência de Veiculo + ID_Veiculo para META com base em 'Placement' e aba SOURCE.
    """
    logging.debug(">>> In atribuir_veiculo_e_id_meta")
    df = inferir_veiculo_meta_por_placement(df)
    return atribuir_id_veiculo_generico(df)

def atribuir_id_veiculo_generico(df):
    """
    Atribui 'ID_Veiculo' com base no valor da coluna 'Veiculo',
    buscando a correspondência na aba SOURCE.
    """
    try:
        df_source = carregar_aba_google_sheets(
            CREDENTIALS_PATH,
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
