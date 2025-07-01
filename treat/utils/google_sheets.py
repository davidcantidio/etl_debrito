import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from gspread_dataframe import get_as_dataframe


# --- leitura de aba via DataFrame ---
def carregar_aba_google_sheets(creds_path, sheet_url, nome_aba):
    """
    Autentica com o Google Sheets e retorna um DataFrame da aba especificada.
    """
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    client = gspread.authorize(creds)

    sheet = client.open_by_url(sheet_url)
    aba = sheet.worksheet(nome_aba)

    df = get_as_dataframe(aba, dtype=str, na_filter=False)
    return df


# --- escrita em batch (batch_update) ---
def batch_update_sheet_values(
    creds_path: str,
    sheet_url: str,
    nome_aba: str,
    a1_range: str,
    values: list[list],
    value_input_option: str = "USER_ENTERED",
) -> None:
    """
    Autentica com o Google Sheets e faz batch_update de uma faixa A1.
    - creds_path: caminho para o JSON de credenciais
    - sheet_url: URL completa da planilha
    - nome_aba: nome da aba onde escrever
    - a1_range: range no formato "A1" ou "A1:D10"
    - values: lista de listas com os valores (primeira linha normalmente cabeçalhos)
    - value_input_option: USER_ENTERED ou RAW
    """
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    client = gspread.authorize(creds)

    sheet = client.open_by_url(sheet_url)
    aba = sheet.worksheet(nome_aba)

    aba.batch_update(
        [{"range": a1_range, "values": values}], value_input_option=value_input_option
    )
