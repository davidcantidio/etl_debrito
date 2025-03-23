import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from gspread_dataframe import get_as_dataframe

def carregar_aba_google_sheets(creds_path, sheet_url, nome_aba):
    """
    Autentica com o Google Sheets e retorna um DataFrame da aba especificada.
    """
    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    client = gspread.authorize(creds)

    sheet = client.open_by_url(sheet_url)
    aba = sheet.worksheet(nome_aba)

    df = get_as_dataframe(aba, dtype=str, na_filter=False)
    return df
