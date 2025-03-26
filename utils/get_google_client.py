import gspread
from oauth2client.service_account import ServiceAccountCredentials

def get_google_client(creds_path):
    """
    Retorna um cliente autenticado do Google Sheets usando as credenciais fornecidas.
    
    Parâmetros:
        creds_path (str): Caminho para o arquivo JSON de credenciais.
    
    Retorna:
        gspread.Client: Cliente autenticado para acessar o Google Sheets.
    """
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    return gspread.authorize(creds)
