# limpar_aba_google_sheets.py

from google.oauth2 import service_account
from googleapiclient.discovery import build

def get_sheet_id(service, spreadsheet_id: str, aba_nome: str) -> int:
    """
    Retorna o ID interno da aba a partir do seu nome.
    """
    spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    for sheet in spreadsheet['sheets']:
        if sheet['properties']['title'] == aba_nome:
            return sheet['properties']['sheetId']
    raise ValueError(f"Aba '{aba_nome}' não encontrada no documento.")

def limpar_aba_mantendo_cabecalho(creds_path: str, spreadsheet_id: str, aba_nome: str):
    """
    Remove todas as linhas da aba, exceto a primeira (cabeçalho).
    """
    creds = service_account.Credentials.from_service_account_file(
        creds_path, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    service = build("sheets", "v4", credentials=creds)
    
    sheet_id = get_sheet_id(service, spreadsheet_id, aba_nome)

    request_body = {
        "requests": [
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": 1  # começa na linha 2 (0-indexed)
                    }
                }
            }
        ]
    }

    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=request_body
    ).execute()

    print(f"Aba '{aba_nome}' limpa com sucesso (mantido apenas o cabeçalho).")

# =======================
# APLICAÇÃO DIRETA AQUI:
# =======================

if __name__ == "__main__":
    # Caminho do arquivo de credenciais e ID da planilha do projeto atual
    creds_path = "creds.json"
    spreadsheet_id = "1DazUQxspLgT0utOFHcTINbFngXw7Fq0LOq6v4lRGixg"

    # Nome da aba que você quer limpar
    aba_para_limpar = "modeloGenero"

    limpar_aba_mantendo_cabecalho(creds_path, spreadsheet_id, aba_para_limpar)
