from google.oauth2 import service_account
from googleapiclient.discovery import build


def get_sheet_id(service, spreadsheet_id: str, aba_nome: str) -> int:
    """
    Retorna o ID interno da aba a partir do seu nome.
    """
    spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    for sheet in spreadsheet["sheets"]:
        if sheet["properties"]["title"] == aba_nome:
            return sheet["properties"]["sheetId"]
    raise ValueError(f"Aba '{aba_nome}' não encontrada no documento.")


def limpar_aba_mantendo_cabecalho(service, spreadsheet_id: str, aba_nome: str):
    """
    Remove todas as linhas da aba, exceto a primeira (cabeçalho).
    """
    sheet_id = get_sheet_id(service, spreadsheet_id, aba_nome)

    request_body = {
        "requests": [
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": 1,  # começa na linha 2 (0-indexed)
                    }
                }
            }
        ]
    }

    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body=request_body
    ).execute()

    print(f"Aba '{aba_nome}' limpa com sucesso (mantido apenas o cabeçalho).")


if __name__ == "__main__":
    # Configuração inicial
    creds_path = "creds.json"
    spreadsheet_id = "1DazUQxspLgT0utOFHcTINbFngXw7Fq0LOq6v4lRGixg"

    # Abas a limpar
    abas_para_limpar = [
        "modeloGeral",
        "modeloIdade",
        "modeloAlcance",
        "modeloRegiao",
        "modeloGenero",
    ]

    # Autenticação e serviço
    creds = service_account.Credentials.from_service_account_file(
        creds_path, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    service = build("sheets", "v4", credentials=creds)

    # Executa a limpeza em todas as abas
    for aba in abas_para_limpar:
        try:
            limpar_aba_mantendo_cabecalho(service, spreadsheet_id, aba)
        except Exception as e:
            print(f"Erro ao limpar aba '{aba}': {e}")
