# File: utils/limpar_aba_google_sheets.py

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

def limpar_aba_mantendo_cabecalho(
    service,
    spreadsheet_id: str,
    aba_nome: str,
    clear_range: str = "A2:ZZ"
):
    """
    Remove todos os valores da aba, exceto a primeira linha (cabeçalho).
    Não altera o número de linhas/fórmulas/formatações do sheet.

    :param service: objeto sheets v4 (build(...))
    :param spreadsheet_id: ID da planilha
    :param aba_nome: nome da aba a limpar
    :param clear_range: intervalo a partir da segunda linha (padrão "A2:ZZ")
    """
    range_to_clear = f"{aba_nome}!{clear_range}"
    try:
        service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=range_to_clear
        ).execute()
        print(f"Aba '{aba_nome}' limpa com sucesso (cabeçalho preservado).")
    except HttpError as e:
        print(f"Erro ao limpar aba '{aba_nome}': {e}")

if __name__ == "__main__":
    # Configuração inicial — adapte paths/IDs conforme seu ambiente
    CREDS_PATH     = "creds.json"
    SPREADSHEET_ID = "1DazUQxspLgT0utOFHcTINbFngXw7Fq0LOq6v4lRGixg"

    # lista de abas-modelo a limpar
    ABAS = [
        "modeloGeral",
        "modeloIdade",
        "modeloAlcance",
        "modeloRegiao",
        "modeloGenero",
    ]

    # Autenticação e serviço
    creds = service_account.Credentials.from_service_account_file(
        CREDS_PATH,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    service = build("sheets", "v4", credentials=creds)

    # Executa a limpeza em cada aba
    for aba in ABAS:
        limpar_aba_mantendo_cabecalho(service, SPREADSHEET_ID, aba)