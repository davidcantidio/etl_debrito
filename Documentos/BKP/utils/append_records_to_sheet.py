import logging
import gspread
from gspread_dataframe import set_with_dataframe
from utils.get_google_client import get_google_client

def append_records_to_sheet(creds_path, spreadsheet_id, sheet_name, df):
    """
    Insere os registros do DataFrame na aba de destino, a partir da coluna B,
    e redimensiona a planilha para acomodar os dados inseridos.

    Parâmetros:
        creds_path (str): Caminho para o arquivo de credenciais.
        spreadsheet_id (str): ID da planilha.
        sheet_name (str): Nome da aba de destino.
        df (pandas.DataFrame): DataFrame contendo os registros a serem inseridos.
    """
    # Obtém o cliente autenticado do Google Sheets
    client = get_google_client(creds_path)
    sh = client.open_by_key(spreadsheet_id)
    worksheet = sh.worksheet(sheet_name)
    
    # Determina a próxima linha disponível e se deve incluir cabeçalho
    data = worksheet.get_all_values()
    if not data:
        next_row = 1
        include_header = True
    else:
        next_row = len(data) + 1
        include_header = False

    # Insere o DataFrame na aba, iniciando na coluna B (col=1)
    set_with_dataframe(worksheet, df, row=next_row, col=1, include_column_header=include_header)
    
    # Redimensiona a planilha de acordo com os dados atuais
    data = worksheet.get_all_values()
    total_rows = len(data)
    total_cols = max(len(row) for row in data) if data else 0
    worksheet.resize(rows=total_rows, cols=total_cols)
    
    logging.info(f"Inseridos {df.shape[0]} registros na aba '{sheet_name}' a partir da linha {next_row}.")
    logging.info(f"Planilha redimensionada para {total_rows} linhas e {total_cols} colunas.")
