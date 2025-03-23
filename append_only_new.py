import os
import logging
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from scripts.ELET_etl_geral_meta import etl_geral_meta
from utils.google_sheets import carregar_aba_google_sheets

def ler_dataframe_aba(creds_path, spreadsheet_id, aba_name, offset_col=0):
    """
    Lê a aba 'aba_name' da planilha identificada por spreadsheet_id e retorna um DataFrame limpo.
    Remove linhas completamente vazias e, se existir, linhas cujo valor na coluna 'ID' esteja vazio.
    O parâmetro offset_col permite ignorar as primeiras colunas (se necessário).
    """
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    client = gspread.authorize(creds)
    sh = client.open_by_key(spreadsheet_id)
    worksheet = sh.worksheet(aba_name)
    
    df = get_as_dataframe(worksheet, dtype=str, na_filter=False)
    # Se um offset de colunas foi especificado (por exemplo, se os dados começam na coluna B)
    if offset_col:
        df = df.iloc[:, offset_col:]
    # Remove linhas completamente vazias
    df = df.dropna(how="all")
    # Se a coluna 'ID' existir, converte para string e remove linhas com valor vazio ou espaços
    if "ID" in df.columns:
        df["ID"] = df["ID"].astype(str)
        df = df[df["ID"].str.strip() != ""]
    return df

def append_novos_registros(creds_path, spreadsheet_id, aba_destino, df_novos):
    """
    Anexa os registros de df_novos ao final da aba_destino, sem sobrescrever os dados existentes.
    Após a inserção, redimensiona a aba para o tamanho exato dos dados.
    """
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    client = gspread.authorize(creds)
    sh = client.open_by_key(spreadsheet_id)
    worksheet = sh.worksheet(aba_destino)

    # Ler os dados atuais da aba de destino e remover linhas vazias
    df_destino_atual = get_as_dataframe(worksheet, dtype=str, na_filter=False)
    df_destino_atual = df_destino_atual.dropna(how="all")
    if "ID" in df_destino_atual.columns:
        df_destino_atual["ID"] = df_destino_atual["ID"].astype(str)
        df_destino_atual = df_destino_atual[df_destino_atual["ID"].str.strip() != ""]
    
    # Log dos primeiros 10 IDs únicos da aba de destino (se existir)
    if "ID" in df_destino_atual.columns:
        destino_ids = df_destino_atual["ID"].dropna().str.strip().unique().tolist()
        logging.info(f"IDs únicos na aba destino: {destino_ids[:10]} ... Total: {len(destino_ids)}")
    else:
        logging.info("A aba destino não possui coluna 'ID'.")
    
    # Definir a próxima linha livre: se a aba estiver vazia, incluir cabeçalho
    if df_destino_atual.empty:
        next_row = 1
        include_header = True
    else:
        next_row = df_destino_atual.shape[0] + 1
        include_header = False

    set_with_dataframe(
        worksheet,
        df_novos,
        row=next_row,
        col=1,
        include_column_header=include_header
    )

    # Redimensiona a aba para o total exato de linhas e colunas
    total_rows = (df_destino_atual.shape[0] if not df_destino_atual.empty else 0) + df_novos.shape[0] + (1 if include_header else 0)
    total_cols = df_novos.shape[1]
    worksheet.resize(rows=total_rows, cols=total_cols)
    
    logging.info(f"Inseridos {df_novos.shape[0]} novos registros na aba '{aba_destino}' a partir da linha {next_row}.")

def main():
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    # Parâmetros
    creds_path = "creds.json"
    
    # Planilha de origem: Aba "Meta - Geral"
    fonte_sheet_url = "https://docs.google.com/spreadsheets/d/1DazUQxspLgT0utOFHcTINbFngXw7Fq0LOq6v4lRGixg/edit"
    fonte_aba = "Meta - Geral"
    # Extraído da URL
    fonte_sheet_id = "1DazUQxspLgT0utOFHcTINbFngXw7Fq0LOq6v4lRGixg"

    # Planilha de destino: Aba "modeloGeral"
    spreadsheet_id = "1DazUQxspLgT0utOFHcTINbFngXw7Fq0LOq6v4lRGixg"
    aba_destino = "modeloGeral"

    # 1) Ler dados da aba de origem e aplicar ETL
    logging.info(f"Lendo dados da aba de origem '{fonte_aba}'...")
    df_origem = carregar_aba_google_sheets(creds_path, fonte_sheet_url, fonte_aba)
    logging.info(f"Dados carregados da origem: {df_origem.shape[0]} linhas.")

    logging.info("Aplicando ETL Meta...")
    etl = etl_geral_meta(df_origem)
    df_tratado = etl.processar()
    logging.info(f"ETL finalizado: {df_tratado.shape[0]} linhas tratadas.")

    # Log dos primeiros 10 IDs únicos gerados na origem
    if "ID" in df_tratado.columns:
        origem_ids = df_tratado["ID"].dropna().astype(str).str.strip().unique().tolist()
        logging.info(f"IDs únicos na origem (ETL): {origem_ids[:10]} ... Total: {len(origem_ids)}")
    else:
        logging.warning("A coluna 'ID' não foi encontrada no DF tratado.")

    # 2) Ler os dados existentes na aba de destino e limpar linhas vazias
    logging.info(f"Lendo dados atuais da aba de destino '{aba_destino}'...")
    df_destino_atual = ler_dataframe_aba(creds_path, spreadsheet_id, aba_destino, offset_col=0)
    if "ID" in df_destino_atual.columns:
        destino_ids = df_destino_atual["ID"].dropna().astype(str).str.strip().unique().tolist()
        logging.info(f"IDs únicos na aba destino: {destino_ids[:10]} ... Total: {len(destino_ids)}")
    else:
        logging.info("A aba de destino não possui coluna 'ID'.")
    logging.info(f"Dados atuais na aba destino (após limpeza): {df_destino_atual.shape[0]} linhas.")

    # 3) Filtrar registros novos: comparar pelo campo "ID"
    if df_destino_atual.empty:
        logging.info("A aba de destino está vazia; todos os registros serão considerados novos.")
        df_novos = df_tratado.copy()
    else:
        if "ID" not in df_destino_atual.columns:
            logging.warning("A aba de destino não possui a coluna 'ID'. Todas as linhas serão consideradas novas.")
            df_novos = df_tratado.copy()
        else:
            df_novos = df_tratado[~df_tratado["ID"].isin(df_destino_atual["ID"])].copy()
    
    if df_novos.empty:
        logging.info("Não há novos registros para inserir. Processo encerrado.")
        return
    else:
        logging.info(f"Foram identificados {df_novos.shape[0]} novos registros para inserção.")

    # 4) Anexar os registros novos ao final da aba de destino
    append_novos_registros(creds_path, spreadsheet_id, aba_destino, df_novos)
    logging.info("Processo de inserção incremental concluído com sucesso.")

if __name__ == "__main__":
    main()
