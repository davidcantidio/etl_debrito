import os
import logging
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe
from scripts.ELET_etl_geral_meta import etl_geral_meta
from utils.google_sheets import carregar_aba_google_sheets


def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


def get_google_client(creds_path):
    """Retorna um cliente autenticado do Google Sheets."""
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    return gspread.authorize(creds)


def read_sheet_as_dataframe(client, spreadsheet_id, sheet_name, offset_col=0):
    """Lê uma aba da planilha e retorna um DataFrame limpo."""
    sh = client.open_by_key(spreadsheet_id)
    worksheet = sh.worksheet(sheet_name)
    data = worksheet.get_all_values()
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data[1:], columns=data[0])
    if offset_col > 0 and df.columns[0].strip() == "":
        df = df.iloc[:, offset_col:]
    df = df.dropna(how="all")
    if "ID" in df.columns:
        df["ID"] = df["ID"].astype(str)
        df = df[df["ID"].str.strip() != ""]
    return df


def get_campaign_parameterization(creds_path, spreadsheet_id):
    """
    Carrega os mapeamentos de campanhas a partir da aba BI_PARAMETRIZAÇÃO e retorna
    dois dicionários: mapping_campanha e mapping_sigla.
    """
    client = get_google_client(creds_path)
    sh = client.open_by_key(spreadsheet_id)
    worksheet = sh.worksheet("BI_PARAMETRIZAÇÃO")
    data = worksheet.get_all_values()
    if len(data) < 3:
        raise ValueError("A aba 'BI_PARAMETRIZAÇÃO' não tem dados suficientes "
                         "(cabeçalho na linha 2 e dados a partir da linha 3).")
    
    raw_headers = data[1]
    headers = [col.strip().replace('\n', ' ').replace('"', '').upper() for col in raw_headers]
    logging.info(f"Colunas reais detectadas (linha 2): {headers}")
    
    df_param = pd.DataFrame(data[2:], columns=headers)
    lookup = "NOME CAMPANHA"
    if lookup not in df_param.columns:
        raise ValueError(f"A coluna de lookup '{lookup}' não foi encontrada na aba BI_PARAMETRIZAÇÃO.")
    
    # Para Campanha, usamos a coluna que contenha "NOME CAMPANHA"
    col_dest_campanha = next((col for col in df_param.columns if "NOME CAMPANHA" in col), lookup)
    # Para SIGLA, buscamos a coluna que contenha "SIGLA"
    col_sigla = next((col for col in df_param.columns if "SIGLA" in col), None)
    if not col_dest_campanha or not col_sigla:
        raise ValueError("As colunas 'NOME CAMPANHA' e/ou 'SIGLA' não foram encontradas após normalização.")
    
    logging.info(f"Coluna de lookup: '{lookup}'")
    logging.info(f"Coluna usada para Campanha (destino): '{col_dest_campanha}'")
    logging.info(f"Coluna usada para SIGLA: '{col_sigla}'")
    
    mapping_campanha = {
        str(k).strip().upper(): str(v).strip()
        for k, v in zip(df_param[lookup], df_param[col_dest_campanha])
    }
    mapping_sigla = {
        str(k).strip().upper(): str(v).strip()
        for k, v in zip(df_param[lookup], df_param[col_sigla])
    }
    
    logging.info(f"Total de campanhas mapeadas: {len(mapping_campanha)}")
    logging.info(f"Exemplos de chaves no mapping (Campanha): {list(mapping_campanha.keys())[:10]}")
    return mapping_campanha, mapping_sigla


def get_missing_records(df_origin, df_target):
    """
    Compara os DataFrames de origem e destino usando a coluna 'ID' com contagem cumulativa
    para lidar com duplicatas, retornando os registros que estão faltando no destino.
    """
    origin = df_origin.copy()
    target = df_target.copy()
    
    if "ID" not in target.columns or target.empty:
        return origin.copy()
    
    origin["ID_index"] = origin.groupby("ID").cumcount() + 1
    target["ID_index"] = target.groupby("ID").cumcount() + 1
    
    merged = pd.merge(origin, target[["ID", "ID_index"]],
                      on=["ID", "ID_index"], how="left", indicator=True)
    missing = merged[merged["_merge"] == "left_only"].drop(columns=["_merge", "ID_index"])
    return missing


def append_records_to_sheet(creds_path, spreadsheet_id, sheet_name, df):
    """Insere os registros do DataFrame na aba de destino a partir da coluna B."""
    client = get_google_client(creds_path)
    sh = client.open_by_key(spreadsheet_id)
    worksheet = sh.worksheet(sheet_name)
    
    # Determina a próxima linha e se deve incluir cabeçalho
    data = worksheet.get_all_values()
    if not data:
        next_row = 1
        include_header = True
    else:
        next_row = len(data) + 1
        include_header = False

    set_with_dataframe(worksheet, df, row=next_row, col=2, include_column_header=include_header)
    
    # Redimensiona a planilha para acomodar os dados
    data = worksheet.get_all_values()
    total_rows = len(data)
    total_cols = max(len(row) for row in data) if data else 0
    worksheet.resize(rows=total_rows, cols=total_cols)
    
    logging.info(f"Inseridos {df.shape[0]} registros faltantes na aba '{sheet_name}' a partir da linha {next_row}.")
    logging.info(f"Planilha redimensionada para {total_rows} linhas e {total_cols} colunas.")


def update_modelo_geral(creds_path, spreadsheet_id, source_sheet, target_sheet):
    """
    Realiza o fluxo completo de atualização do modelo:
      1. Lê a aba de origem.
      2. Carrega a parametrização de campanhas.
      3. Executa o ETL (via etl_geral_meta).
      4. Lê a aba de destino.
      5. Identifica registros faltantes e os insere na aba de destino.
    """
    logging.info(f"Lendo dados da aba de origem '{source_sheet}'...")
    df_origin = carregar_aba_google_sheets(
        creds_path, f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit", source_sheet
    )
    if "Date" in df_origin.columns:
        df_origin = df_origin[df_origin["Date"].astype(str).str.strip() != ""]
    logging.info(f"Dados carregados da origem '{source_sheet}': {df_origin.shape[0]} linhas.")
    
    logging.info("Carregando parametrização de campanhas...")
    mapping_campanha, mapping_sigla = get_campaign_parameterization(creds_path, spreadsheet_id)
    
    logging.info("Executando ETL para 'Meta - Geral'...")
    etl = etl_geral_meta(df_origin, mapping_campanha, mapping_sigla)
    df_processed = etl.processar()
    logging.info(f"ETL concluído: {df_processed.shape[0]} linhas tratadas.")
    
    if "ID" in df_processed.columns:
        unique_ids = df_processed["ID"].dropna().astype(str).str.strip().unique()
        logging.info(f"IDs únicos (exemplo): {list(unique_ids)[:10]} ... Total: {len(unique_ids)}")
    
    client = get_google_client(creds_path)
    logging.info(f"Lendo dados da aba de destino '{target_sheet}'...")
    df_target = read_sheet_as_dataframe(client, spreadsheet_id, target_sheet, offset_col=1)
    logging.info(f"Aba de destino '{target_sheet}' contém {df_target.shape[0]} linhas.")
    
    missing_records = get_missing_records(df_processed, df_target)
    if missing_records.empty:
        logging.info("Não há registros faltantes para inserir. Processo finalizado.")
    else:
        logging.info(f"Serão inseridos {missing_records.shape[0]} registros faltantes.")
        append_records_to_sheet(creds_path, spreadsheet_id, target_sheet, missing_records)


def main():
    setup_logging()
    creds_path = "creds.json"
    spreadsheet_id = "1DazUQxspLgT0utOFHcTINbFngXw7Fq0LOq6v4lRGixg"
    source_sheet = "Meta - Geral"
    target_sheet = "modeloGeral"
    
    update_modelo_geral(creds_path, spreadsheet_id, source_sheet, target_sheet)
    logging.info("Processo de atualização para 'modeloGeral' concluído com sucesso.")


if __name__ == "__main__":
    main()
