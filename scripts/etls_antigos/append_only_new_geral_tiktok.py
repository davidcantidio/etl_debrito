import logging
from scripts.ELET_etl_geral_tk import etl_geral_tk
from utils.google_sheets import carregar_aba_google_sheets
from utils.setup_logging import setup_logging
from utils.get_campaign_parameterization import get_campaign_parameterization
from utils.read_sheet_as_dataframe import read_sheet_as_dataframe
from utils.get_missing_records import get_missing_records
from utils.append_records_to_sheet import append_records_to_sheet
from utils.get_google_client import get_google_client

def main():
    setup_logging()
    creds_path = "creds.json"
    spreadsheet_id = "1DazUQxspLgT0utOFHcTINbFngXw7Fq0LOq6v4lRGixg"
    source_sheet = "Tiktok"
    target_sheet = "modeloGeral"
    
    # 1. Ler a aba de origem
    logging.info(f"Lendo dados da aba de origem '{source_sheet}'...")
    df_origin = carregar_aba_google_sheets(
        creds_path,
        f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit",
        source_sheet
    )
    logging.debug("Colunas do DataFrame da aba Tiktok:")
    logging.debug(df_origin.columns.tolist())

    if "Date" in df_origin.columns:
        df_origin = df_origin[df_origin["Date"].astype(str).str.strip() != ""]
    logging.info(f"Dados carregados da origem '{source_sheet}': {df_origin.shape[0]} linhas.")
    
    # 2. Carregar os mapeamentos de campanhas
    logging.info("Carregando parametrização de campanhas...")
    mapping_campanha, mapping_sigla = get_campaign_parameterization(creds_path, spreadsheet_id)
    
    # 3. Executar o ETL para Tiktok
    logging.info("Executando ETL para Tiktok...")
    etl_instance = etl_geral_tk(df_origin, mapping_campanha, mapping_sigla)
    df_processed = etl_instance.processar()
    logging.info(f"ETL finalizado: {df_processed.shape[0]} linhas tratadas.")
    
    # 4. Aplicar a parametrização externa
    etl_instance.aplicar_parametrizacao_campanha_externa(mapping_campanha, mapping_sigla)
    df_processed = etl_instance.df
    logging.info(f"ETL concluído: {df_processed.shape[0]} linhas tratadas.")
    logging.debug("Colunas do df_processed:")
    logging.debug(df_processed.columns.tolist())

    logging.debug("Primeiras 5 linhas (df_processed):")
    logging.debug("\n" + df_processed.head(5).to_string())

    if "Campaign name" in df_processed.columns:
        logging.debug("Exemplos de Campaign name no DF final:")
        logging.debug(df_processed["Campaign name"].head(5).tolist())
        logging.debug("Campanha e ID_Campanha (amostra):")
        logging.debug(df_processed[["Campaign name", "Campanha", "ID_Campanha"]].head(5).to_string())

    
    if "ID" in df_processed.columns:
        unique_ids = df_processed["ID"].dropna().astype(str).str.strip().unique()
        logging.info(f"IDs únicos (exemplo): {list(unique_ids)[:10]} ... Total: {len(unique_ids)}")
    
    # 5. Ler a aba de destino utilizando offset_col=0 (colunas a partir da coluna A)
    client = get_google_client(creds_path)
    logging.info(f"Lendo dados da aba de destino '{target_sheet}'...")
    df_target = read_sheet_as_dataframe(client, spreadsheet_id, target_sheet, offset_col=0)
    logging.info(f"Aba de destino '{target_sheet}' contém {df_target.shape[0]} linhas.")
    
    # 6. Identificar registros faltantes e inseri-los
    missing_records = get_missing_records(df_processed, df_target)
    if missing_records.empty:
        logging.info("Não há registros faltantes para inserir. Processo finalizado.")
    else:
        logging.info(f"Serão inseridos {missing_records.shape[0]} registros faltantes.")
        append_records_to_sheet(creds_path, spreadsheet_id, target_sheet, missing_records)
    
    logging.info("Processo de atualização para 'modeloGeral' (Tiktok) concluído com sucesso.")

if __name__ == "__main__":
    main()
