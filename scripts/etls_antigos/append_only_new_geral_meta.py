import logging
from scripts.ELET_etl_geral_meta import etl_geral_meta
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
    source_sheet = "Meta - Geral"
    target_sheet = "modeloGeral"
    
    # 1. Ler a aba de origem usando a função de carregamento já existente
    logging.info(f"Lendo dados da aba de origem '{source_sheet}'...")
    df_origin = carregar_aba_google_sheets(
        creds_path,
        f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit",
        source_sheet
    )
    if "Date" in df_origin.columns:
        df_origin = df_origin[df_origin["Date"].astype(str).str.strip() != ""]
    logging.info(f"Dados carregados da origem: {df_origin.shape[0]} linhas.")
    
    # 2. Carregar os mapeamentos de campanhas da aba BI_PARAMETRIZAÇÃO
    logging.info("Carregando parametrização de campanhas...")
    mapping_campanha, mapping_sigla = get_campaign_parameterization(creds_path, spreadsheet_id)
    
    # 3. Executar o ETL para 'Meta - Geral'
    logging.info("Executando ETL para 'Meta - Geral'...")
    etl_instance = etl_geral_meta(df_origin, mapping_campanha, mapping_sigla)
    df_tratado = etl_instance.processar()
    logging.info(f"ETL finalizado: {df_tratado.shape[0]} linhas tratadas.")
    
    if "ID" in df_tratado.columns:
        unique_ids = df_tratado["ID"].dropna().astype(str).str.strip().unique()
        logging.info(f"IDs únicos (exemplo): {list(unique_ids)[:10]} ... Total: {len(unique_ids)}")
    
    # 4. Ler a aba de destino utilizando offset_col=0 (dados iniciam na coluna A)
    client = get_google_client(creds_path)
    logging.info(f"Lendo dados da aba de destino '{target_sheet}'...")
    df_destino = read_sheet_as_dataframe(client, spreadsheet_id, target_sheet, offset_col=0)
    logging.info(f"Aba de destino '{target_sheet}' contém {df_destino.shape[0]} linhas.")
    
    # 5. Identificar registros faltantes e inseri-los, se houver
    missing_records = get_missing_records(df_tratado, df_destino)
    if missing_records.empty:
        logging.info("Não há registros faltantes para inserir. Processo encerrado.")
    else:
        logging.info(f"Serão inseridos {missing_records.shape[0]} registros faltantes.")
        append_records_to_sheet(creds_path, spreadsheet_id, target_sheet, missing_records)
    
    logging.info("Processo de atualização para 'modeloGeral' concluído com sucesso.")

if __name__ == "__main__":
    main()
