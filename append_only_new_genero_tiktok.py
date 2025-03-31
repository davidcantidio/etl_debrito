import logging
from scripts.ELET_etl_genero_tiktok import etl_genero_tiktok  # <-- Arquivo/classe criada para o ETL do Gênero
from utils.google_sheets import carregar_aba_google_sheets
from utils.setup_logging import setup_logging
from utils.get_campaign_parameterization import get_campaign_parameterization
from utils.read_sheet_as_dataframe import read_sheet_as_dataframe
from utils.get_missing_records import get_missing_records
from utils.append_records_to_sheet import append_records_to_sheet
from utils.get_google_client import get_google_client

def main():
    # 1) Configura o nível de log (pode ser DEBUG ou INFO)
    setup_logging(level=logging.DEBUG)
    
    creds_path = "creds.json"
    spreadsheet_id = "1DazUQxspLgT0utOFHcTINbFngXw7Fq0LOq6v4lRGixg"  # Ajuste para o ID da sua planilha
    
    # 2) Definir a aba de origem e a aba de destino
    source_sheet = "Tiktok Genero"
    target_sheet = "modeloGenero"
    
    logging.info(f"Lendo dados da aba de origem '{source_sheet}'...")
    df_origin = carregar_aba_google_sheets(
        creds_path,
        f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit",
        source_sheet
    )
    if "Date" in df_origin.columns:
        # Filtra linhas onde "Date" está preenchido
        df_origin = df_origin[df_origin["Date"].astype(str).str.strip() != ""]
    logging.info(f"Dados carregados da origem '{source_sheet}': {df_origin.shape[0]} linhas.")
    
    # 3) Carregar mapeamentos de campanha (para gerar Campanha/ID_Campanha via lookup)
    logging.info("Carregando parametrização de campanhas...")
    mapping_campanha, mapping_sigla = get_campaign_parameterization(creds_path, spreadsheet_id)
    
    # 4) Executar o ETL para TikTok Gênero
    logging.info("Executando ETL para Tiktok Gênero...")
    etl_instance = etl_genero_tiktok(df_origin, mapping_campanha, mapping_sigla)
    df_processed = etl_instance.processar()
    logging.info(f"ETL finalizado: {df_processed.shape[0]} linhas tratadas.")
    
    if "ID" in df_processed.columns:
        unique_ids = df_processed["ID"].dropna().astype(str).str.strip().unique()
        logging.info(f"IDs únicos (exemplo): {list(unique_ids)[:10]} ... Total: {len(unique_ids)}")
    
    # 5) Ler a aba de destino, onde iremos inserir (modeloGenero)
    client = get_google_client(creds_path)
    logging.info(f"Lendo dados da aba de destino '{target_sheet}'...")
    df_target = read_sheet_as_dataframe(client, spreadsheet_id, target_sheet, offset_col=0)
    logging.info(f"Aba de destino '{target_sheet}' contém {df_target.shape[0]} linhas.")
    
    # 6) Identificar registros faltantes e inseri-los
    missing_records = get_missing_records(df_processed, df_target)
    if missing_records.empty:
        logging.info("Não há registros faltantes para inserir. Processo encerrado.")
    else:
        logging.info(f"Serão inseridos {missing_records.shape[0]} registros faltantes.")
        append_records_to_sheet(creds_path, spreadsheet_id, target_sheet, missing_records)
    
    logging.info(f"Processo de atualização para '{target_sheet}' concluído com sucesso.")

if __name__ == "__main__":
    main()
