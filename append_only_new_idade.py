import logging
from utils.google_sheets import (
    carregar_aba_google_sheets,
    CREDS_PATH as creds_path,
    SPREADSHEET_ID as spreadsheet_id,
    SPREADSHEET_URL as spreadsheet_url,
)
from utils.setup_logging import setup_logging
from utils.get_campaign_parameterization import get_campaign_parameterization
from utils.read_sheet_as_dataframe import read_sheet_as_dataframe_range
from utils.get_missing_records import get_missing_records
from utils.append_records_to_sheet import append_records_to_sheet
from utils.get_google_client import get_google_client

# Importação dos ETLs de idade
from scripts.etl_idade import (
    MetaIdadeETL,
    TikTokIdadeETL,
    LinkedinIdadeETL,
    PinterestIdadeETL,
)


def main():
    setup_logging(level=logging.DEBUG)

    source_sheet = "tiktokIdade"
    target_sheet = "modeloIdade"

    plataforma = source_sheet.lower().replace("idade", "").strip()

    if plataforma == "meta":
        etl_class = MetaIdadeETL
        veiculo_nome = None  # Inferido dinamicamente por Placement
    elif plataforma == "tiktok":
        etl_class = TikTokIdadeETL
        veiculo_nome = "Tiktok"
    elif plataforma == "linkedin":
        etl_class = LinkedinIdadeETL
        veiculo_nome = "Linkedin"
    elif plataforma == "pinterest":
        etl_class = PinterestIdadeETL
        veiculo_nome = "Pinterest"
    else:
        raise ValueError(f"Não há subclasse de ETL definida para a plataforma '{plataforma}'")

    logging.info(f"Lendo dados da aba de origem '{source_sheet}'...")
    df_origin = carregar_aba_google_sheets(creds_path, spreadsheet_url, source_sheet)

    if "Date" in df_origin.columns:
        df_origin = df_origin[df_origin["Date"].astype(str).str.strip() != ""]
    logging.debug(f"df_origin shape após limpeza: {df_origin.shape}")

    logging.info("Carregando mapeamentos de campanha (BI_PARAMETRIZAÇÃO)...")
    mapping_campanha, mapping_sigla = get_campaign_parameterization(creds_path, spreadsheet_id)

    etl_instance = etl_class(
        df=df_origin,
        mapping_campanha=mapping_campanha,
        mapping_sigla=mapping_sigla
    )

    logging.info(f"Lendo dados da aba de destino '{target_sheet}'...")
    client = get_google_client(creds_path)
    df_target = read_sheet_as_dataframe_range(
        client, spreadsheet_id, sheet_name=target_sheet, range_str="A1:AN", header_row_index=0
    )

    logging.info(f"Executando ETL de Idade para '{plataforma}'...")
    df_processed = etl_instance.processar(df_destino=df_target)


    logging.info(f"ETL finalizado: {df_processed.shape[0]} linhas tratadas.")
    missing_records = get_missing_records(df_processed, df_target)
    if missing_records.empty:
        logging.info("Não há registros faltantes para inserir.")
    else:
        append_records_to_sheet(creds_path, spreadsheet_id, target_sheet, missing_records)

    logging.info(f"Atualização da aba '{target_sheet}' concluída com sucesso.")

if __name__ == "__main__":
    main()