# append_only_new_regiao.py

import logging
import time
from utils.google_sheets import (
    carregar_aba_google_sheets,
    CREDS_PATH as creds_path,
    SPREADSHEET_ID as spreadsheet_id,
    SPREADSHEET_URL as spreadsheet_url,
)
from utils.setup_logging import setup_logging
from utils.lookups_bi_parametrizacao import get_campaign_parameterization
from utils.read_sheet_as_dataframe import read_sheet_as_dataframe_range
from utils.get_missing_records import get_missing_records
from utils.append_records_to_sheet import append_records_to_sheet
from utils.get_google_client import get_google_client

# Importação dos ETLs de região
from scripts.etl_regiao import (
    MetaRegiaoETL,
    TikTokRegiaoETL,
    LinkedinRegiaoETL,
    PinterestRegiaoETL,
)
from utils.filter_utils import remove_zero_impressoes

def run_etl_regiao():
    setup_logging(level=logging.DEBUG)

    plataformas = ["meta", "tiktok", "linkedin", "pinterest"]
    target_sheet = "modeloRegiao"

    for plataforma in plataformas:
        logging.info(f"=== Iniciando ETL Região para plataforma: {plataforma} ===")

        source_sheet = f"{plataforma}Regiao"
        if plataforma == "meta":
            etl_class = MetaRegiaoETL
            veiculo_nome = None
        elif plataforma == "tiktok":
            etl_class = TikTokRegiaoETL
            veiculo_nome = "Tiktok"
        elif plataforma == "linkedin":
            etl_class = LinkedinRegiaoETL
            veiculo_nome = "Linkedin"
        elif plataforma == "pinterest":
            etl_class = PinterestRegiaoETL
            veiculo_nome = "Pinterest"
        else:
            logging.warning(f"Plataforma '{plataforma}' não suportada. Pulando.")
            continue

        logging.info(f"Lendo dados da aba de origem '{source_sheet}'...")
        df_origin = carregar_aba_google_sheets(creds_path, spreadsheet_url, source_sheet)
        if "date" in df_origin.columns:
            df_origin = df_origin[df_origin["date"].astype(str).str.strip() != ""]
        logging.debug(f"df_origin shape após limpeza: {df_origin.shape}")

        logging.info("Carregando mapeamentos de campanha (BI_PARAMETRIZAÇÃO)...")
        mapping_campanha, mapping_sigla = get_campaign_parameterization(creds_path, spreadsheet_id)

        etl_instance = etl_class(
            df=df_origin,
            id_veiculo=None,
            veiculo=veiculo_nome,
            mapping_campanha=mapping_campanha,
            mapping_sigla=mapping_sigla
        )

        logging.info(f"Lendo dados da aba de destino '{target_sheet}'...")
        client = get_google_client(creds_path)
        df_target = read_sheet_as_dataframe_range(
            client, spreadsheet_id, sheet_name=target_sheet, range_str="A1:AN", header_row_index=0
        )

        logging.info(f"Executando ETL de Região para '{plataforma}'...")
        df_processed = etl_instance.processar(df_destino=df_target)
        logging.info(f"ETL finalizado: {df_processed.shape[0]} linhas tratadas.")

        missing_records = get_missing_records(df_processed, df_target)
        # filtra antes de inserir apenas linhas com Impressoes > 0
        df_to_append = remove_zero_impressoes(missing_records)

        if df_to_append.empty:
            logging.info("Não há registros válidos (Impressoes > 0) para inserir.")
        else:
            append_records_to_sheet(creds_path, spreadsheet_id, target_sheet, df_to_append)
            logging.info(f"Inseridos {df_to_append.shape[0]} novos registros na aba '{target_sheet}'.")

        logging.info(f"Atualização da aba '{target_sheet}' para '{plataforma}' concluída com sucesso.")
        logging.info("Aguardando 60 segundos antes do próximo ETL...")
        time.sleep(60)

    logging.info("Todos os ETLs de Região foram executados com sucesso.")
if __name__ == "__main__":
    run_etl_regiao()
