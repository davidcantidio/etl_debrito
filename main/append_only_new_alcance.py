# append_only_new_alcance.py

import logging
import time
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
from utils.common_linkedin import preparar_kwargs_linkedin
from utils.filter_utils import remove_zero_impressoes    # ← importe aqui

from scripts.etl_alcance import (
    MetaAlcanceETL,
    TikTokAlcanceETL,
    LinkedinAlcanceETL,
    PinterestAlcanceETL,
)

def run_etl_alcance():
    setup_logging(level=logging.DEBUG)

    plataformas = ["meta"]
    target_sheet = "modeloAlcance"

    etl_class_map = {
        "meta": (MetaAlcanceETL, None),
        "tiktok": (TikTokAlcanceETL, "Tiktok"),
        "linkedin": (LinkedinAlcanceETL, "Linkedin"),
        "pinterest": (PinterestAlcanceETL, "Pinterest"),
    }

    for plataforma in plataformas:
        source_sheet = f"{plataforma}Alcance"
        logging.info(f"==== Iniciando ETL de Alcance para plataforma: {plataforma} ====")

        if plataforma not in etl_class_map:
            logging.warning(f"Plataforma '{plataforma}' não suportada. Pulando...")
            continue

        etl_class, veiculo_nome = etl_class_map[plataforma]

        logging.info(f"Lendo dados da aba de origem '{source_sheet}'...")
        df_origin = carregar_aba_google_sheets(creds_path, spreadsheet_url, source_sheet)
        if "Date" in df_origin.columns:
            df_origin = df_origin[df_origin["Date"].astype(str).str.strip() != ""]
        logging.debug(f"df_origin shape após limpeza: {df_origin.shape}")

        logging.info("Carregando mapeamentos de campanha (BI_PARAMETRIZAÇÃO)...")
        mapping_campanha, mapping_sigla = get_campaign_parameterization(creds_path, spreadsheet_id)

        extra_kwargs = {}
        if plataforma == "linkedin":
            extra_kwargs.update(preparar_kwargs_linkedin())

        etl_instance = etl_class(
            df=df_origin,
            id_veiculo=None,
            veiculo=veiculo_nome,
            mapping_campanha=mapping_campanha,
            mapping_sigla=mapping_sigla,
            **extra_kwargs
        )

        logging.info(f"Lendo dados da aba de destino '{target_sheet}'...")
        client = get_google_client(creds_path)
        df_target = read_sheet_as_dataframe_range(
            client, spreadsheet_id,
            sheet_name=target_sheet,
            range_str="A1:AM",
            header_row_index=0
        )

        logging.info(f"Executando ETL de Alcance para '{plataforma}'...")
        df_processed = etl_instance.processar(df_destino=df_target)
        logging.info(f"ETL finalizado: {df_processed.shape[0]} linhas tratadas.")

        # só insere quem efetivamente faltou E que tem impressões > 0
        missing = get_missing_records(df_processed, df_target)
        if missing.empty:
            logging.info("Não há registros faltantes para inserir.")
        else:
            to_write = remove_zero_impressoes(missing)
            if to_write.empty:
                logging.info("Todos os registros faltantes têm Impressoes = 0; nada a inserir.")
            else:
                append_records_to_sheet(creds_path, spreadsheet_id, target_sheet, to_write)
                logging.info(f"Inseridos {to_write.shape[0]} registros na aba '{target_sheet}'.")

        logging.info("Aguardando 60 segundos antes de processar próxima plataforma...\n")
        time.sleep(60)

    logging.info("Todos os ETLs de Alcance foram executados com sucesso.")
