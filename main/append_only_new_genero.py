# append_only_new_genero.py

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
from utils.filter_utils import remove_zero_impressoes
from utils.normalize import format_columns_to_comma_decimal
from utils.fields_lists import GENDER_MODEL_COLUMN_ORDER

from scripts.etl_genero import (
    MetaGeneroETL,
    TikTokGeneroETL,
    LinkedinGeneroETL,
    PinterestGeneroETL,
)

def run_etl_genero():
    """
    Executa o ETL de Gênero para todas as plataformas, com delay de 60s entre elas,
    e só insere os registros com Impressoes > 0.
    """
    setup_logging(level=logging.DEBUG)

    plataformas = ["meta", "tiktok", "linkedin", "pinterest"]
    target_sheet = "modeloGenero"
    numeric_cols = ["Investimento", "Impressoes", "Cliques_no_Link", "Visualizacoes_ate_100"]

    etl_class_map = {
        "meta": (MetaGeneroETL, None),
        "tiktok": (TikTokGeneroETL, "Tiktok"),
        "linkedin": (LinkedinGeneroETL, "Linkedin"),
        "pinterest": (PinterestGeneroETL, "Pinterest"),
    }

    for plataforma in plataformas:
        source_sheet = f"{plataforma}Genero"
        logging.info(f"==== Iniciando ETL de Gênero para plataforma: {plataforma} ====")

        if plataforma not in etl_class_map:
            logging.warning(f"Plataforma '{plataforma}' não suportada. Pulando...")
            continue

        etl_class, veiculo_nome = etl_class_map[plataforma]

        logging.info(f"Lendo dados da aba de origem '{source_sheet}'...")
        df_origin = carregar_aba_google_sheets(creds_path, spreadsheet_url, source_sheet)

        # Limpeza: remove linhas sem Date
        if "Date" in df_origin.columns:
            df_origin = df_origin[df_origin["Date"].astype(str).str.strip() != ""]
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
            client, spreadsheet_id,
            sheet_name=target_sheet,
            range_str="A1:AN",
            header_row_index=0
        )

        logging.info(f"Executando ETL de Gênero para '{plataforma}'...")
        df_processed = etl_instance.processar(df_destino=df_target)
        logging.info(f"ETL finalizado: {df_processed.shape[0]} linhas tratadas.")

        missing_records = get_missing_records(df_processed, df_target)
        if missing_records.empty:
            logging.info("Não há registros faltantes para inserir.")
        else:
            # filtra só os registros com Impressoes > 0
            to_write = missing_records[missing_records["Impressoes"] > 0].copy()
            if to_write.empty:
                logging.info("Todos os registros faltantes têm Impressoes = 0; nada a inserir.")
            else:
                # formata colunas numéricas para decimal BR
                to_write = format_columns_to_comma_decimal(to_write, numeric_cols)
                # garante a ordem exata de colunas do modelo de Gênero
                to_write = to_write[GENDER_MODEL_COLUMN_ORDER]
                append_records_to_sheet(creds_path, spreadsheet_id, target_sheet, to_write)
                logging.info(f"Inseridos {to_write.shape[0]} registros na aba '{target_sheet}'.")

        logging.info(f"Atualização da aba '{target_sheet}' concluída com sucesso.")
        logging.info("Aguardando 60 segundos antes de processar próxima plataforma...\n")
        time.sleep(60)

    logging.info("Todos os ETLs de Gênero foram executados com sucesso.")

if __name__ == "__main__":
    run_etl_genero()
