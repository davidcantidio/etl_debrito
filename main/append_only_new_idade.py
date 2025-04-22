# append_only_new_idade.py

import logging
import time
from gspread.utils import rowcol_to_a1
from utils.fields_lists import AGE_MODEL_COLUMN_ORDER
from utils.get_google_client import get_google_client
from utils.google_sheets import (
    carregar_aba_google_sheets,
    CREDS_PATH as creds_path,
    SPREADSHEET_ID as spreadsheet_id,
    SPREADSHEET_URL as spreadsheet_url,
)
from utils.setup_logging import setup_logging
from utils.filter_utils import remove_zero_impressoes
from utils.get_campaign_parameterization import get_campaign_parameterization
from utils.read_sheet_as_dataframe import read_sheet_as_dataframe_range
from utils.get_missing_records import get_missing_records
from utils.append_records_to_sheet import append_records_to_sheet
from utils.normalize import format_columns_to_comma_decimal
# Importação dos ETLs de idade
from scripts.etl_idade import (
    MetaIdadeETL,
    TikTokIdadeETL,
    LinkedinIdadeETL,
    PinterestIdadeETL,
)


def garantir_cabecalho_idade():
    """
    Garante que a linha 1 da aba 'modeloIdade' contenha todas as colunas de AGE_MODEL_COLUMN_ORDER.
    Se alguma estiver faltando, sobrescreve apenas A1 até a última coluna do modelo.
    """
    client = get_google_client(creds_path)
    ws = client.open_by_key(spreadsheet_id).worksheet("modeloIdade")

    header_atual = ws.row_values(1)  # sem valores vazios à direita
    faltam = set(AGE_MODEL_COLUMN_ORDER) - set(header_atual)
    if faltam:
        ultima = rowcol_to_a1(1, len(AGE_MODEL_COLUMN_ORDER))  # ex: 'P1'
        ws.update(f"A1:{ultima}", [AGE_MODEL_COLUMN_ORDER])
        logging.info("Cabeçalho de modeloIdade atualizado para as 16 colunas do modelo.")
    else:
        logging.debug("Cabeçalho de modeloIdade já está completo.")


def run_etl_idade():
    setup_logging(level=logging.DEBUG)
    garantir_cabecalho_idade()

    plataformas = ["meta", "tiktok", "linkedin", "pinterest"]
    target_sheet = "modeloIdade"

    for plataforma in plataformas:
        logging.info(f"=== Iniciando ETL Idade para plataforma: {plataforma} ===")
        source_sheet = f"{plataforma}Idade"

        if plataforma == "meta":
            etl_class = MetaIdadeETL
            veiculo_nome = None
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
            logging.warning(f"Plataforma '{plataforma}' não suportada. Pulando.")
            continue

        logging.info(f"Lendo dados da aba de origem '{source_sheet}'...")
        df_origin = carregar_aba_google_sheets(creds_path, spreadsheet_url, source_sheet)
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
            client,
            spreadsheet_id,
            sheet_name=target_sheet,
            range_str="A1:AN",
            header_row_index=0
        )

        logging.info(f"Executando ETL de Idade para '{plataforma}'...")
        df_processed = etl_instance.processar(df_destino=df_target)
        logging.info(f"ETL finalizado: {df_processed.shape[0]} linhas tratadas.")

        missing_records = get_missing_records(df_processed, df_target)
        if missing_records.empty:
            logging.info("Não há registros faltantes para inserir.")
        else:
            # Filtra só aqueles com Impressoes > 0
            to_write = missing_records[missing_records["Impressoes"] > 0].copy()
            if to_write.empty:
                logging.info("Todos os registros faltantes têm Impressoes = 0; nada a inserir.")
            else:
                # Formata colunas numéricas
                numeric_cols = ["Investimento", "Impressoes", "Cliques_no_Link", "Visualizacoes_ate_100"]
                to_write = format_columns_to_comma_decimal(to_write, numeric_cols)
                # Garante ordem de colunas exata do modelo
                to_write = to_write[AGE_MODEL_COLUMN_ORDER]
                append_records_to_sheet(creds_path, spreadsheet_id, target_sheet, to_write)
                logging.info(f"Inseridos {to_write.shape[0]} registros na aba '{target_sheet}'.")

        logging.info(f"Atualização da aba '{target_sheet}' concluída com sucesso.")
        logging.info("Aguardando 60 segundos antes do próximo ETL...")
        time.sleep(60)

    logging.info("Todos os ETLs de Idade foram executados com sucesso.")

if __name__ == "__main__":
    run_etl_idade()
