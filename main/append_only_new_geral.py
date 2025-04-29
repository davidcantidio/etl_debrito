# append_only_new_geral.py

import logging
import time
from utils.google_sheets import (
    carregar_aba_google_sheets,
    CREDS_PATH as creds_path,
    SPREADSHEET_ID as spreadsheet_id,
    SPREADSHEET_URL as spreadsheet_url,
)
from utils.setup_logging import setup_logging
from utils.substitute_origin_values import (
    apply_all_origin_substitutions,
    )
from utils.datas import fill_missing_start_end_from_params
from utils.get_campaign_parameterization import get_campaign_parameterization
from utils.read_sheet_as_dataframe import read_sheet_as_dataframe_range
from utils.get_missing_records import get_missing_records
from utils.append_records_to_sheet import append_records_to_sheet
from utils.get_google_client import get_google_client
from utils.common_linkedin import preparar_kwargs_linkedin
from utils.normalize import normalize_date_columns, fill_empty_objective_with_reach
from scripts.etl_geral import (
    MetaGeralETL,
    TiktokGeralETL,
    LinkedinGeralETL,
    PinterestGeralETL,
)


def run_etl_geral():
    """
    Executa o fluxo completo do ETL Geral para todas as plataformas,
    com delay de 60 segundos entre cada execução.
    """
    setup_logging(level=logging.DEBUG)

    plataformas = ["tiktok"]
    target_sheet = "modeloGeral"

    for plataforma in plataformas:
        logging.info(f"=== Iniciando ETL Geral para plataforma: {plataforma} ===")

        source_sheet = f"{plataforma}Geral"

        etl_class_map = {
            "meta": (MetaGeralETL, None),
            "tiktok": (TiktokGeralETL, "Tiktok"),
            "linkedin": (LinkedinGeralETL, "Linkedin"),
            "pinterest": (PinterestGeralETL, "Pinterest"),
        }

        if plataforma not in etl_class_map:
            logging.warning(f"Plataforma '{plataforma}' não suportada. Pulando.")
            continue

        etl_class, veiculo_nome = etl_class_map[plataforma]

        # 1) Leitura dos dados de origem
        logging.info(f"Lendo dados da aba de origem '{source_sheet}'...")
        df_origin = carregar_aba_google_sheets(creds_path, spreadsheet_url, source_sheet)

        client = get_google_client(creds_path)
        sheet = client.open_by_key(spreadsheet_id).worksheet(source_sheet)
        headers = sheet.row_values(1)
        logging.debug(f"Cabeçalho real encontrado na aba '{source_sheet}': {headers}")
        # 2) Substituições de exceção (in-place + grava no Sheets)]

        df_origin = apply_all_origin_substitutions(
            df_origin,
            sheet_name=source_sheet,
            write_back=True,
            inplace=True
        )

        
        df_origin = normalize_date_columns(
            df_origin,
            date_columns=["Date", "Start", "End"],
            sheet_name=source_sheet,
            write_back=True,
            inplace=True,
        )
        # 3) Preenche Start/End faltantes via BI_PARAMETRIZAÇÃO (com write_back se desejar)
        logging.info("Preenchendo datas Start/End faltantes a partir de BI_PARAMETRIZAÇÃO…")
        df_origin = fill_missing_start_end_from_params(
            df_origin,
            sheet_name=source_sheet,   # necessário para batch_update
            write_back=True,
            inplace=True            # grava também no Sheets
                )

        df_origin = fill_empty_objective_with_reach(
            df_origin,
            sheet_name=source_sheet,   # necessário para gravação
            write_back=True,
            inplace=True,
        )
                # 4) Filtra linhas vazias de Date
        if "Date" in df_origin.columns:
            df_origin = df_origin[df_origin["Date"].astype(str).str.strip() != ""]
        logging.debug(f"df_origin shape após limpeza: {df_origin.shape}")

        # 5) Carrega parametrização de campanha
        logging.info("Carregando mapeamentos de campanha (BI_PARAMETRIZAÇÃO)...")
        mapping_campanha, mapping_sigla = get_campaign_parameterization(creds_path, spreadsheet_id)

        extra_kwargs = {}
        if plataforma == "linkedin":
            extra_kwargs.update(preparar_kwargs_linkedin())

        # 6) Instancia e executa o ETL
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

        logging.info(f"Executando ETL Geral para '{plataforma}'...")
        df_processed = etl_instance.processar(df_destino=df_target)

        logging.debug("Prévia dos dados processados:")
        logging.debug(df_processed.head(3).to_string())

        # 7) Compara e insere novos registros
        logging.info(f"ETL finalizado: {df_processed.shape[0]} linhas tratadas.")
        missing_records = get_missing_records(df_processed, df_target)

        if missing_records.empty:
            logging.info("Não há registros faltantes para inserir.")
        else:
            append_records_to_sheet(creds_path, spreadsheet_id, target_sheet, missing_records)
            logging.info(f"Inseridos {missing_records.shape[0]} novos registros na aba '{target_sheet}'.")

        logging.info(f"Atualização da aba '{target_sheet}' concluída com sucesso.")
        logging.info("Aguardando 60 segundos antes de prosseguir para a próxima plataforma...")
        time.sleep(60)

    logging.info("Todos os ETLs Gerais foram executados com sucesso.")


if __name__ == "__main__":
    run_etl_geral()
