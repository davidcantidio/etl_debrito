# append_only_new_regiao.py

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

# Importação dos ETLs de região
from scripts.etl_regiao import (
    MetaRegiaoETL,
    TikTokRegiaoETL,
    LinkedinRegiaoETL,
    PinterestRegiaoETL,
)

def run_etl_regiao():
    setup_logging(level=logging.DEBUG)

    # Aba de origem com os dados regionais. Ajuste conforme a plataforma (ex.: "tiktokRegiao")
    source_sheet = "metaRegiao"  
    # Aba de destino para o modelo de região
    target_sheet = "modeloRegiao"    

    # Define a plataforma removendo a palavra "regiao" do nome da aba de origem
    plataforma = source_sheet.lower().replace("regiao", "").strip()

    if plataforma == "meta":
        etl_class = MetaRegiaoETL
        veiculo_nome = None  # Será inferido dinamicamente via Placement, se aplicável
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
        raise ValueError(f"Não há subclasse de ETL definida para a plataforma '{plataforma}'")

    logging.info(f"Lendo dados da aba de origem '{source_sheet}'...")
    df_origin = carregar_aba_google_sheets(creds_path, spreadsheet_url, source_sheet)

    # Remove linhas com a coluna "Date" vazia
    if "Date" in df_origin.columns:
        df_origin = df_origin[df_origin["Date"].astype(str).str.strip() != ""]
    logging.debug(f"df_origin shape após limpeza: {df_origin.shape}")

    logging.info("Carregando mapeamentos de campanha (BI_PARAMETRIZAÇÃO)...")
    mapping_campanha, mapping_sigla = get_campaign_parameterization(creds_path, spreadsheet_id)

    # Inicializa a instância do ETL utilizando os parâmetros de campanha e identificação do veículo
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
    if missing_records.empty:
        logging.info("Não há registros faltantes para inserir.")
    else:
        append_records_to_sheet(creds_path, spreadsheet_id, target_sheet, missing_records)
        logging.info(f"Inseridos {missing_records.shape[0]} novos registros na aba '{target_sheet}'.")

    logging.info(f"Atualização da aba '{target_sheet}' concluída com sucesso.")
    return df_processed

if __name__ == "__main__":
    run_etl_regiao()
