import logging
import sys
import pandas as pd
from utils.google_sheets import carregar_aba_google_sheets
from utils.setup_logging import setup_logging
from utils.get_campaign_parameterization import get_campaign_parameterization
from utils.read_sheet_as_dataframe import read_sheet_as_dataframe
from utils.get_missing_records import get_missing_records
from utils.append_records_to_sheet import append_records_to_sheet
from utils.get_google_client import get_google_client
from utils.geolocalizacao import carregar_caches_padrao

# Importa as subclasses de ETL específicas de região
from scripts.etl_regiao import TiktokRegiaoETL, MetaRegiaoETL, LinkedinRegiaoETL, PinterestRegiaoETL

def get_id_veiculo_from_source(creds_path, spreadsheet_url, nome_veiculo):
    df_source = carregar_aba_google_sheets(creds_path, spreadsheet_url, "SOURCE")
    filtro = df_source['Descrição da Mídia'].str.strip().str.lower() == nome_veiculo.lower()
    id_val = df_source.loc[filtro, 'ID_Veiculo']
    if not id_val.empty:
        return int(id_val.values[0])
    raise ValueError(f"ID_Veiculo para '{nome_veiculo}' não encontrado na aba SOURCE")

def main():
    setup_logging(level=logging.INFO)

    creds_path = "creds.json"
    spreadsheet_id = "1DazUQxspLgT0utOFHcTINbFngXw7Fq0LOq6v4lRGixg"
    spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit"

    # Definição da aba de origem e destino
    source_sheet = "tiktokRegiao"
    target_sheet = "modeloRegiao"

    # Identifica a plataforma com base no nome da aba (remove "regiao" e espaços)
    plataforma = source_sheet.lower().replace("regiao", "").strip()

    # Mapeamento de plataforma para a classe ETL e nome do veículo
    if plataforma == "tiktok":
        etl_class = TiktokRegiaoETL
        veiculo_nome = "Tiktok"
    elif plataforma == "meta":
        etl_class = MetaRegiaoETL
        veiculo_nome = "Meta"
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
    if "Date" in df_origin.columns:
        df_origin = df_origin[df_origin["Date"].astype(str).str.strip() != ""]

    logging.info("Carregando mapeamentos de campanha...")
    mapping_campanha, mapping_sigla = get_campaign_parameterization(creds_path, spreadsheet_id)

    logging.info("Carregando caches de geolocalização...")
    cache_estados, cache_municipios = carregar_caches_padrao()

    logging.info(f"Buscando ID_Veiculo da aba SOURCE para '{veiculo_nome}'...")
    id_veiculo = get_id_veiculo_from_source(creds_path, spreadsheet_url, veiculo_nome)

    logging.info(f"Executando ETL de Região para '{plataforma}'...")
    etl_instance = etl_class(
        df=df_origin,
        id_veiculo=id_veiculo,
        veiculo=veiculo_nome,
        mapping_campanha=mapping_campanha,
        mapping_sigla=mapping_sigla,
        cache_estados=cache_estados,
        cache_municipios=cache_municipios
    )

    client = get_google_client(creds_path)
    logging.info(f"Lendo dados da aba de destino '{target_sheet}'...")
    df_target = read_sheet_as_dataframe(client, spreadsheet_id, target_sheet, offset_col=0)
    if not df_target.empty:
        logging.info("Primeiras linhas da aba de destino:")
        logging.info(df_target.head().to_string())
    if 'Numero' in df_target.columns:
        logging.info("Últimos valores em 'Numero': " + str(df_target['Numero'].tail().tolist()))
    else:
        logging.warning("Coluna 'Numero' não encontrada na aba destino!")

    # Verificação de df_target:
    logging.info("Colunas do df_target: " + str(df_target.columns.tolist()))
    logging.info("Exemplo de valores na coluna 'Numero': " + str(df_target["Numero"].head().tolist() if "Numero" in df_target.columns else "Coluna 'Numero' não encontrada"))
    logging.info(f"Número de linhas em df_target: {df_target.shape[0]}")
    
    # Se df_target for vazio, garantimos que seja um DataFrame vazio (isso já está sendo feito, mas aqui fica explícito)
    if df_target is None or df_target.empty:
        logging.warning("df_target está vazio. A numeração dos novos registros iniciará no valor padrão.")
        df_target = pd.DataFrame()
    
    df_processed = etl_instance.processar(df_destino=df_target)
    logging.info(f"ETL finalizado: {df_processed.shape[0]} linhas tratadas.")

    missing_records = get_missing_records(df_processed, df_target)
    if missing_records.empty:
        logging.info("Não há registros faltantes para inserir. Processo encerrado.")
    else:
        logging.info(f"Serão inseridos {missing_records.shape[0]} registros faltantes.")
        append_records_to_sheet(creds_path, spreadsheet_id, target_sheet, missing_records)

    logging.info(f"Processo de atualização para '{target_sheet}' concluído com sucesso.")

    if hasattr(etl_instance, "exibir_correspondencia_regiao"):
        print("\n--- Dicionário de correspondência Região ---")
        etl_instance.exibir_correspondencia_regiao()

if __name__ == "__main__":
    main()
