import logging
from utils.google_sheets import carregar_aba_google_sheets
from utils.setup_logging import setup_logging
from utils.get_campaign_parameterization import get_campaign_parameterization
from utils.read_sheet_as_dataframe import read_sheet_as_dataframe_range
from utils.get_missing_records import get_missing_records
from utils.append_records_to_sheet import append_records_to_sheet
from utils.get_google_client import get_google_client
from utils.numeracao import gerar_numeracao

from scripts.etl_idade import MetaIdadeETL, TikTokIdadeETL, LinkedinIdadeETL, PinterestIdadeETL

def get_id_veiculo_from_source(creds_path, spreadsheet_url, nome_veiculo):
    df_source = carregar_aba_google_sheets(creds_path, spreadsheet_url, "SOURCE")
    filtro = df_source['Descrição da Mídia'].str.strip().str.lower() == nome_veiculo.lower()
    id_val = df_source.loc[filtro, 'ID_Veiculo']
    if not id_val.empty:
        return int(id_val.values[0])
    raise ValueError(f"ID_Veiculo para '{nome_veiculo}' não encontrado na aba SOURCE")

def main():
    setup_logging(level=logging.DEBUG)

    creds_path = "creds.json"
    spreadsheet_id = "1DazUQxspLgT0utOFHcTINbFngXw7Fq0LOq6v4lRGixg"
    spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit"

    source_sheet = "metaIdade"
    target_sheet = "modeloIdade"

    plataforma = source_sheet.lower().replace("idade", "")

    if plataforma == "meta":
        etl_class = MetaIdadeETL
        veiculo_nome = "Meta"
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
        raise ValueError(f"Plataforma '{plataforma}' não suportada para ETL de idade")

    logging.info(f"Lendo dados da aba de origem '{source_sheet}'...")
    df_origin = carregar_aba_google_sheets(creds_path, spreadsheet_url, source_sheet)
    if "Date" in df_origin.columns:
        df_origin = df_origin[df_origin["Date"].astype(str).str.strip() != ""]
    logging.debug(f"df_origin shape após filtro: {df_origin.shape}")

    logging.info("Carregando parametrização de campanhas...")
    mapping_campanha, mapping_sigla = get_campaign_parameterization(creds_path, spreadsheet_id)

    logging.info(f"Buscando ID_Veiculo para '{veiculo_nome}'...")
    id_veiculo = get_id_veiculo_from_source(creds_path, spreadsheet_url, veiculo_nome)
    logging.debug(f"id_veiculo={id_veiculo}")

    etl_instance = etl_class(df_origin, mapping_campanha, mapping_sigla)
    etl_instance.atribuir_id_veiculo(id_veiculo)

    client = get_google_client(creds_path)
    logging.info(f"Lendo dados da aba de destino '{target_sheet}'...")
    df_target = read_sheet_as_dataframe_range(client, spreadsheet_id, target_sheet, range_str="A1:P", header_row_index=0)

    df_processed = etl_instance.processar()
    logging.info(f"ETL finalizado: {df_processed.shape[0]} linhas tratadas.")

    logging.info("Aplicando numeração sequencial...")
    df_processed = gerar_numeracao(df_processed, df_target)

    missing_records = get_missing_records(df_processed, df_target)
    if missing_records.empty:
        logging.info("Não há registros faltantes para inserir. Processo encerrado.")
    else:
        logging.info(f"Serão inseridos {missing_records.shape[0]} registros faltantes.")
        append_records_to_sheet(creds_path, spreadsheet_id, target_sheet, missing_records)

    logging.info(f"Processo de atualização para '{target_sheet}' concluído com sucesso.")

if __name__ == "__main__":
    main()
