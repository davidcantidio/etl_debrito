# append_only_new_geral.py

import logging
import sys
from utils.google_sheets import carregar_aba_google_sheets
from utils.setup_logging import setup_logging
from utils.get_campaign_parameterization import get_campaign_parameterization
from utils.read_sheet_as_dataframe import read_sheet_as_dataframe
from utils.get_missing_records import get_missing_records
from utils.append_records_to_sheet import append_records_to_sheet
from utils.get_google_client import get_google_client

# Import das classes do novo etl_geral.py, incluindo a Meta que já faz o ajuste do Preview Link FB
from scripts.etl_geral import (
    MetaGeralETL,
    TiktokGeralETL,
    LinkedinGeralETL,
    PinterestGeralETL,
)

def get_id_veiculo_from_source(creds_path, spreadsheet_url, nome_veiculo):
    """
    Lê a aba SOURCE e obtém o ID_Veiculo (coluna 'ID_Veiculo') associado
    ao 'nome_veiculo' (coluna 'Descrição da Mídia').
    """
    df_source = carregar_aba_google_sheets(creds_path, spreadsheet_url, "SOURCE")
    filtro = df_source['Descrição da Mídia'].str.strip().str.lower() == nome_veiculo.lower()
    id_val = df_source.loc[filtro, 'ID_Veiculo']
    if not id_val.empty:
        return int(id_val.values[0])
    raise ValueError(f"ID_Veiculo para '{nome_veiculo}' não encontrado na aba SOURCE")

def main():
    setup_logging(level=logging.DEBUG)

    # Ajuste conforme o seu ambiente
    creds_path = "creds.json"
    spreadsheet_id = "1DazUQxspLgT0utOFHcTINbFngXw7Fq0LOq6v4lRGixg"
    spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit"

    # Defina aqui a aba de ORIGEM (ex.: "metaGeral", "tiktokGeral", "linkedinGeral", etc.)
    source_sheet = "metaGeral"

    # Aba de DESTINO onde será feito o append:
    target_sheet = "modeloGeral"

    # Identifica a plataforma com base no nome da aba (ex.: "meta" para "metaGeral")
    plataforma = source_sheet.lower().replace("geral", "").strip()

    # Mapeamento de plataforma para a classe ETL e nome do veículo
    if plataforma == "meta":
        etl_class = MetaGeralETL
        veiculo_nome = "Meta"
    elif plataforma == "tiktok":
        etl_class = TiktokGeralETL
        veiculo_nome = "Tiktok"
    elif plataforma == "linkedin":
        etl_class = LinkedinGeralETL
        veiculo_nome = "Linkedin"
    elif plataforma == "pinterest":
        etl_class = PinterestGeralETL
        veiculo_nome = "Pinterest"
    else:
        raise ValueError(f"Não há subclasse de ETL definida para a plataforma '{plataforma}'")

    logging.info(f"Lendo dados da aba de origem '{source_sheet}'...")
    df_origin = carregar_aba_google_sheets(creds_path, spreadsheet_url, source_sheet)
    if "Date" in df_origin.columns:
        # Remove linhas sem data, se necessário
        df_origin = df_origin[df_origin["Date"].astype(str).str.strip() != ""]

    logging.info("Carregando mapeamentos de campanha (BI_PARAMETRIZAÇÃO)...")
    mapping_campanha, mapping_sigla = get_campaign_parameterization(creds_path, spreadsheet_id)

    logging.info(f"Buscando ID_Veiculo para '{veiculo_nome}' na aba SOURCE...")
    id_veiculo = get_id_veiculo_from_source(creds_path, spreadsheet_url, veiculo_nome)

    # Executa o ETL usando a classe apropriada.
    # No caso de Meta, o método processar() já aplica a lógica para ajustar o preview link.
    logging.info(f"Executando ETL Geral para '{plataforma}'...")
    etl_instance = etl_class(
        df=df_origin,
        id_veiculo=id_veiculo,
        veiculo=veiculo_nome,
        mapping_campanha=mapping_campanha,
        mapping_sigla=mapping_sigla
    )
    df_processed = etl_instance.processar()
    logging.info(f"ETL finalizado: {df_processed.shape[0]} linhas tratadas.")

    # Lê o DataFrame de destino para comparação
    client = get_google_client(creds_path)
    logging.info(f"Lendo dados da aba de destino '{target_sheet}'...")
    df_target = read_sheet_as_dataframe(client, spreadsheet_id, target_sheet, offset_col=0)
    logging.info(f"Aba de destino '{target_sheet}' contém {df_target.shape[0]} linhas.")

    # Verifica quais registros ainda não estão no destino
    missing_records = get_missing_records(df_processed, df_target)
    if missing_records.empty:
        logging.info("Não há registros faltantes para inserir. Processo encerrado.")
    else:
        logging.info(f"Serão inseridos {missing_records.shape[0]} registros faltantes.")
        append_records_to_sheet(creds_path, spreadsheet_id, target_sheet, missing_records)

    logging.info(f"Processo de atualização para '{target_sheet}' concluído com sucesso.")

if __name__ == "__main__":
    main()
