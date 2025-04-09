import logging
from utils.google_sheets import carregar_aba_google_sheets
from utils.setup_logging import setup_logging
from utils.get_campaign_parameterization import get_campaign_parameterization
from utils.read_sheet_as_dataframe import read_sheet_as_dataframe_range
from utils.get_missing_records import get_missing_records
from utils.append_records_to_sheet import append_records_to_sheet
from utils.get_google_client import get_google_client

# Funções de normalização
from utils.normalize import (
    normalize_columns,
    normalize_parametrizacao_values,
)

# Função para construir o mapping ID->PREVIEW
from utils.preview_links import construir_mapping_preview_parametrizacao

# Funções para construir e usar o mapping utm_content->CRIATIVO
from utils.get_nome_campanha import carregar_mapeamento_nome_creativo

# Importa as classes do etl_geral
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

    creds_path = "creds.json"
    spreadsheet_id = "1DazUQxspLgT0utOFHcTINbFngXw7Fq0LOq6v4lRGixg"
    spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit"

    source_sheet = "pinterestGeral"
    target_sheet = "modeloGeral"

    plataforma = source_sheet.lower().replace("geral", "").strip()

    if plataforma == "meta":
        etl_class = MetaGeralETL
        veiculo_nome = None  # Inferido dinamicamente
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
        df_origin = df_origin[df_origin["Date"].astype(str).str.strip() != ""]
    logging.debug(f"df_origin shape após remoção de linhas com Date vazio: {df_origin.shape}")

    logging.info("Carregando mapeamentos de campanha (BI_PARAMETRIZAÇÃO) ...")
    mapping_campanha, mapping_sigla = get_campaign_parameterization(creds_path, spreadsheet_id)

    extra_kwargs = {}
    if plataforma == "linkedin":
        client = get_google_client(creds_path)
        df_parametrizacao = read_sheet_as_dataframe_range(
            client, spreadsheet_id, sheet_name="BI_PARAMETRIZAÇÃO", range_str="A2:ZZ", header_row_index=0
        )
        df_parametrizacao.columns = normalize_columns(df_parametrizacao.columns)
        df_parametrizacao = normalize_parametrizacao_values(df_parametrizacao)
        mapping_preview = construir_mapping_preview_parametrizacao(df_parametrizacao)
        mapping_criativo = carregar_mapeamento_nome_creativo(df_parametrizacao)
        extra_kwargs["mapping_preview"] = mapping_preview
        extra_kwargs["mapping_criativo"] = mapping_criativo
    else:
        client = get_google_client(creds_path)

    if veiculo_nome:
        id_veiculo = get_id_veiculo_from_source(creds_path, spreadsheet_url, veiculo_nome)
    else:
        id_veiculo = None

    etl_instance = etl_class(
        df=df_origin,
        id_veiculo=id_veiculo,
        veiculo=veiculo_nome,
        mapping_campanha=mapping_campanha,
        mapping_sigla=mapping_sigla,
        **extra_kwargs
    )

    logging.info(f"Lendo dados da aba de destino '{target_sheet}'...")
    df_target = read_sheet_as_dataframe_range(client, spreadsheet_id, sheet_name=target_sheet, range_str="A1:AM", header_row_index=0)

    logging.info(f"Executando ETL Geral para '{plataforma}'...")
    df_processed = etl_instance.processar(df_destino=df_target)

    logging.info(f"ETL finalizado: {df_processed.shape[0]} linhas tratadas.")
    missing_records = get_missing_records(df_processed, df_target)
    if missing_records.empty:
        logging.info("Não há registros faltantes para inserir.")
    else:
        append_records_to_sheet(creds_path, spreadsheet_id, target_sheet, missing_records)

    logging.info(f"Processo de atualização para '{target_sheet}' concluído com sucesso.")

if __name__ == "__main__":
    main()
