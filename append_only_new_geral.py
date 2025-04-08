# append_only_new_geral.py

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
    # Vamos usar nivel DEBUG para coletar todos os logs
    setup_logging(level=logging.DEBUG)

    creds_path = "creds.json"
    spreadsheet_id = "1DazUQxspLgT0utOFHcTINbFngXw7Fq0LOq6v4lRGixg"
    spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit"

    source_sheet = "linkedinGeral"
    target_sheet = "modeloGeral"

    plataforma = source_sheet.lower().replace("geral", "").strip()

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
    logging.debug(f"df_origin shape: {df_origin.shape}")
    logging.debug(f"Colunas df_origin: {df_origin.columns}")

    # Remove linhas em que a coluna "Date" está vazia (se existir)
    if "Date" in df_origin.columns:
        df_origin = df_origin[df_origin["Date"].astype(str).str.strip() != ""]
    logging.debug(f"df_origin shape após remoção de linhas com Date vazio: {df_origin.shape}")

    # Aqui não estamos normalizando o nome das colunas de df_origin,
    # pois isso quebraria referências como "Date", "Campaign name" etc.
    # Mas, se desejar normalizar também, tenha cuidado para ajustar o ETL.
    # df_origin.columns = normalize_columns(df_origin.columns)  # <--- só se quiser mesmo renomear colunas

    # Caso você queira normalizar o *conteúdo* do df_origin também, poderia fazer:
    # df_origin = normalize_parametrizacao_values(df_origin)

    # ---------------------------------------------------
    # Carregando mapeamentos de campanha (BI_PARAMETRIZAÇÃO)
    # ---------------------------------------------------
    logging.info("Carregando mapeamentos de campanha (BI_PARAMETRIZAÇÃO) ...")
    mapping_campanha, mapping_sigla = get_campaign_parameterization(creds_path, spreadsheet_id)
    logging.debug(f"mapping_campanha sample: {list(mapping_campanha.items())[:5]}")
    logging.debug(f"mapping_sigla sample: {list(mapping_sigla.items())[:5]}")

    # Se for LinkedIn, carregamos também mapping_preview e mapping_criativo
    extra_kwargs = {}
    if plataforma == "linkedin":
        logging.info("Carregando mapping de preview (ID→PREVIEW) e criativo (utm_content→CRIATIVO) para LinkedIn...")
        client = get_google_client(creds_path)
        df_parametrizacao = read_sheet_as_dataframe_range(
            client,
            spreadsheet_id,
            sheet_name="BI_PARAMETRIZAÇÃO",
            range_str="A2:ZZ",  # ler de A2 em diante
            header_row_index=0   # a primeira linha do range (A2) vira cabeçalho
        )

        # 1) Normalizar as colunas da aba BI_PARAMETRIZAÇÃO
        df_parametrizacao.columns = normalize_columns(df_parametrizacao.columns)  # remove acentos, espaços etc.

        # 2) NEW: Normalizar TODOS os valores (linhas) do df_parametrizacao
        df_parametrizacao = normalize_parametrizacao_values(df_parametrizacao)
        logging.debug(f"df_parametrizacao shape: {df_parametrizacao.shape}")
        logging.debug(f"df_parametrizacao columns: {df_parametrizacao.columns}")
        df_parametrizacao.columns = normalize_columns(df_parametrizacao.columns)
        df_parametrizacao.rename(columns={'UTM_CONTENT': 'utm_content'}, inplace=True)

        df_parametrizacao = normalize_parametrizacao_values(df_parametrizacao)


        mapping_preview = construir_mapping_preview_parametrizacao(df_parametrizacao)
        logging.debug(f"mapping_preview (ID->PREVIEW) size: {len(mapping_preview)}")
        for k in list(mapping_preview.keys())[:5]:
            logging.debug(f"  preview: {k} => {mapping_preview[k]}")

        mapping_criativo = carregar_mapeamento_nome_creativo(df_parametrizacao)
        logging.debug(f"mapping_criativo (utm_content->CRIATIVO) size: {len(mapping_criativo)}")
        for k in list(mapping_criativo.keys())[:5]:
            logging.debug(f"  criativo: {k} => {mapping_criativo[k]}")

        extra_kwargs["mapping_preview"] = mapping_preview
        extra_kwargs["mapping_criativo"] = mapping_criativo
    else:
        # Se não for LinkedIn, ainda precisamos do client
        client = get_google_client(creds_path)

    logging.info(f"Buscando ID_Veiculo para '{veiculo_nome}' na aba SOURCE...")
    id_veiculo = get_id_veiculo_from_source(creds_path, spreadsheet_url, veiculo_nome)
    logging.debug(f"id_veiculo={id_veiculo}")

    etl_instance = etl_class(
        df=df_origin,
        id_veiculo=id_veiculo,
        veiculo=veiculo_nome,
        mapping_campanha=mapping_campanha,
        mapping_sigla=mapping_sigla,
        **extra_kwargs
    )

    logging.info(f"Lendo dados da aba de destino '{target_sheet}'...")
    df_target = read_sheet_as_dataframe_range(
        client,
        spreadsheet_id,
        sheet_name=target_sheet,
        range_str="A1:AM",  # definimos o range de colunas para o destino
        header_row_index=0   # A1 vira cabeçalho do df_target
    )
    logging.debug(f"df_target shape: {df_target.shape}")
    logging.debug(f"Colunas df_target: {df_target.columns}")

    logging.info(f"Executando ETL Geral para '{plataforma}'...")
    df_processed = etl_instance.processar(df_destino=df_target)

    logging.info(f"ETL finalizado: {df_processed.shape[0]} linhas tratadas.")
    logging.debug("Alguns registros de df_processed (ID_Content, Nome_do_Anuncio, Nome_do_Conjunto_de_Anuncio):")
    logging.debug(f"{df_processed[['ID_Content','Nome_do_Anuncio','Nome_do_Conjunto_de_Anuncio']].head(10)}")

    missing_records = get_missing_records(df_processed, df_target)
    if missing_records.empty:
        logging.info("Não há registros faltantes para inserir. Processo encerrado.")
    else:
        logging.info(f"Serão inseridos {missing_records.shape[0]} registros faltantes.")
        append_records_to_sheet(creds_path, spreadsheet_id, target_sheet, missing_records)

    logging.info(f"Processo de atualização para '{target_sheet}' concluído com sucesso.")


if __name__ == "__main__":
    main()
