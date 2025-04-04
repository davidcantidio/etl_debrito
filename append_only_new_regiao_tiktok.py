import logging
import sys
from utils.google_sheets import carregar_aba_google_sheets
from utils.setup_logging import setup_logging
from utils.get_campaign_parameterization import get_campaign_parameterization
from utils.read_sheet_as_dataframe import read_sheet_as_dataframe
from utils.get_missing_records import get_missing_records
from utils.append_records_to_sheet import append_records_to_sheet
from utils.get_google_client import get_google_client
from utils.geolocalizacao import carregar_caches_padrao

# Supondo que você tenha a classe 'RegiaoETL' (subclasse de 'BaseETL') em "scripts/etl_regiao.py"
from scripts.etl_regiao import RegiaoETL

def get_id_veiculo_from_source(creds_path, spreadsheet_url, nome_veiculo):
    df_source = carregar_aba_google_sheets(creds_path, spreadsheet_url, "SOURCE")
    filtro = df_source['Descrição da Mídia'].str.strip().str.lower() == nome_veiculo.lower()
    id_val = df_source.loc[filtro, 'ID_Veiculo']
    if not id_val.empty:
        return int(id_val.values[0])
    raise ValueError(f"ID_Veiculo para '{nome_veiculo}' nao encontrado na aba SOURCE")


def main():
    setup_logging(level=logging.INFO)

    creds_path = "creds.json"
    spreadsheet_id = "1DazUQxspLgT0utOFHcTINbFngXw7Fq0LOq6v4lRGixg"
    spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit"

    # Exemplo: "Tiktok Regiao", "Meta Regiao", "Linkedin Regiao" etc.
    # Se quiser, pode ler esse valor de sys.argv ou input() para facilitar testes
    source_sheet = "tiktokRegiao"
    target_sheet = "modeloRegiao"
    veiculo_nome = "Tiktok"

    # Extrai a plataforma do nome da aba (ex.: "Tiktok" ou "Meta")
    # Se houver mais palavras, pega a primeira
    plataforma = source_sheet.split()[0]

    logging.info(f"Lendo dados da aba de origem '{source_sheet}'...")
    df_origin = carregar_aba_google_sheets(creds_path, spreadsheet_url, source_sheet)
    if "Date" in df_origin.columns:
        df_origin = df_origin[df_origin["Date"].astype(str).str.strip() != ""]

    logging.info("Carregando mapeamentos de campanha...")
    mapping_campanha, mapping_sigla = get_campaign_parameterization(creds_path, spreadsheet_id)

    logging.info("Carregando caches de geolocalizacao...")
    cache_estados, cache_municipios = carregar_caches_padrao()

    logging.info(f"Buscando ID_Veiculo da aba SOURCE para '{plataforma}'...")
    id_veiculo = get_id_veiculo_from_source(creds_path, spreadsheet_url, veiculo_nome)

    logging.info(f"Executando ETL de Região para '{plataforma}'...")
    etl_instance = RegiaoETL(
        df=df_origin,
        id_veiculo=id_veiculo,
        veiculo=plataforma,
        mapping_campanha=mapping_campanha,  # nome correto
        mapping_sigla=mapping_sigla,        # nome correto
        cache_estados=cache_estados,
        cache_municipios=cache_municipios
    )


    df_processed = etl_instance.processar()

    logging.info(f"ETL finalizado: {df_processed.shape[0]} linhas tratadas.")

    client = get_google_client(creds_path)
    logging.info(f"Lendo dados da aba de destino '{target_sheet}'...")
    df_target = read_sheet_as_dataframe(client, spreadsheet_id, target_sheet, offset_col=0)
    logging.info(f"Aba de destino '{target_sheet}' contem {df_target.shape[0]} linhas.")

    missing_records = get_missing_records(df_processed, df_target)
    if missing_records.empty:
        logging.info("Nao ha registros faltantes para inserir. Processo encerrado.")
    else:
        logging.info(f"Serao inseridos {missing_records.shape[0]} registros faltantes.")
        append_records_to_sheet(creds_path, spreadsheet_id, target_sheet, missing_records)

    logging.info(f"Processo de atualizacao para '{target_sheet}' concluido com sucesso.")

    # Se tiver um método para exibir a correspondência de 'Province name' → Estado
    if hasattr(etl_instance, "exibir_correspondencia_regiao"):
        print("\n--- Dicionario de correspondencia Regiao ---")
        etl_instance.exibir_correspondencia_regiao()

if __name__ == "__main__":
    main()
