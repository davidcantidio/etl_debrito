import os
import logging
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from scripts.ELET_etl_alcance_meta import etl_alcance_meta
from utils.google_sheets import carregar_aba_google_sheets

def ler_dataframe_aba(creds_path, spreadsheet_id, aba_name, offset_col=0):
    """
    Lê a aba 'aba_name' da planilha e retorna um DataFrame limpo.
    Remove linhas vazias e, se existir, linhas com a coluna 'ID' vazia.
    """
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    client = gspread.authorize(creds)
    sh = client.open_by_key(spreadsheet_id)
    worksheet = sh.worksheet(aba_name)
    
    data = worksheet.get_all_values()
    if not data:
        return pd.DataFrame()
    
    df = pd.DataFrame(data[1:], columns=data[0])
    if offset_col > 0 and df.columns[0].strip() == "":
        df = df.iloc[:, offset_col:]
    df = df.dropna(how="all")

    # Remove linhas em que 'ID' está vazio, se 'ID' existir
    if "ID" in df.columns:
        df["ID"] = df["ID"].astype(str)
        df = df[df["ID"].str.strip() != ""]
    return df

def identificar_registros_faltantes(df_origem, df_destino):
    """
    Compara os DataFrames de origem e destino usando a coluna 'ID' e uma contagem cumulativa para cada ocorrência.
    Retorna as linhas da origem que estão faltando no destino.
    """
    df_origem = df_origem.copy()
    df_destino = df_destino.copy()
    
    if "ID" not in df_destino.columns or df_destino.empty:
        return df_origem.copy()
    
    df_origem["ID_index"] = df_origem.groupby("ID").cumcount() + 1
    df_destino["ID_index"] = df_destino.groupby("ID").cumcount() + 1
    
    merged = pd.merge(df_origem, df_destino[["ID", "ID_index"]],
                      on=["ID", "ID_index"], how="left", indicator=True)
    faltantes = merged[merged["_merge"] == "left_only"].drop(columns=["_merge", "ID_index"])
    return faltantes

def carregar_parametrizacao_campanhas(creds_path, spreadsheet_id):
    """
    Lê a aba 'BI_PARAMETRIZAÇÃO' e retorna dois dicionários:
      - mapping_campanha: map. "NOME CAMPANHA" → "Campanha"
      - mapping_sigla: map. "NOME CAMPANHA" → "ID_Campanha"
    Considera cabeçalhos na linha 2 e dados a partir da linha 3.
    """
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    client = gspread.authorize(creds)
    
    worksheet = client.open_by_key(spreadsheet_id).worksheet("BI_PARAMETRIZAÇÃO")
    data = worksheet.get_all_values()
    if len(data) < 3:
        raise ValueError("A aba 'BI_PARAMETRIZAÇÃO' não tem dados suficientes (cabeçalho na linha 2 e dados na linha 3).")
    
    raw_headers = data[1]
    headers = [col.strip().replace('\n',' ').replace('"','').upper() for col in raw_headers]
    logging.info(f"Colunas reais detectadas (linha 2): {headers}")
    
    df_param = pd.DataFrame(data[2:], columns=headers)
    
    lookup = "NOME CAMPANHA"
    if lookup not in df_param.columns:
        raise ValueError(f"A coluna de lookup '{lookup}' não foi encontrada na aba BI_PARAMETRIZAÇÃO.")
    
    # Coluna para "Campanha" (destino)
    col_dest_campanha = next((col for col in df_param.columns if "CAMPANHA OBRIGATÓRIO" in col), lookup)
    # Coluna para "SIGLA"
    col_sigla = next((col for col in df_param.columns if "SIGLA" in col), None)
    
    if not col_dest_campanha or not col_sigla:
        raise ValueError("As colunas 'CAMPANHA OBRIGATÓRIO' e/ou 'SIGLA' não foram encontradas.")
    
    logging.info(f"Coluna de lookup: '{lookup}'")
    logging.info(f"Coluna de destino p/ Campanha: '{col_dest_campanha}'")
    logging.info(f"Coluna usada p/ SIGLA: '{col_sigla}'")
    
    mapping_campanha = {
        str(k).strip().upper(): str(v).strip()
        for k, v in zip(df_param[lookup], df_param[col_dest_campanha])
    }
    mapping_sigla = {
        str(k).strip().upper(): str(v).strip()
        for k, v in zip(df_param[lookup], df_param[col_sigla])
    }
    
    logging.info(f"Total de campanhas mapeadas: {len(mapping_campanha)}")
    logging.info(f"Exemplos chaves no mapping (Campanha): {list(mapping_campanha.keys())[:10]}")
    return mapping_campanha, mapping_sigla

def append_new_records_by_id(creds_path, spreadsheet_id, aba_destino, df_origem_etl):
    """
    Compara registros do DataFrame de origem com os da aba de destino usando 'ID'.
    Insere registros faltantes no destino, iniciando na coluna B (col=2).
    """
    # Não removemos mais "Numero" e "Data", pois queremos mantê-los no destino
    # Aplica apenas a filtragem de Data se estiver vazia
    if "Data" in df_origem_etl.columns:
        df_origem_etl["Data"] = df_origem_etl["Data"].astype(str)
        # Filtra datas vazias
        df_origem_etl = df_origem_etl[df_origem_etl["Data"].str.strip() != ""]

    # Lê a aba de destino, assumindo offset_col=1 se os dados do destino começam na coluna B
    df_destino = ler_dataframe_aba(creds_path, spreadsheet_id, aba_destino, offset_col=1)
    logging.info(f"Dados atuais na aba destino (após limpeza): {df_destino.shape[0]} linhas.")
    
    faltantes = identificar_registros_faltantes(df_origem_etl, df_destino)
    if faltantes.empty:
        logging.info("Não há registros faltantes p/ inserir. Processo encerrado.")
        return
    
    logging.info(f"Foram identificados {faltantes.shape[0]} registros faltantes para inserir (baseado em 'ID').")
    
    # Determina a próxima linha disponível e se deve incluir cabeçalho
    if df_destino.empty:
        next_row = 1
        include_header = True
    else:
        next_row = df_destino.shape[0] + 1
        include_header = False

    # Autentica e atualiza a planilha
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    client = gspread.authorize(creds)
    sh = client.open_by_key(spreadsheet_id)
    worksheet = sh.worksheet(aba_destino)
    
    set_with_dataframe(
        worksheet,
        faltantes,
        row=next_row,
        col=2,  # a partir da coluna B
        include_column_header=include_header
    )
    
    # Redimensiona a planilha
    data = worksheet.get_all_values()
    total_rows = len(data)
    total_cols = max(len(row) for row in data) if data else 0
    worksheet.resize(rows=total_rows, cols=total_cols)
    
    logging.info(f"Inseridos {faltantes.shape[0]} registros na aba '{aba_destino}' "
                 f"a partir da linha {next_row}, col B.")
    logging.info(f"Planilha redimensionada p/ {total_rows} linhas e {total_cols} colunas.")

def main():
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    creds_path = "creds.json"
    spreadsheet_id = "1DazUQxspLgT0utOFHcTINbFngXw7Fq0LOq6v4lRGixg"
    
    fonte_aba = "Meta - Alcance"
    aba_destino = "modeloAlcance"
    
    logging.info(f"Lendo dados da aba de origem '{fonte_aba}'...")
    df_origem = carregar_aba_google_sheets(creds_path,
                                           f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit",
                                           fonte_aba)
    if "Date" in df_origem.columns:
        df_origem = df_origem[df_origem["Date"].astype(str).str.strip() != ""]
    logging.info(f"Dados carregados da origem (com 'Date' preenchido): {df_origem.shape[0]} linhas.")
    
    # Carrega mapeamento
    logging.info("Carregando parametrização de campanhas (p/ 'Campanha' e 'ID_Campanha')...")
    mapping_campanha, mapping_sigla = carregar_parametrizacao_campanhas(creds_path, spreadsheet_id)
    
    logging.info("Aplicando ETL de Alcance Meta com mapeamento externo...")
    from scripts.ELET_etl_alcance_meta import etl_alcance_meta
    etl = etl_alcance_meta(df_origem, mapping_campanha, mapping_sigla)
    df_tratado = etl.processar()
    logging.info(f"ETL finalizado: {df_tratado.shape[0]} linhas tratadas.")
    
    if "ID" in df_tratado.columns:
        origem_ids = df_tratado["ID"].dropna().astype(str).str.strip().unique()
        logging.info(f"IDs únicos na origem (ETL): {list(origem_ids)[:10]} ... Total: {len(origem_ids)}")
    
    append_new_records_by_id(creds_path, spreadsheet_id, aba_destino, df_tratado)
    
    logging.info("Processo de atualização para modeloAlcance concluído com sucesso.")

if __name__ == "__main__":
    main()
