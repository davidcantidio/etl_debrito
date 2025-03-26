import logging
import pandas as pd
from utils.get_google_client import get_google_client

def get_campaign_parameterization(creds_path, spreadsheet_id):
    """
    Lê a aba 'BI_PARAMETRIZAÇÃO' e retorna dois dicionários de mapeamento:
      - mapping_campanha: mapeia o valor da coluna de lookup "NOME CAMPANHA" para o valor desejado para a coluna "Campanha"
      - mapping_sigla: mapeia o valor da coluna de lookup "NOME CAMPANHA" para o valor desejado para a coluna "ID_Campanha"
    
    Considera que:
      - Os cabeçalhos reais estão na linha 2 e os dados começam na linha 3.
      - A coluna de lookup é "NOME CAMPANHA".
      - Para mapeamento, busca-se a coluna que contenha "NOME CAMPANHA" (para Campanha) e a coluna que contenha "SIGLA".
    
    Parâmetros:
        creds_path (str): Caminho para o arquivo de credenciais.
        spreadsheet_id (str): ID da planilha.
    
    Retorna:
        tuple: (mapping_campanha, mapping_sigla) – dois dicionários com os mapeamentos.
    """
    client = get_google_client(creds_path)
    sh = client.open_by_key(spreadsheet_id)
    worksheet = sh.worksheet("BI_PARAMETRIZAÇÃO")
    data = worksheet.get_all_values()
    
    if len(data) < 3:
        raise ValueError("A aba 'BI_PARAMETRIZAÇÃO' não tem dados suficientes (cabeçalho na linha 2 e dados a partir da linha 3).")
    
    # Normaliza os cabeçalhos reais (linha 2)
    raw_headers = data[1]
    headers = [col.strip().replace('\n', ' ').replace('"', '').upper() for col in raw_headers]
    logging.info(f"Colunas reais detectadas (linha 2): {headers}")
    
    df_param = pd.DataFrame(data[2:], columns=headers)
    
    lookup = "NOME CAMPANHA"
    if lookup not in df_param.columns:
        raise ValueError(f"A coluna de lookup '{lookup}' não foi encontrada na aba BI_PARAMETRIZAÇÃO.")
    
    # Define a coluna para Campanha: busca aquela que contenha "NOME CAMPANHA"
    col_dest_campanha = next((col for col in df_param.columns if "NOME CAMPANHA" in col), lookup)
    # Define a coluna para SIGLA: busca aquela que contenha "SIGLA"
    col_sigla = next((col for col in df_param.columns if "SIGLA" in col), None)
    
    if not col_dest_campanha or not col_sigla:
        raise ValueError("As colunas 'NOME CAMPANHA' e/ou 'SIGLA' não foram encontradas após normalização.")
    
    logging.info(f"Coluna de lookup: '{lookup}'")
    logging.info(f"Coluna usada para Campanha (destino): '{col_dest_campanha}'")
    logging.info(f"Coluna usada para SIGLA: '{col_sigla}'")
    
    mapping_campanha = {
        str(k).strip().upper(): str(v).strip()
        for k, v in zip(df_param[lookup], df_param[col_dest_campanha])
    }
    mapping_sigla = {
        str(k).strip().upper(): str(v).strip()
        for k, v in zip(df_param[lookup], df_param[col_sigla])
    }
    
    logging.info(f"Total de campanhas mapeadas: {len(mapping_campanha)}")
    logging.info(f"Exemplos de chaves no mapping (Campanha): {list(mapping_campanha.keys())[:10]}")
    
    return mapping_campanha, mapping_sigla
