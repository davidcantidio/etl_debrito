import logging
import sys
import pandas as pd
from utils.google_sheets import carregar_aba_google_sheets
from utils.setup_logging import setup_logging
from utils.get_google_client import get_google_client
from utils.append_records_to_sheet import append_records_to_sheet

def main():
    setup_logging(level=logging.INFO)
    
    # Configurações
    creds_path = "creds.json"
    spreadsheet_id = "1DazUQxspLgT0utOFHcTINbFngXw7Fq0LOq6v4lRGixg"
    spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit"
    
    # Abas de origem e destino
    source_sheet = "parametrizar"
    target_sheet = "CONTEÚDO _MÍDIA"  # Cabeçalhos na linha 2
    
    logging.info(f"Lendo dados da aba de origem '{source_sheet}'...")
    df_origem = carregar_aba_google_sheets(creds_path, spreadsheet_url, source_sheet)
    
    if df_origem.empty:
        logging.warning(f"Aba '{source_sheet}' está vazia. Encerrando processo.")
        sys.exit(0)
    
    # Remove linhas completamente vazias
    df_origem = df_origem.dropna(how='all')
    
    logging.info("Colunas lidas da aba de origem: " + str(df_origem.columns.tolist()))
    
    # Lista de colunas de destino na ordem desejada
    dest_cols = [
        "Inicio", "Fim", "Veiculo", "Categoria", "Campanha", "Remarketing/Ação",
        "Objetivo de Mídia", "Região", "Tipo de compra", "Link das Redes", "Segmentação",
        "Responsável", "Data de preechimento", "TAXONOMIA - Campanha",
        "TAXONOMIA - \"CONJUNTO DE ANÚNCIO\"", "TAXONOMIA - \"CRIATIVO\"",
        "URL - Parametrizada", "Status", "Editoria", "Nome criativo", "Formato",
        "Conteúdo", "CTA", "Título peça", "Legenda peça", "URL de destino", "Peça na rede"
    ]
    
    # Se a aba de origem não possuir alguma coluna de destino, cria com valor vazio
    for col in dest_cols:
        if col not in df_origem.columns:
            logging.warning(f"Coluna '{col}' não encontrada na aba de origem. Será criada vazia.")
            df_origem[col] = ""
    
    # Reorganiza o DataFrame com base na ordem de destino
    df_destino_ord = df_origem[dest_cols]
    
    logging.info("Exibindo as 5 primeiras linhas após reorganização:")
    logging.info(df_destino_ord.head().to_string())
    
    # Obtém o cliente do Google e faz a escrita na aba de destino (append)
    client = get_google_client(creds_path)
    logging.info(f"Adicionando dados na aba '{target_sheet}'...")
    append_records_to_sheet(creds_path, spreadsheet_id, target_sheet, df_destino_ord)
    
    logging.info(f"Processo de atualização para '{target_sheet}' concluído com sucesso.")

if __name__ == "__main__":
    main()
