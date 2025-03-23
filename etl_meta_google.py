import os
import logging
from utils.google_sheets import carregar_aba_google_sheets
from scripts.ELET_etl_geral_meta import etl_geral_meta

def main():
    # Configuração de logging
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    # Parâmetros
    creds_path = "creds.json"
    sheet_url = "https://docs.google.com/spreadsheets/d/1DazUQxspLgT0utOFHcTINbFngXw7Fq0LOq6v4lRGixg/edit"
    aba = "Meta - Geral"
    output_path = "data/meta_output.json"

    try:
        logging.info("Lendo dados do Google Sheets...")
        df_meta = carregar_aba_google_sheets(creds_path, sheet_url, aba)
        logging.info(f"Dados carregados: {df_meta.shape[0]} linhas")
    except Exception as e:
        logging.error("Erro ao ler dados do Google Sheets: %s", e)
        raise

    try:
        logging.info("Aplicando ETL Meta...")
        etl = etl_geral_meta(df_meta)
        df_tratado = etl.processar()
        logging.info(f"ETL finalizado: {df_tratado.shape[0]} linhas tratadas")

        # (Opcional) Se quiser inspecionar IDs gerados, caso exista coluna "ID":
        if "ID" in df_tratado.columns:
            unique_ids = df_tratado["ID"].dropna().unique().tolist()
            logging.info(f"Alguns IDs (máximo 10): {unique_ids[:10]}")
            logging.info(f"Total de IDs únicos: {len(unique_ids)}")

    except Exception as e:
        logging.error("Erro durante o processamento ETL: %s", e)
        raise

    try:
        logging.info(f"Salvando JSON em: {output_path}")
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df_tratado.to_json(output_path, orient="records", indent=2, force_ascii=False)
        logging.info("Processo concluído com sucesso.")
    except Exception as e:
        logging.error("Erro ao salvar o JSON: %s", e)
        raise

if __name__ == "__main__":
    main()
