import logging
from utils.setup_logging import setup_logging
from main.append_only_new_geral import run_etl_geral
# from main.append_only_new_idade import run_etl_idade
# from main.append_only_new_regiao import run_etl_regiao

def main():
    # Configura o logging para DEBUG (ou outro nível desejado)
    setup_logging(level=logging.DEBUG)
    logging.info("Iniciando execução centralizada de ETLs.")

    # Executa o ETL Geral
    logging.info("Executando ETL Geral...")
    run_etl_geral()

    # # Executa o ETL de Idade
    # logging.info("Executando ETL Idade...")
    # run_etl_idade()

    # # Executa o ETL de Região
    # logging.info("Executando ETL Região...")
    # run_etl_regiao()

    logging.info("Todos os ETLs foram executados com sucesso.")

if __name__ == "__main__":
    main()
