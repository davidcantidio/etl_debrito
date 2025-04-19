# main.py

import argparse
import logging

logging.basicConfig(level=logging.INFO)

def run_etl_idade():
    from main.append_only_new_idade import run_etl_idade
    run_etl_idade()

def run_etl_genero():
    from main.append_only_new_genero import run_etl_genero
    run_etl_genero()

def run_etl_regiao():
    from main.append_only_new_regiao import run_etl_regiao
    run_etl_regiao()

def run_etl_alcance():
    from main.append_only_new_alcance import run_etl_alcance
    run_etl_alcance()

def run_etl_geral():
    from main.append_only_new_geral import run_etl_geral
    run_etl_geral()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Executa módulos ETL de forma isolada")
    parser.add_argument(
        "--modulo",
        choices=["idade", "genero", "regiao", "alcance", "geral", "todos"],
        required=True,
        help="Escolha qual ETL rodar (ou todos)",
    )

    args = parser.parse_args()

    if args.modulo == "idade":
        run_etl_idade()
    elif args.modulo == "genero":
        run_etl_genero()
    elif args.modulo == "regiao":
        run_etl_regiao()
    elif args.modulo == "alcance":
        run_etl_alcance()
    elif args.modulo == "geral":
        run_etl_geral()
    elif args.modulo == "todos":
        run_etl_geral()
        run_etl_idade()
        run_etl_genero()
        run_etl_regiao()
        run_etl_alcance()
