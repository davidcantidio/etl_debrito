import argparse
import logging

from ponto_de_controle.origin import read_origin_df
from ponto_de_controle.transform import transform_df
from ponto_de_controle.destination import read_destination_df
from ponto_de_controle.diff import diff_new_rows
from ponto_de_controle.witter import write_df_to_sheet_final

logger = logging.getLogger(__name__)

def main(*, dry_run: bool) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    logger.info("── Início do pipeline ──")

    df_origin = read_origin_df()
    df_transf = transform_df(df_origin)
    df_dest = read_destination_df()
    df_new = diff_new_rows(df_transf, df_dest)

    logger.info("▶ Total de linhas a gravar: %d", len(df_new))
    # em notebook cairá no exceptImport
    try:
        from IPython.display import display  # type: ignore
        display(df_new.drop(columns="__ID__", errors="ignore"))
    except ImportError:
        print(df_new.drop(columns="__ID__", errors="ignore"))

    write_df_to_sheet_final(df_new, dry_run=dry_run)
    logger.info("── Fim ──")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Atualiza ponto de controle")
    parser.add_argument("--dry-run", action="store_true", help="não grava no sheet")
    args = parser.parse_args()
    main(dry_run=args.dry_run)
