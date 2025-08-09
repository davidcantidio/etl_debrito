import logging
from typing import Any
import pandas as pd

from ponto_de_controle.constants import DEST_SHEET_ID, DEST_TAB, HEAD_ROW_DEST, GOOGLE_CREDS_PATH
from transform.transform.utils.write_dataframe_to_sheet import write_dataframe_to_sheet
# Removido import circular - read_destination_df

logger = logging.getLogger(__name__)

def write_df_to_sheet_final(df_new: pd.DataFrame, *, dry_run: bool, dest_rows_count: int = None) -> None:
    """
    Grava df_new no Google Sheets se dry_run==False.
    A coluna __ID__ é removida antes do envio.
    
    Args:
        df_new: DataFrame com novas linhas a gravar
        dry_run: Se True, apenas simula a gravação
        dest_rows_count: Número de linhas já existentes no destino.
                        Se None, lê o destino para calcular.
    """
    total = len(df_new)
    if dry_run:
        logger.info("DRY-RUN: %d linhas seriam gravadas", total)
        return
    if total == 0:
        logger.info("Nada a gravar.")
        return

    # Calcula linha inicial para gravação
    if dest_rows_count is None:
        # Se não foi fornecido, lê o destino para calcular
        from ponto_de_controle.destination import read_destination_df
        df_dest = read_destination_df()
        dest_rows_count = len(df_dest)
    
    start_row = HEAD_ROW_DEST + 1 + dest_rows_count + 1

    creds = GOOGLE_CREDS_PATH.read_text(encoding="utf-8")
    write_dataframe_to_sheet(
        spreadsheet_id=DEST_SHEET_ID,
        sheet_name=DEST_TAB,
        df=df_new.drop(columns="__ID__", errors="ignore"),
        start_row=start_row,
        include_header=False,
        google_credentials_json=creds,
    )
    logger.info("Gravadas %d linhas na linha %d", total, start_row)
