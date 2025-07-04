from extract import read_df
import pandas as pd
import logging
from typing import Iterable

from ponto_de_controle.constants import DEST_SHEET_ID, DEST_TAB, HEAD_ROW_DEST
from ponto_de_controle.constants import DEST_COLUMNS
from treat.utils.campos_calculados import make_id_ponto_de_controle

logger = logging.getLogger(__name__)

def read_destination_df() -> pd.DataFrame:
    """
    Lê a planilha de destino, resolve colunas duplicadas,
    reindexa em DEST_COLUMNS e devolve DataFrame único por __ID__.
    """
    logger.info("Lendo destino %s › %s …", DEST_SHEET_ID, DEST_TAB)
    df = read_df(sheet_id=DEST_SHEET_ID, tab=DEST_TAB, header_row=HEAD_ROW_DEST)

    # renomeia colunas duplicadas deterministicamente
    if df.columns.duplicated().any():
        seen = {}
        new_cols = []
        for col in df.columns:
            count = seen.get(col, 0)
            new_cols.append(col if count == 0 else f"{col}.{count}")
            seen[col] = count + 1
        df.columns = new_cols
        logger.warning("Cabeçalhos duplicados renomeados: %s", df.columns.tolist())

    # reindexa e preenche faltantes
    df = df.reindex(columns=DEST_COLUMNS, fill_value="")

    # gera __ID__ e deduplica
    df["__ID__"] = df.apply(make_id_ponto_de_controle, axis=1, columns=DEST_COLUMNS)
    before = len(df)
    df = df.drop_duplicates("__ID__", keep="first").reset_index(drop=True)
    logger.info("Destino: %d → %d linhas únicas", before, len(df))

    return df
