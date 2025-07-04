from typing import Tuple
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def diff_new_rows(src: pd.DataFrame, dst: pd.DataFrame) -> pd.DataFrame:
    """
    Retorna registros de `src` cujo __ID__ não existe em `dst`.
    """
    assert "__ID__" in src.columns, "Coluna '__ID__' ausente em src"
    assert "__ID__" in dst.columns, "Coluna '__ID__' ausente em dst"

    mask = ~src["__ID__"].isin(dst["__ID__"])
    df_new = src.loc[mask].copy()

    logger.info("Diff: src=%d, dst=%d, new=%d", len(src), len(dst), len(df_new))
    return df_new
