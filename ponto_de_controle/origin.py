# ponto_de_controle/origin.py

from __future__ import annotations

import logging

import pandas as pd

from extract import read_df
from treat.utils.campos_calculados import add_key_creative, dedupe_by_key_creative
from ponto_de_controle.constants import ORIGIN_SHEET_ID, ORIGIN_TAB, MIN_DATE
from ponto_de_controle.debug import debug_shape

logger = logging.getLogger(__name__)


def read_origin_df() -> pd.DataFrame:
    """
    Lê a planilha de origem, gera `key_creative`, aplica deduplicação,
    filtra por data mínima e valida resultados.

    Retorna:
        pd.DataFrame: dataframe pronto para transformação/destino.
    """
    logger.info("Lendo origem %s › %s …", ORIGIN_SHEET_ID, ORIGIN_TAB)
    df = read_df(sheet_id=ORIGIN_SHEET_ID, tab=ORIGIN_TAB, header_row=0)

    # 1) Gera key_creative e deduplica
    df = add_key_creative(df)
    df = dedupe_by_key_creative(df)

    # 2) Filtra temporalmente
    df["date_dt"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df[df["date_dt"] >= MIN_DATE].copy()
    df.drop(columns=["date_dt"], inplace=True)

    # 3) Debug e validações
    debug_shape(df, name="df_origin (filtrado)")
    assert df["key_creative"].ne("").all(), "Encontrado key_creative vazio!"

    return df
