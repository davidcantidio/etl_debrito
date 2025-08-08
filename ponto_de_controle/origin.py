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
    
    Raises:
        ValueError: Se configurações obrigatórias estão ausentes
        RuntimeError: Se não há dados válidos após filtros
    """
    if not ORIGIN_SHEET_ID:
        raise ValueError("ORIGIN_SHEET_ID não configurado")
    
    if not ORIGIN_TAB:
        raise ValueError("ORIGIN_TAB não configurado")
    
    logger.info("Lendo origem %s › %s …", ORIGIN_SHEET_ID, ORIGIN_TAB)
    try:
        df = read_df(sheet_id=ORIGIN_SHEET_ID, tab=ORIGIN_TAB, header_row=0)
    except Exception as e:
        logger.error("Erro ao ler planilha de origem: %s", e)
        raise RuntimeError(f"Falha ao acessar planilha de origem: {e}") from e
    
    if df.empty:
        raise RuntimeError("Planilha de origem está vazia")

    # 1) Gera key_creative e deduplica
    df = add_key_creative(df)
    df = dedupe_by_key_creative(df)

    # 2) Filtra temporalmente
    if "date" not in df.columns:
        raise RuntimeError("Coluna 'date' não encontrada nos dados de origem")
    
    df["date_dt"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    valid_dates = df["date_dt"].notna().sum()
    
    if valid_dates == 0:
        raise RuntimeError("Nenhuma data válida encontrada na coluna 'date'")
    
    df = df[df["date_dt"] >= MIN_DATE].copy()
    df.drop(columns=["date_dt"], inplace=True)
    
    if df.empty:
        raise RuntimeError(f"Nenhum registro após filtro de data >= {MIN_DATE}")

    # 3) Debug e validações
    debug_shape(df, name="df_origin (filtrado)")
    
    empty_keys = df["key_creative"].eq("").sum()
    if empty_keys > 0:
        logger.warning("Encontrados %d registros com key_creative vazio", empty_keys)
        raise RuntimeError(f"Encontrados {empty_keys} key_creative vazios")

    logger.info("Origem processada com sucesso: %d registros válidos", len(df))
    return df
