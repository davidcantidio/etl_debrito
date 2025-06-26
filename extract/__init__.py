"""
extract  ─ Camada de extração de dados Google Sheets
====================================================

Este módulo expõe:

- read_df : helper simples para ler UMA aba como pandas.DataFrame
- SheetsFetcher : classe avançada (batchGet, cache, write-back)

Uso rápido
----------

from extract import read_df
df = read_df(sheet_id="ID_DA_PLANILHA",
             tab="NOME_DA_ABA",
             header_row=0)

from extract import SheetsFetcher
fetcher = SheetsFetcher(
    spreadsheet_id="ID_DA_PLANILHA",
    creds_path="creds.json",
    header_row=0,
)
df2 = fetcher.get(["NOME_DA_ABA"])["NOME_DA_ABA"]
"""

from __future__ import annotations

import os
from typing import Any

import pandas as pd

from .sheets_fetcher import SheetsFetcher

__all__ = ["read_df", "SheetsFetcher"]


# --------------------------------------------------------------------------- #
# util interno                                                                #
# --------------------------------------------------------------------------- #
def _default_creds_path() -> str:
    """
    Retorna o caminho do arquivo de credenciais.

    Prioridade:
    1. variável de ambiente GOOGLE_CREDS_PATH
    2. arquivo 'creds.json' na raiz do projeto
    """
    return os.getenv("GOOGLE_CREDS_PATH", "creds.json")


# --------------------------------------------------------------------------- #
# API pública                                                                 #
# --------------------------------------------------------------------------- #
def read_df(
    *,
    sheet_id: str,
    tab: str,
    header_row: int = 0,
    creds_path: str | None = None,
) -> pd.DataFrame:
    """
    Lê **uma única aba** de um Google Sheets em formato DataFrame.

    Parâmetros
    ----------
    sheet_id : str
        ID da planilha (trecho entre `/d/` … `/edit` na URL).
    tab : str
        Nome exato da aba.
    header_row : int, default 0
        Linha (zero-based) que contém o cabeçalho.
        Ex.: cabeçalho na linha 5 → header_row=4.
    creds_path : str | None, default None
        Caminho do JSON da service account; se None usa _default_creds_path().

    Retorno
    -------
    pandas.DataFrame
        DataFrame com colunas rotuladas; células vazias preenchidas com "".
    """
    fetcher = SheetsFetcher(
        spreadsheet_id=sheet_id,
        creds_path=creds_path or _default_creds_path(),
        header_row=0,  # buscamos tudo, ajustamos depois
    )

    raw_df = fetcher.get([tab])[tab]
    if raw_df.empty:
        return raw_df.copy()

    # Se header_row é 0 basta retornar
    if header_row == 0:
        return raw_df.reset_index(drop=True)

    if header_row >= len(raw_df):
        raise ValueError(
            f"header_row={header_row} fora do range (aba {tab} tem {len(raw_df)} linhas)"
        )

    new_header = raw_df.iloc[header_row].tolist()
    body = raw_df.iloc[header_row + 1 :].copy()
    body.columns = new_header
    body.reset_index(drop=True, inplace=True)
    return body
