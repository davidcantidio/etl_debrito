"""
extract  ── Camada de extração de dados Google Sheets
=====================================================

Este módulo expõe:

- read_df: helper simples para leitura de uma aba como DataFrame.
- SheetsFetcher: classe avançada para batchGet, cache e write-back.

Uso rápido
----------

from extract import read_df
df = read_df(sheet_id="ID_DA_PLANILHA", tab="NOME_DA_ABA", header_row=0)

from extract import SheetsFetcher
fetcher = SheetsFetcher(
    spreadsheet_id="ID_DA_PLANILHA",
    creds_path="caminho/para/creds.json",
    header_row=0
)
df2 = fetcher.get(["NOME_DA_ABA"])["NOME_DA_ABA"]
"""

from __future__ import annotations

import os
from typing import Any

import pandas as pd

from .sheets_fetcher import SheetsFetcher


__all__ = ["read_df", "SheetsFetcher"]


def _default_creds_path() -> str:
    """
    Retorna o caminho do arquivo de credenciais de serviço.

    Usa a variável de ambiente GOOGLE_CREDS_PATH, se definida;
    caso contrário, retorna 'creds.json'.
    """
    return os.getenv("GOOGLE_CREDS_PATH", "creds.json")


def read_df(
    *,
    sheet_id: str,
    tab: str,
    header_row: int = 0,
    creds_path: str | None = None,
) -> pd.DataFrame:
    """
    Lê uma única aba de um Google Sheets e devolve como pandas.DataFrame.

    Parâmetros
    ----------
    sheet_id : str
        ID da planilha (parte entre /d/ e /edit na URL).
    tab : str
        Nome da aba a ser lida.
    header_row : int, optional
        Índice zero-based da linha de cabeçalho no Sheets.
        Ex.: se o cabeçalho está na linha 5, passe header_row=4.
        (default: 0)
    creds_path : str | None, optional
        Caminho para o arquivo JSON de credenciais de serviço.
        Se None, usa _default_creds_path(). (default: None)

    Retorno
    -------
    pd.DataFrame
        DataFrame com o conteúdo da aba. Linhas/colunas faltantes são
        preenchidas com "" para manter retangularidade.
    """
    creds_file = creds_path or _default_creds_path()
    fetcher = SheetsFetcher(
        spreadsheet_id=sheet_id,
        creds_path=creds_file,
        header_row=header_row,
    )
    data = fetcher.get([tab])
    return data.get(tab, pd.DataFrame())
