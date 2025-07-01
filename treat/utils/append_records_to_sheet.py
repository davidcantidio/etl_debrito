# treat/utils/append.py

import logging
from typing import Optional

import pandas as pd
from gspread_dataframe import set_with_dataframe

from extract.sheets_fetcher import SheetsFetcher

log = logging.getLogger(__name__)


def append_records_to_sheet(
    fetcher: SheetsFetcher,
    spreadsheet_id: str,
    sheet_name: str,
    df: pd.DataFrame,
    *,
    include_header: Optional[bool] = None,
) -> None:
    """
    Insere os registros de `df` na aba `sheet_name`, usando apenas UMA chamada de leitura
    (via SheetsFetcher) e UMA chamada de escrita (batch_update). Depois redimensiona
    a aba com base nas dimensões combinadas de dados existentes + novos.

    Parâmetros:
    ----------
    fetcher : SheetsFetcher
        Instância pré-autenticada para acessar planilhas. Deve conter `spreadsheet_id`.
    spreadsheet_id : str
        ID da planilha (deve coincidir com o usado pelo fetcher).
    sheet_name : str
        Nome da aba onde serão inseridos os dados.
    df : pd.DataFrame
        DataFrame com os registros a serem inseridos.
    include_header : bool | None
        Se True, força incluir cabeçalho do DataFrame. Se False, nunca inclui. Se None (padrão),
        inclui header apenas se a aba estiver vazia (sem linhas).
    """
    # 1) Obtém dados existentes via SheetsFetcher (em memória)
    try:
        existing = fetcher.get([sheet_name])[sheet_name]
    except Exception as e:
        log.error(f"Falha ao ler aba '{sheet_name}' via SheetsFetcher: {e}")
        raise

    existing_rows = existing.shape[0]
    existing_cols = existing.shape[1] if existing_rows > 0 else 0

    # 2) Decide se inclui cabeçalho
    if include_header is None:
        include_header = existing_rows == 0

    # 3) Calcular a linha inicial (1-based)
    next_row = existing_rows + 1 if not include_header else 1

    # 4) Inserir DataFrame a partir da coluna B (col=1, index inicia em 1)
    #    - include_column_header controla inclusão do cabeçalho
    try:
        worksheet = fetcher.open_worksheet(sheet_name)
        set_with_dataframe(
            worksheet, df, row=next_row, col=1, include_column_header=include_header
        )
    except Exception as e:
        log.error(f"Falha ao escrever dados em '{sheet_name}': {e}")
        raise

    # 5) Calcular novas dimensões sem ler de novo:
    new_rows = df.shape[0]
    new_cols = df.shape[1] + (
        1 if include_header else 0
    )  # colunas do df, header não conta como coluna extra
    total_rows = (
        existing_rows + new_rows + (1 if include_header and existing_rows == 0 else 0)
    )
    total_cols = max(existing_cols, new_cols)

    # 6) Ajustar tamanho da planilha
    try:
        worksheet.resize(rows=total_rows, cols=total_cols)
        log.info(
            f"Inseridos {new_rows} registros em '{sheet_name}' a partir da linha {next_row}. "
            f"Planilha redimensionada para {total_rows} linhas e {total_cols} colunas."
        )
    except Exception as e:
        log.error(f"Falha ao redimensionar '{sheet_name}': {e}")
        raise
