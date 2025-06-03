from __future__ import annotations

import datetime
import logging
from typing import Optional
from treat.utils.sheets_cache import get_worksheet

import pandas as pd

from treat.utils.write_back import write_back_df

log = logging.getLogger(__name__)

def _normalize_scalar(df: pd.DataFrame) -> pd.DataFrame:
    df_num   = df.select_dtypes("number").fillna(0)
    df_other = df.select_dtypes(exclude="number").fillna("")
    df = pd.concat([df_num, df_other], axis=1)[df.columns]  # preserva ordem
    return df.applymap(lambda v: v.isoformat() if isinstance(v, datetime.date) else v)

def write_back_origin(
    df_raw: pd.DataFrame,
    df_ok: pd.DataFrame,
    creds_path: str,
    spreadsheet_id: str,
    sheet_name: str,
    write_back: bool,
    dry_run: bool = False,
    a1_range: str = "A1",
    value_input_option: str = "RAW",
) -> pd.DataFrame:
    # Sanity-check: df_ok NÃO deve ter colunas novas
    extras = set(df_ok.columns) - set(df_raw.columns)
    if extras:
        raise ValueError(f"[write_back_origin] Colunas inesperadas: {sorted(extras)}")

    """
    Grava as correções em `df_ok` de volta na aba de origem,
    ajustando o tamanho da planilha para não ultrapassar limites.

    Retorna o DataFrame efetivamente gravado (ou apenas `df_ok` se dry_run).
    """
    # 1) Se write_back desligado, não grava nada
    if not write_back:
        log.info("🔸 Write-back de origem desativado para '%s'", sheet_name)
        return df_ok

    # 2) Prepara DataFrame para gravação
    #    Assume que df_ok já foi filtrado para conter somente colunas originais
    df_wb = df_ok.copy()
    n_linhas, n_colunas = df_wb.shape

    # 3) Dimensões esperadas (incluindo cabeçalho)
    desired_rows = n_linhas + 1  # +1 para o cabeçalho
    desired_cols = n_colunas

    # 4) Recupera a worksheet via cache
    ws = get_worksheet(creds_path, spreadsheet_id, sheet_name)

    # ——— Redimensionamento seguro da aba ———
    frozen = ws._properties.get("gridProperties", {}).get("frozenRowCount", 0)
    min_rows = max(frozen + 1, 2)

    # calcula tamanho necessário (inclui cabeçalho)
    desired_rows = max(desired_rows, min_rows)

    if ws.row_count != desired_rows or ws.col_count != desired_cols:
        ws.resize(rows=desired_rows, cols=desired_cols)
    # ————————————————————————————————

    # ————————————————————————————————

    # 5) Log de instrumentação: quantas linhas e colunas serão escritas
    total_cells = desired_rows * desired_cols
    log.info(
        "ℹ️  Preparando write-back origin para '%s': %d linhas × %d colunas (com cabeçalho) = %s células",
        sheet_name,
        n_linhas,
        n_colunas,
        f"{total_cells:,}",
    )

    # 6) Se dry_run, não grava de fato
    if dry_run:
        log.info("🔸 Dry-run ativo: não gravando '%s'", sheet_name)
        return df_wb

    # 7) Grava em batches (chunked dentro de write_back_df)
    write_back_df(
        df=df_wb,
        creds_path=creds_path,
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        a1_range=a1_range,
        value_input_option=value_input_option,
    )
    log.info("✅ Write-back concluído para '%s' (%d linhas × %d colunas)", sheet_name, n_linhas, n_colunas)

    return df_wb
