from __future__ import annotations

import datetime
import logging
from typing import Optional

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
    *,
    write_back: bool = True,
    dry_run: bool = False,
    a1_range: str = "A1",
    value_input_option: str = "USER_ENTERED",
) -> Optional[pd.DataFrame]:
    """
    Filtra `df_ok` para manter apenas as colunas de `df_raw` e grava de volta na aba de origem.

    :param df_raw: DataFrame original lido da aba de origem
    :param df_ok: DataFrame normalizado completo
    :param creds_path: caminho para credenciais do service account
    :param spreadsheet_id: ID da planilha Google Sheets
    :param sheet_name: nome da aba de origem
    :param write_back: habilita/desabilita a gravação
    :param dry_run: se True, apenas loga as ações sem chamar a API
    :param a1_range: célula inicial para gravação (ex.: "A1")
    :param value_input_option: modo de input para batch_update

    :return: DataFrame gravado ou None se write_back=False
    """
    if not write_back:
        log.info("Write-back desabilitado para '%s'", sheet_name)
        return None

    # 1) Intersecção de colunas na ordem original de df_raw
    cols_raw = list(df_raw.columns)
    cols_to_write = [c for c in cols_raw if c in df_ok.columns]
    df_wb = df_ok[cols_to_write].copy()

    # 2) Remove colunas duplicadas, mantendo a primeira
    if df_wb.columns.duplicated().any():
        dup = df_wb.columns[df_wb.columns.duplicated()].unique().tolist()
        log.warning(
            "Colunas duplicadas encontradas; mantendo 1ª ocorrência: %s", dup
        )
        df_wb = df_wb.loc[:, ~df_wb.columns.duplicated(keep="first")]

    # 3) Normalização básica de valores
    df_wb = _normalize_scalar(df_wb)

    log.info(
        "Write-back '%s': %d linhas, %d colunas", sheet_name, len(df_wb), df_wb.shape[1]
    )

    if dry_run:
        log.info("[dry-run] Nenhum batch_update enviado para '%s'", sheet_name)
        return df_wb

    # 4) Executa o write-back via cache de worksheet
    write_back_df(
        df=df_wb,
        creds_path=creds_path,
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        a1_range=a1_range,
        value_input_option=value_input_option,
    )
    log.info("✅ Write-back concluído para '%s'", sheet_name)
    return df_wb
