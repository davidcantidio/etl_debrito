from __future__ import annotations

import logging
from typing import Optional

import pandas as pd
import datetime

from treat.utils.write_back import write_back_df

log = logging.getLogger(__name__)


def write_back_origin(
    df_raw: pd.DataFrame,
    df_ok: pd.DataFrame,
    creds_path: str,
    spreadsheet_id: str,
    sheet_name: str,
    write_back: bool = True,
    dry_run: bool = False,
    a1_range: str = "A1",
    value_input_option: str = "USER_ENTERED",
) -> Optional[pd.DataFrame]:
    """
    Filtra df_ok para manter apenas colunas que existem em df_raw e
    grava de volta na aba de origem se write_back=True.
    Retorna o DataFrame enviado para o Sheets.

    :param df_raw: DataFrame original lido da aba de origem
    :param df_ok: DataFrame normalizado completo
    :param creds_path: caminho para credenciais do service account
    :param spreadsheet_id: ID da planilha Google Sheets
    :param sheet_name: nome da aba de origem
    :param write_back: habilita/desabilita a gravação
    :param dry_run: se True, apenas loga as ações sem chamar o Sheets API
    :param a1_range: range A1 para início da gravação
    :param value_input_option: modo de input para batch_update
    """
    if not write_back:
        log.info("Write-back desabilitado para '%s'", sheet_name)
        return None

    # 1) Intersecção de colunas, mantendo ordem do raw
    cols_raw = list(df_raw.columns)
    cols_to_write = [c for c in cols_raw if c in df_ok.columns]
    df_wb = df_ok[cols_to_write].copy()

    # 1.1) Remove colunas duplicadas (mantém a primeira ocorrência)
    if df_wb.columns.duplicated().any():
        dup_cols = df_wb.columns[df_wb.columns.duplicated()].unique().tolist()
        log.warning("Colunas duplicadas detectadas – mantendo primeira ocorrência: %s", dup_cols)
        df_wb = df_wb.loc[:, ~df_wb.columns.duplicated(keep="first")]

    # 1.2) Garantir que cada célula seja um escalar simples
    def _to_scalar(v):
        """Converte Series/list para escalar, e limpa repr de Series."""
        if isinstance(v, pd.Series):
            return v.iat[0] if not v.empty else ""
        if isinstance(v, (list, tuple)):
            return v[0] if v else ""
        if isinstance(v, str) and "dtype:" in v and "\n" in v:
            first_line = v.split("\n")[0]
            tokens = first_line.split()
            return tokens[-1] if tokens else first_line.strip()
        return v

    df_wb = df_wb.applymap(_to_scalar)

    # 1) Primeiro: números vazios viram 0
    num_cols = df_wb.select_dtypes(include="number").columns
    df_wb[num_cols] = df_wb[num_cols].fillna(0)

    # 2) Depois: para qualquer campo não numérico, “limpa” para string vazia
    df_wb = df_wb.fillna("")

    def _date_to_iso(v):
        return v.isoformat() if isinstance(v, datetime.date) else v
    

    df_wb = df_wb.applymap(_date_to_iso)


    log.info(
        "Preparando write-back: %d linhas, colunas: %s",
        len(df_wb), cols_to_write,
    )

    if dry_run:
        log.info("[Dry run] Não foi efetuado batch_update em '%s'", sheet_name)
        return df_wb

    # 2) Executar o write-back
    write_back_df(
        df=df_wb,
        creds_path=creds_path,
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        a1_range=a1_range,
        value_input_option=value_input_option,
    )

    log.info("✅ Write-back concluído (%d linhas) em '%s'", len(df_wb), sheet_name)
    return df_wb
