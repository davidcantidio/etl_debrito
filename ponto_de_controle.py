"""Pipeline de atualização do Ponto de Controle.

# HOW TO RUN
-------------
1. ``poetry install``
2. ``python ponto_de_controle.py --dry-run``
"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import date
from pathlib import Path
from typing import Iterable

import pandas as pd
from dotenv import load_dotenv

from extract import read_df
from treat.utils.campos_calculados import (
    DEFAULT_DEST_COLUMNS,
    add_key_creative,
    dedupe_by_key_creative,
    make_id_ponto_de_controle,
)
from treat.utils.datas import concat_period, normalize_date_to_str_DD_M_YYYY
from treat.utils.normalize import normalize_vehicle
from treat.utils.write_dataframe_to_sheet import write_dataframe_to_sheet


logger = logging.getLogger(__name__)


def load_google_creds() -> str:
    """Carrega o JSON de credenciais do caminho indicado em ``GOOGLE_CREDS_PATH``."""

    path = Path(os.getenv("GOOGLE_CREDS_PATH", "creds.json"))
    return path.read_text(encoding="utf-8")


DEST_COLUMNS: Iterable[str] = DEFAULT_DEST_COLUMNS
MIN_DATE = date(2025, 6, 1)


def read_origin_df() -> pd.DataFrame:
    """Lê a planilha de origem e aplica deduplicação de criativos."""

    sheet_id = os.environ["ORIGIN_SHEET_ID"]
    tab = os.environ["ORIGIN_TAB"]
    df = read_df(sheet_id=sheet_id, tab=tab, header_row=0)
    df = add_key_creative(df)
    df = dedupe_by_key_creative(df)
    df["date_dt"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df[df["date_dt"] >= MIN_DATE].copy()
    df.drop(columns=["date_dt"], inplace=True)
    return df


def transform_df(df: pd.DataFrame) -> pd.DataFrame:
    """Transforma ``df`` nas colunas de destino e gera ``__ID__``."""

    df2 = df.copy()
    df2["Data"] = df2["start"].apply(normalize_date_to_str_DD_M_YYYY)
    df2["Periodo"] = df2.apply(lambda r: concat_period(r["start"], r["end"]), axis=1)
    df2["Veiculo"] = df2["Veiculo"].apply(normalize_vehicle)
    df2["Link conteúdos impulsionados"] = df2.get("URL_do_Anuncio", "")
    df2["Agência"] = "De Brito"
    df2["Editoria"] = df2["Campanha"]
    df2["Objetivo"] = df2.get("objective", "")
    df2["Meta"] = ""
    df2["Status"] = ""
    df2["Resultado"] = ""

    df_transf = df2.reindex(columns=DEST_COLUMNS, fill_value="")
    df_transf["__ID__"] = df_transf.apply(
        make_id_ponto_de_controle, axis=1, columns=DEST_COLUMNS
    )
    return df_transf


def read_destination_df() -> pd.DataFrame:
    """Lê a planilha de destino já com ``__ID__`` calculado."""

    sheet_id = os.environ["DEST_SHEET_ID"]
    tab = os.environ["DEST_TAB"]
    head = int(os.getenv("HEAD_ROW_DEST", "4"))
    df = read_df(sheet_id=sheet_id, tab=tab, header_row=head)
    df = df.reindex(columns=DEST_COLUMNS, fill_value="")
    df["__ID__"] = df.apply(make_id_ponto_de_controle, axis=1, columns=DEST_COLUMNS)
    df = df.drop_duplicates("__ID__", keep="first").reset_index(drop=True)
    return df


def diff_new_rows(src: pd.DataFrame, dst: pd.DataFrame) -> pd.DataFrame:
    """Retorna registros de ``src`` cujo ``__ID__`` não exista em ``dst``."""

    mask = ~src["__ID__"].isin(dst["__ID__"])
    return src[mask].copy()


def write_df_to_sheet_final(df_new: pd.DataFrame, *, dry_run: bool) -> None:
    """Escreve ``df_new`` no destino se ``dry_run`` for ``False``."""

    if dry_run or df_new.empty:
        logger.info("DRY-RUN: %d linhas seriam gravadas", len(df_new))
        return

    creds_json = load_google_creds()
    write_dataframe_to_sheet(
        spreadsheet_id=os.environ["DEST_SHEET_ID"],
        sheet_name=os.environ["DEST_TAB"],
        df=df_new,
        start_row=int(os.getenv("HEAD_ROW_DEST", "4")) + 2,
        google_credentials_json=creds_json,
    )


def main(dry_run: bool) -> None:
    """Executa o pipeline completo."""

    load_dotenv()
    logging.basicConfig(level=logging.INFO)

    df_origin = read_origin_df()
    logger.info("Origem: %d linhas", len(df_origin))

    df_transf = transform_df(df_origin)
    df_dest = read_destination_df()
    df_new = diff_new_rows(df_transf, df_dest)

    logger.info("Novas linhas identificadas: %d", len(df_new))
    write_df_to_sheet_final(df_new, dry_run=dry_run)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Atualiza ponto de controle")
    parser.add_argument("--dry-run", action="store_true", help="não grava no sheet")
    args = parser.parse_args()
    main(args.dry_run)

