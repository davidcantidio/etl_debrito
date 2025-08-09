# %% [markdown]
# # Pipeline de atualização – Ponto de Controle
#
# - Conecta aos sheets de **origem** e **destino**
# - Gera / filtra / normaliza dados
# - Escreve apenas linhas novas (dry-run opcional)
#
# **Como executar**
#
# ```bash
# poetry install
# poetry run python ponto_de_controle_notebook.py --dry-run   # só loga
# poetry run python ponto_de_controle_notebook.py             # grava no sheet
# ```
#
# ---
# _Cada célula imprime o estado dos DataFrames para facilitar o rastreio._
#

# %% [markdown]
"""
Imports e constantes – todos num único bloco para facilitar lint/format.
"""

# %%
from __future__ import annotations

import argparse
import logging
import os
from collections import OrderedDict
from pathlib import Path
from typing import Iterable

import pandas as pd
from dotenv import load_dotenv

from transform.extract import read_df
from transform.transform.utils.campos_calculados import (
    DEFAULT_DEST_COLUMNS,
    add_key_creative,
    dedupe_by_key_creative,
    make_id_ponto_de_controle,
)
from transform.transform.utils.datas import concat_period, normalize_date_to_str_DD_M_YYYY
from transform.transform.utils.normalize import normalize_vehicle
from transform.transform.utils.write_dataframe_to_sheet import write_dataframe_to_sheet

# Importa MIN_DATE da configuração central
from config import MIN_DATE

logger = logging.getLogger(__name__)
pd.set_option("display.max_rows", 20)
pd.set_option("display.max_columns", None)

# %%
# ---------------------------------------------------------------------
# Config – pode ser sobrescrita por variáveis de ambiente (.env)
# ---------------------------------------------------------------------
load_dotenv()  # carrega .env local, se existir

ORIGIN_SHEET_ID: str = os.getenv("ORIGIN_SHEET_ID", "")
ORIGIN_TAB: str = os.getenv("ORIGIN_TAB", "modeloGeral")

DEST_SHEET_ID: str = os.getenv("DEST_SHEET_ID", "")
DEST_TAB: str = os.getenv("DEST_TAB", "IMPULSIONAMENTOS 2025")
HEAD_ROW_DEST: int = int(os.getenv("HEAD_ROW_DEST", "4"))  # zero-based

GOOGLE_CREDS_PATH: Path = Path(os.getenv("GOOGLE_CREDS_PATH", "creds.json"))

# MIN_DATE agora vem de config.py via constants.py
DEST_COLUMNS: list[str] = list(OrderedDict.fromkeys(DEFAULT_DEST_COLUMNS))  # garante unicidade
assert len(DEST_COLUMNS) == 11, "DEST_COLUMNS deve conter 11 rótulos únicos"

print("▶ DEST_COLUMNS:", DEST_COLUMNS)

# %% [markdown]
# ## Auxiliares

# %%
def load_google_creds() -> str:
    """Retorna o JSON de credenciais para uso na API Google."""
    return GOOGLE_CREDS_PATH.read_text(encoding="utf-8")


def debug_shape(df: pd.DataFrame, *, name: str) -> None:
    """Imprime forma, colunas e as 5 primeiras linhas de `df`."""
    print(f"▼ {name}: {df.shape[0]} × {df.shape[1]}")
    display(df.head())


# %% [markdown]
# ## 1 · Extrair & preparar **origem**

# %%
def read_origin_df() -> pd.DataFrame:
    """
    Lê planilha de origem, gera `key_creative`, filtra por data mínima
    e garante unicidade.
    """
    logger.info("Lendo origem %s › %s …", ORIGIN_SHEET_ID, ORIGIN_TAB)
    df = read_df(sheet_id=ORIGIN_SHEET_ID, tab=ORIGIN_TAB, header_row=0)

    # key_creative + dedup
    df = add_key_creative(df)
    df = dedupe_by_key_creative(df)

    # filtro temporal
    df["date_dt"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df[df["date_dt"] >= MIN_DATE].copy()
    df.drop(columns=["date_dt"], inplace=True)

    debug_shape(df, name="df_origin (pos-filtro)")
    assert df["key_creative"].ne("").all(), "Há key_creative vazio!"
    return df


df_origin = read_origin_df()  # executa já nesta célula

# %% [markdown]
# ## 2 · Transformar para colunas de destino

# %%
def transform_df(df: pd.DataFrame) -> pd.DataFrame:
    """Converte `df` para o layout de destino e calcula `__ID__`."""
    df2 = df.copy()

    df2["Data"] = df2["start"].apply(normalize_date_to_str_DD_M_YYYY)
    df2["Periodo"] = df2.apply(lambda r: concat_period(r["start"], r["end"]), axis=1)
    df2["Veiculo"] = df2["Veiculo"].apply(normalize_vehicle)

    df2["Link conteúdos impulsionados"] = df2.get("URL_do_Anuncio", "")
    df2["Agência"] = "De Brito"
    df2["Editoria"] = df2["Campanha"]
    df2["Objetivo"] = df2.get("objective", "")
    df2[["Meta", "Status", "Resultado"]] = ""

    df_t = df2.reindex(columns=DEST_COLUMNS, fill_value="")
    df_t["__ID__"] = df_t.apply(
        make_id_ponto_de_controle, axis=1, columns=DEST_COLUMNS
    )
    debug_shape(df_t, name="df_transf")
    return df_t


df_transf = transform_df(df_origin)

# %% [markdown]
# ## 3 · Extrair **destino** e deduplicar cabeçalho

# %%
def read_destination_df() -> pd.DataFrame:
    """
    Lê planilha de destino, resolve rótulos duplicados,
    reindexa em DEST_COLUMNS e gera `__ID__`.
    """
    logger.info("Lendo destino %s › %s …", DEST_SHEET_ID, DEST_TAB)
    df = read_df(sheet_id=DEST_SHEET_ID, tab=DEST_TAB, header_row=HEAD_ROW_DEST)

    # 1) cabeçalhos duplicados → Data, Data.1 …
    if df.columns.duplicated().any():
        logger.warning("Cabeçalhos duplicados detectados – renomeando")
        df.columns = pd.io.parsers.ParserBase(
            {"names": df.columns}
        )._maybe_dedup_names(df.columns)

    # 2) reindexa/normaliza
    df = df.reindex(columns=DEST_COLUMNS, fill_value="")

    # 3) gera __ID__ e deduplica linhas
    df["__ID__"] = df.apply(make_id_ponto_de_controle, axis=1, columns=DEST_COLUMNS)
    df = df.drop_duplicates("__ID__", keep="first").reset_index(drop=True)

    debug_shape(df, name="df_dest")
    return df


df_dest = read_destination_df()

# %% [markdown]
# ## 4 · Diferença & escrita

# %%
def diff_new_rows(src: pd.DataFrame, dst: pd.DataFrame) -> pd.DataFrame:
    """Linhas de `src` cujo `__ID__` não está em `dst`."""
    return src[~src["__ID__"].isin(dst["__ID__"])].copy()


df_new = diff_new_rows(df_transf, df_dest)
debug_shape(df_new, name="df_new (a gravar)")

# %%
def write_df_to_sheet_final(df_new: pd.DataFrame, *, dry_run: bool) -> None:
    """
    Grava `df_new` no destino se `dry_run` for False.
    Linha inicial = HEAD_ROW_DEST + dados existentes + 1.
    """
    if dry_run:
        logger.info("DRY-RUN: %d linhas seriam gravadas", len(df_new))
        return
    if df_new.empty:
        logger.info("Nada a gravar – destino já está atualizado.")
        return

    creds_json = load_google_creds()
    start_row = HEAD_ROW_DEST + 1 + len(df_dest) + 1  # header + dados + linha vazia
    write_dataframe_to_sheet(
        spreadsheet_id=DEST_SHEET_ID,
        sheet_name=DEST_TAB,
        df=df_new.drop(columns="__ID__", errors="ignore"),
        start_row=start_row,
        include_header=False,
        google_credentials_json=creds_json,
    )
    logger.info("Gravadas %d linhas na linha %d", len(df_new), start_row)


# %% [markdown]
# ## 5 · Função `main` + CLI

# %%
def main(*, dry_run: bool) -> None:
    """Orquestra todo o pipeline."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    logger.info("── Início do pipeline ──")
    write_df_to_sheet_final(df_new, dry_run=dry_run)
    logger.info("── Fim ──")


if __name__ == "__main__" and "get_ipython" not in globals():
    parser = argparse.ArgumentParser(description="Atualiza ponto de controle")
    parser.add_argument("--dry-run", action="store_true", help="não grava no sheet")
    args = parser.parse_args()
    main(dry_run=args.dry_run)
else:
    # Em notebook executamos já como dry-run para segurança
    main(dry_run=True)
