# utils/common/pinterest/age_merge.py
from __future__ import annotations

import logging
from typing import List

import pandas as pd

log = logging.getLogger(__name__)

# Métricas que vamos manter / redistribuir
METRICS: List[str] = ["impressions", "link_clicks", "cost", "video_watched_100"]


# ─────────────────────────────── Helpers ────────────────────────────────
def _coerce_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """
    Converte as colunas *cols* para numérico, substituindo NaN por 0.
    """
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    return df


# ───────────────────────── Pré-processamento base ───────────────────────
def load_and_prepare_pinterest_general(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepara o *DataFrame* da aba **pinterestGeral**:

    1. `strip()` nos nomes de coluna  
    2. remove linhas sem `pin_id` **ou** `date`  
    3. força métricas → numérico
    """
    df = df.copy()
    df.columns = df.columns.str.strip()
    df = df.dropna(subset=["pin_id", "date"]).reset_index(drop=True)
    df = _coerce_numeric(df, METRICS)
    return df


def load_and_prepare_pinterest_age(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepara o *DataFrame* da aba **pinterestIdade**:

    1. `strip()` nos nomes de coluna  
    2. remove linhas sem `pin_id`, `date` ou `age`  
    3. força métricas → numérico
    """
    df = df.copy()
    df.columns = df.columns.str.strip()
    df = df.dropna(subset=["pin_id", "date", "age"]).reset_index(drop=True)
    df = _coerce_numeric(df, METRICS)
    return df


# ─────────────────────────────── Merge ──────────────────────────────────
def merge_pinterest_age_data(
    df_general: pd.DataFrame,
    df_age: pd.DataFrame,
) -> pd.DataFrame:
    """
    Faz o *inner-join* entre **pinterestGeral** e **pinterestIdade**
    via (`pin_id`, `date`) e devolve já no esquema final esperado
    pelo modelo-destino.

    Colunas resultantes — serão criadas vazias se ainda não existirem:

    ```
    date, account_name, ID_Veiculo, Veiculo,
    ID_Campanha, Campanha, ad_group_name, ad_name, objective,
    age, impressions, cost, link_clicks, video_watched_100, ID
    ```
    """
    df_gen = load_and_prepare_pinterest_general(df_general)
    df_age = load_and_prepare_pinterest_age(df_age)

    df = pd.merge(
        df_gen,
        df_age,
        on=["pin_id", "date"],
        how="inner",
        suffixes=("", "_dem"),
    )

    # --- renomeia / garante colunas obrigatórias ------------------------
    df = df.rename(columns={"pin_id": "ID"})

    required = [
        "account_name",
        "ID_Veiculo",
        "Veiculo",
        "ID_Campanha",
        "Campanha",
        "ad_group_name",
        "ad_name",
        "objective",
    ]
    for col in required:
        if col not in df.columns:
            df[col] = ""                          # placeholder vazio

    final_cols: List[str] = [
        "date",
        "account_name",
        "ID_Veiculo",
        "Veiculo",
        "ID_Campanha",
        "Campanha",
        "ad_group_name",
        "ad_name",
        "objective",
        "age",
        *METRICS,
        "ID",
    ]

    df_final = df[final_cols].copy()
    log.info("✅ merge_pinterest_age_data — %s linhas", len(df_final))
    return df_final


# ─────────────────────────────── CLI quick-test ─────────────────────────
if __name__ == "__main__":
    # Exemplo de uso: receba os dataframes já carregados em memória
    # (all_raw é o dicionário vindo do SheetsFetcher em seu notebook/pipeline)
    from extract.sheets_fetcher import SheetsFetcher

    SHEETS = ["pinterestGeral", "pinterestIdade"]
    fetcher = SheetsFetcher(
        spreadsheet_id="YOUR_SPREADSHEET_ID",
        creds_path="creds.json",
    )
    all_raw = fetcher.get(SHEETS)

    df_out = merge_pinterest_age_data(
        df_general=all_raw["pinterestGeral"],
        df_age=all_raw["pinterestIdade"],
    )
    print(df_out.head())
