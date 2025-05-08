# utils/common/meta/age_placements_merge.py
from __future__ import annotations
import logging
from collections import Counter
from math import floor
from typing import Dict, List, Tuple

import pandas as pd
from utils.google_sheets import CREDS_PATH, SPREADSHEET_ID
from utils.get_google_client import get_google_client
from utils.read_sheet_as_dataframe import read_sheet_as_dataframe_range
from utils.normalize import normalize_age

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

METRICAS: List[str] = ["impressions", "link_clicks", "cost", "video_watches_100"]

# ------------------------------- CACHE -------------------------------- #
_client = get_google_client(CREDS_PATH)
_cache: dict[str, pd.DataFrame] = {}


def _read_sheet(sheet: str) -> pd.DataFrame:
    if sheet not in _cache:
        _cache[sheet] = read_sheet_as_dataframe_range(
            _client,
            SPREADSHEET_ID,
            sheet_name=sheet,
            range_str="A1:ZZ",
            header_row_index=0,
        )
    return _cache[sheet].copy()


# ------------------------- PREPARAÇÃO DE DADOS ------------------------ #
def load_and_prepare_meta_age_data() -> pd.DataFrame:
    df = _read_sheet("metaIdade")  # Carrega os dados da aba 'metaIdade'
    
    # Verificação de dados consistentes antes de qualquer transformação
    df = df.dropna(subset=["ad_id", "date", "impressions", "cost"])  # Garante que não haja NaN nessas colunas essenciais
    
    # 0) Strip em todos os nomes de coluna
    df.columns = df.columns.str.strip()
    
    # 1) Remove registros com dados ausentes em colunas críticas
    df = df.dropna(subset=["ad_id", "date", "age"])  # Garante que 'age' está presente
    
    # 2) Converte cost (pode vir como 'cost' ou 'cost' etc) - busca case-insensitive
    cost_col = next((c for c in df.columns if c.lower() == "cost"), None)
    if cost_col:
        s = df[cost_col].astype(str)
        s = s.str.replace("\u00a0", "")
        s = s.str.replace(r"\.(?=\d{3}(?:\.|,))", "", regex=True)
        s = s.str.replace(",", ".", regex=False)
        df[cost_col] = pd.to_numeric(s, errors="coerce").fillna(0)
        df.rename(columns={cost_col: "cost"}, inplace=True)
    
    # 3) Outras métricas
    for col in ["impressions", "link_clicks", "video_watches_100"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    
    # Verifique se 'age' está presente
    if 'age' not in df.columns:
        logging.error("A coluna 'age' não foi encontrada na aba 'metaIdade'.")
        raise KeyError("A coluna 'age' não foi encontrada na aba 'metaIdade'.")
    
    return df

def load_and_prepare_meta_placement_data() -> pd.DataFrame:
    return _read_sheet("metaGeral").dropna(subset=["ad_id", "date"])


# ------------------------------ PIVOTS -------------------------------- #
def pivot_meta_age_data(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby(["ad_id", "date", "age"], as_index=False)[METRICAS].sum()

def pivot_meta_placement_data(df_placement: pd.DataFrame) -> pd.DataFrame:
    id_vars = ["ad_id", "date"]
    value_vars = [c for c in df_placement.columns if c not in id_vars + ["placement"]]

    pivot_df = df_placement.pivot_table(
        index=id_vars,
        columns="placement",
        values=value_vars,
        aggfunc="sum",
        fill_value=0,
    )

    # Após o pivotamento, garantir que as colunas com métricas sejam numéricas
    pivot_df = pivot_df.apply(pd.to_numeric, errors='coerce').fillna(0)

    pivot_df.columns.name = None
    pivot_df.columns = [f"{pl}_{m}" for m, pl in pivot_df.columns]
    return pivot_df.reset_index()

def merge_placement_and_age_data(df_age: pd.DataFrame, df_place: pd.DataFrame) -> pd.DataFrame:
    df_merged = pd.merge(df_age, df_place, on=["ad_id", "date"], how="inner")
    
    # Verificação para garantir que a coluna 'age' foi incluída no merge
    if 'age' not in df_merged.columns:
        logging.error("A coluna 'age' não foi incluída após o merge.")
        raise KeyError("A coluna 'age' não foi incluída após o merge.")

    return df_merged


# --------------------- REDISTRIBUIÇÃO DE MÉTRICAS --------------------- #
def get_placements(df: pd.DataFrame) -> List[str]:
    return sorted({c.rsplit("_", 1)[0] for c in df.columns if c.endswith("_impressions")})


def compute_pesos_impressao(row: pd.Series, placements: List[str]) -> Tuple[Dict[str, int], int]:
    pesos = {pl: max(int(row.get(f"{pl}_impressions", 0)), 0) for pl in placements}

    # se todos zero → define peso 1 para placement com maior métrica absoluta
    if not any(pesos.values()):
        top_pl = max(
            placements,
            key=lambda pl: max(abs(float(row.get(f"{pl}_{m}", 0))) for m in METRICAS),
        )
        pesos[top_pl] = 1
    return pesos, sum(pesos.values())


def _floor_cents(v: float) -> float:
    return floor(v * 100) / 100.0


def _distribuir_cost(valor: float, pesos: Dict[str, int]) -> Dict[str, float]:
    if valor == 0 or not any(pesos.values()):
        return {pl: 0.0 for pl in pesos}

    total = sum(pesos.values())
    bruto = {pl: peso / total * valor for pl, peso in pesos.items()}
    base = {pl: _floor_cents(v) for pl, v in bruto.items()}
    resto = round(valor - sum(base.values()), 2)

    if resto:
        frac = {pl: bruto[pl] - base[pl] for pl in pesos}
        for pl in sorted(frac, key=frac.get, reverse=True)[: int(resto / 0.01)]:
            base[pl] += 0.01
    return {pl: round(v, 2) for pl, v in base.items()}


def _distribuir_contagem(valor: int, pesos: Dict[str, int]) -> Dict[str, int]:
    if valor == 0 or not any(pesos.values()):
        return {pl: 0 for pl in pesos}

    total = sum(pesos.values())
    bruto = {pl: peso / total * valor for pl, peso in pesos.items()}
    base = {pl: int(floor(v)) for pl, v in bruto.items()}
    resto = valor - sum(base.values())

    if resto:
        frac = {pl: bruto[pl] - base[pl] for pl in pesos}
        for pl in sorted(frac, key=frac.get, reverse=True)[:resto]:
            base[pl] += 1
    return base


# contador global de “casos especiais”
DISTRIBUICAO_LOGS: Counter[str] = Counter()


def distribute_age_metrics(df_in: pd.DataFrame) -> pd.DataFrame:
    # Verifique se 'age' está presente no DataFrame antes de continuar
    if "age" not in df_in.columns:
        logging.error("A coluna 'age' não foi encontrada no DataFrame.")
        raise KeyError("A coluna 'age' não foi encontrada no DataFrame.")

    # Continuação da lógica de distribuição
    placements = get_placements(df_in)

    # Garante colunas‑totais (impressions, cost, etc.)
    for m in METRICAS:
        if m not in df_in.columns:
            cols = [f"{pl}_{m}" for pl in placements if f"{pl}_{m}" in df_in.columns]
            df_in[m] = df_in[cols].sum(axis=1)

    # Garantir que as colunas de métricas sejam numéricas antes de qualquer operação de distribuição
    df_in[METRICAS] = df_in[METRICAS].apply(pd.to_numeric, errors='coerce').fillna(0)

    output_rows: list[dict] = []

    for _, row in df_in.iterrows():
        pesos, _ = compute_pesos_impressao(row, placements)

        # Distribuição proporcional por métrica
        dist: Dict[str, Dict[str, float | int]] = {}
        dist["cost"] = _distribuir_cost(float(row["cost"]), pesos)
        for m in ("impressions", "link_clicks", "video_watches_100"):
            dist[m] = _distribuir_contagem(int(row[m]), pesos)

        # Gera uma linha por placement
        for pl in placements:
            output_rows.append(
                {
                    "ad_id": row["ad_id"],
                    "date": row["date"],
                    "age": row["age"],  # Garantir que 'age' é incluída se necessário
                    "_Plataforma": pl,
                    **{m: dist[m][pl] for m in METRICAS},
                }
            )

    df_out = pd.DataFrame(output_rows)

    # Verificação de integridade
    for m in METRICAS:
        soma_in, soma_out = df_in[m].sum(), df_out[m].sum()
        tol = 0.01 if m == "cost" else 0
        if abs(soma_in - soma_out) > tol:
            raise AssertionError(f"Soma global divergente em '{m}': {soma_in} → {soma_out}")

    log.info("✅ distribute_age_metrics — %s linhas", len(df_out))
    return df_out

# --------------------------- re‐exports --------------------------- #
__all__ = [
    "METRICAS",
    "load_and_prepare_meta_age_data",
    "load_and_prepare_meta_placement_data",
    "pivot_meta_age_data",
    "pivot_meta_placement_data",
    "merge_placement_and_age_data",
    "distribute_age_metrics",
    "get_placements",
    "compute_pesos_impressao",
]
