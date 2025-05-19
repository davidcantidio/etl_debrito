# utils/common/meta/gender_placements_merge.py
from __future__ import annotations
import logging
from collections import Counter
from math import floor
from typing import Dict, List, Tuple

import pandas as pd
from utils.google_sheets import CREDS_PATH, SPREADSHEET_ID
from utils.get_google_client import get_google_client
from utils.read_sheet_as_dataframe import read_sheet_as_dataframe_range

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

METRICAS: List[str] = ["Impressions", "Link clicks", "cost", "video_watches_100"]

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
def load_and_prepare_meta_gender_data() -> pd.DataFrame:
    df = _read_sheet("meta")
    # 0) Strip em todos os nomes de coluna
    df.columns = df.columns.str.strip()
    # 1) Drop obrigatórios
    df = df.dropna(subset=["ad_id", "date", "gender"])
    # 2) Converte cost (pode vir como 'cost' ou 'cost' etc)
    #    vamos buscar case‑insensitive:
    cost_col = next((c for c in df.columns if c.lower() == "cost"), None)
    if cost_col:
        s = df[cost_col].astype(str)
        s = s.str.replace("\u00a0", "")
        s = s.str.replace(r"\.(?=\d{3}(?:\.|,))", "", regex=True)
        s = s.str.replace(",", ".", regex=False)
        df[cost_col] = pd.to_numeric(s, errors="coerce").fillna(0)
        # já padroniza para 'cost'
        df.rename(columns={cost_col: "cost"}, inplace=True)
    # 3) Outras métricas
    for col in ["Impressions", "Link clicks", "video_watches_100"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df


def load_and_prepare_meta_placement_data() -> pd.DataFrame:
    return _read_sheet("metaGeral").dropna(subset=["ad_id", "date"])


# ------------------------------ PIVOTS -------------------------------- #
def pivot_meta_gender_data(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby(["ad_id", "date", "gender"], as_index=False)[METRICAS].sum()


def pivot_meta_placement_data(df_placement: pd.DataFrame) -> pd.DataFrame:
    id_vars = ["ad_id", "date"]
    value_vars = [c for c in df_placement.columns if c not in id_vars + ["placement"]]

    pivot_df = (
        df_placement.pivot_table(
            index=id_vars,
            columns="placement",
            values=value_vars,
            aggfunc="sum",
            fill_value=0,
        )
    )
    # remove o name do eixo de colunas
    pivot_df.columns.name = None
    # achata MultiIndex  ->  "<placement>_<metric>"
    pivot_df.columns = [f"{pl}_{m}" for m, pl in pivot_df.columns]
    return pivot_df.reset_index()


def merge_placement_and_gender_data(df_gender: pd.DataFrame, df_place: pd.DataFrame) -> pd.DataFrame:
    return pd.merge(df_gender, df_place, on=["ad_id", "date"], how="inner")


# --------------------- REDISTRIBUIÇÃO DE MÉTRICAS --------------------- #
def get_placements(df: pd.DataFrame) -> List[str]:
    return sorted({c.rsplit("_", 1)[0] for c in df.columns if c.endswith("_Impressions")})


def compute_pesos_impressao(row: pd.Series, placements: List[str]) -> Tuple[Dict[str, int], int]:
    pesos = {pl: max(int(row.get(f"{pl}_Impressions", 0)), 0) for pl in placements}

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


def _distribuir_contgenderm(valor: int, pesos: Dict[str, int]) -> Dict[str, int]:
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


def distribute_gender_metrics(df_in: pd.DataFrame) -> pd.DataFrame:
    """
    Redistribui as métricas agregadas por  para cada placement,
    preservando a soma global.
    """
    placements = get_placements(df_in)

    # Garante colunas‑totais (Impressions, cost, etc.)
    for m in METRICAS:
        if m not in df_in.columns:
            cols = [f"{pl}_{m}" for pl in placements if f"{pl}_{m}" in df_in.columns]
            df_in[m] = df_in[cols].sum(axis=1)

    output_rows: list[dict] = []         # <-- agora declarado

    for _, row in df_in.iterrows():
        pesos, _ = compute_pesos_impressao(row, placements)

        # distribuições por métrica
        dist: Dict[str, Dict[str, float | int]] = {}
        # cost separado: float 2 casas – demais inteiros
        dist["cost"] = _distribuir_cost(float(row["cost"]), pesos)
        for m in ("Impressions", "Link clicks", "video_watches_100"):
            dist[m] = _distribuir_contgenderm(int(row[m]), pesos)

        # gera 1 linha por placement
        for pl in placements:
            output_rows.append(
                {
                    "ad_id": row["ad_id"],
                    "date":  row["date"],
                    "":   row["gender"],
                    "_Plataforma": pl,
                    **{m: dist[m][pl] for m in METRICAS},
                }
            )

    df_out = pd.DataFrame(output_rows)

    # ---------- checgenderm de integridade global ----------
    for m in METRICAS:
        soma_in, soma_out = df_in[m].sum(), df_out[m].sum()
        tol = 0.01 if m == "cost" else 0
        if abs(soma_in - soma_out) > tol:
            raise AssertionError(
                f"Soma global divergente em '{m}': {soma_in} → {soma_out}"
            )

    log.info("✅ distribute_gender_metrics — %s linhas", len(df_out))
    return df_out


# --------------------------- re‐exports --------------------------- #
__all__ = [
    "METRICAS",
    "load_and_prepare_meta_gender_data",
    "load_and_prepare_meta_placement_data",
    "pivot_meta_gender_data",
    "pivot_meta_placement_data",
    "merge_placement_and_gender_data",
    "distribute_gender_metrics",
    "get_placements",
    "compute_pesos_impressao",
]
