# utils/common/pinterest/region_merge.py
from __future__ import annotations

import logging
from collections import Counter
from math import floor
from typing import Dict, List, Tuple

import pandas as pd

from treat.utils.geo_normalize import (
    carregar_caches_padrao,
    limpeza_basica,
    obter_estado_de_regiao,
)

log = logging.getLogger(__name__)

METRICS: List[str] = ["impressions", "link_clicks", "cost", "video_watched_100"]

# ───────────────────────────── Helpers ──────────────────────────────
def _coerce_numeric(df: pd.DataFrame, cols: list[str]) -> None:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)


# ─────────────────── Preparação das abas de origem ──────────────────
def prepare_pinterest_region(df: pd.DataFrame) -> pd.DataFrame:
    """
    Trata **pinterestRegiao**:
      • strip em headers  
      • remove linhas sem `pin_id`, `date`, `region`  
      • normaliza `region` → `Estado`
      • garante métricas numéricas
    """
    df = df.copy()
    df.columns = df.columns.str.strip()
    required = ["pin_id", "date", "region"]
    df = df.dropna(subset=required).reset_index(drop=True)

    # geo-normalize
    cache_estados, cache_municipios = carregar_caches_padrao()
    df["Estado"] = (
        df["region"]
        .astype(str)
        .apply(limpeza_basica)
        .apply(lambda txt: obter_estado_de_regiao(txt, cache_municipios, cache_estados))
    )

    _coerce_numeric(df, METRICS)
    return df


def prepare_pinterest_general(df: pd.DataFrame) -> pd.DataFrame:
    """
    Trata **pinterestGeral**:
      • strip em headers  
      • remove linhas sem `pin_id` ou `date`
      • garante métricas numéricas
    """
    df = df.copy()
    df.columns = df.columns.str.strip()
    df = df.dropna(subset=["pin_id", "date"]).reset_index(drop=True)
    _coerce_numeric(df, METRICS)
    return df


# ───────────────────── Pivot de placement (aba geral) ─────────────────
def pivot_general_by_placement(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pivota métricas de placement em colunas do tipo `<placement>_<métrica>`.
    """
    id_vars = ["pin_id", "date"]
    value_vars = [c for c in df.columns if c not in id_vars + ["placement"]]

    pv = (
        df.pivot_table(
            index=id_vars,
            columns="placement",
            values=value_vars,
            aggfunc="sum",
            fill_value=0,
        )
        .reset_index()
    )
    pv.columns.name = None
    pv.columns = [f"{pl}_{m}" if isinstance(pl, str) else m for m, pl in pv.columns]
    return pv


# ───────────── Redistribuição proporcional das métricas ──────────────
def _placements_from_columns(df: pd.DataFrame) -> List[str]:
    return sorted({c.rsplit("_", 1)[0] for c in df.columns if c.endswith("_impressions")})


def _calc_weights(row: pd.Series, placements: List[str]) -> Tuple[Dict[str, int], int]:
    weights = {pl: max(int(row.get(f"{pl}_impressions", 0)), 0) for pl in placements}
    if not any(weights.values()):  # tudo zero → usa placement com maior métrica absoluta
        top = max(
            placements,
            key=lambda pl: max(abs(float(row.get(f"{pl}_{m}", 0))) for m in METRICS),
        )
        weights[top] = 1
    return weights, sum(weights.values())


def _floor_cent(v: float) -> float:
    return floor(v * 100) / 100.0


def _dist_cost(valor: float, pesos: Dict[str, int]) -> Dict[str, float]:
    if valor == 0 or not any(pesos.values()):
        return {pl: 0.0 for pl in pesos}

    total = sum(pesos.values())
    bruto = {pl: peso / total * valor for pl, peso in pesos.items()}
    base = {pl: _floor_cent(v) for pl in bruto.values()}
    resto = round(valor - sum(base.values()), 2)

    if resto:
        frac = {pl: bruto[pl] - base_val for pl, base_val in base.items()}
        for pl in sorted(frac, key=frac.get, reverse=True)[: int(resto / 0.01)]:
            base[pl] += 0.01
    return {pl: round(v, 2) for pl, v in base.items()}


def _dist_int(valor: int, pesos: Dict[str, int]) -> Dict[str, int]:
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


_DISTRIB_LOGS: Counter[str] = Counter()


def redistribute_region_metrics(df_in: pd.DataFrame) -> pd.DataFrame:
    """
    Redistribui as métricas agregadas por Estado de volta para cada placement,
    preservando as somas globais.
    """
    placements = _placements_from_columns(df_in)

    # garante colunas-totais
    for m in METRICS:
        if m not in df_in.columns:
            cols = [f"{pl}_{m}" for pl in placements if f"{pl}_{m}" in df_in.columns]
            df_in[m] = df_in[cols].sum(axis=1)

    rows_out: list[dict] = []

    for _, row in df_in.iterrows():
        pesos, _ = _calc_weights(row, placements)

        dist: Dict[str, Dict[str, float | int]] = {"cost": _dist_cost(float(row["cost"]), pesos)}
        for m in ("impressions", "link_clicks", "video_watched_100"):
            dist[m] = _dist_int(int(row[m]), pesos)

        for pl in placements:
            rows_out.append(
                {
                    "pin_id": row["pin_id"],
                    "date": row["date"],
                    "Estado": row["Estado"],
                    "_Placement": pl,
                    **{m: dist[m][pl] for m in METRICS},
                }
            )

    df_out = pd.DataFrame(rows_out)

    # validação de soma global
    for m in METRICS:
        tol = 0.01 if m == "cost" else 0
        if abs(df_in[m].sum() - df_out[m].sum()) > tol:
            raise AssertionError(f"Soma divergente em '{m}'")

    log.info("✅ redistribute_region_metrics — %s linhas", len(df_out))
    return df_out


# ────────────────────────── Pipeline completo ─────────────────────────
def merge_pinterest_region_data(
    df_general: pd.DataFrame,
    df_region: pd.DataFrame,
) -> pd.DataFrame:
    """
    Pipeline completo:
      1. limpeza + geo em cada aba  
      2. pivot de placements  
      3. merge por pin_id + date  
      4. redistribuição proporcional
      5. ordena colunas esperadas
    """
    df_gen_clean = prepare_pinterest_general(df_general)
    df_reg_clean = prepare_pinterest_region(df_region)

    df_pivot = pivot_general_by_placement(df_gen_clean)
    df_join = pd.merge(
        df_reg_clean,
        df_pivot,
        on=["pin_id", "date"],
        how="inner",
        suffixes=("", "_pl"),
    )

    df_final = redistribute_region_metrics(df_join)

    return df_final


__all__ = [
    "METRICS",
    "prepare_pinterest_region",
    "prepare_pinterest_general",
    "pivot_general_by_placement",
    "merge_pinterest_region_data",
    "redistribute_region_metrics",
]
