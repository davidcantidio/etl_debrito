# utils/common/pinterest/pinterest_dimension_pin_id_merge.py
"""
Merge Pinterest demographic tabs (Idade / Gênero / Região) with pinterestGeral
using pin_id + date. 100 % agnóstico de dimensão.
"""

from __future__ import annotations

import logging
from math import floor
from typing import Dict, List, Tuple

import pandas as pd

log = logging.getLogger(__name__)

# ───────────────────────────── Config ────────────────────────────── #
METRICS: List[str] = [
    "impressions",
    "link_clicks",
    "cost",
    "video_watched_100",        # nome usado pelo Supermetrics
]

# ───────────────────────────── Utilitários ───────────────────────── #
def _coerce_numeric(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    return df


def _placements(df: pd.DataFrame) -> List[str]:
    return sorted({c.rsplit("_", 1)[0] for c in df.columns if c.endswith("_impressions")})


def _weights(row: pd.Series, pls: List[str]) -> Tuple[Dict[str, int], int]:
    w = {pl: max(int(row.get(f"{pl}_impressions", 0)), 0) for pl in pls}
    if not any(w.values()):                      # tudo zero → escolhe “maior métrica”
        top = max(pls, key=lambda pl: max(abs(float(row.get(f"{pl}_{m}", 0))) for m in METRICS))
        w[top] = 1
    return w, sum(w.values())


def _floor(v: float) -> float:
    return floor(v * 100) / 100.0


def _dist_cost(v: float, w: Dict[str, int]) -> Dict[str, float]:
    if v == 0 or not any(w.values()):
        return {pl: 0.0 for pl in w}
    total = sum(w.values())
    bruto = {pl: v * wgt / total for pl, wgt in w.items()}
    base  = {pl: _floor(x) for pl, x in bruto.items()}
    resto = round(v - sum(base.values()), 2)
    if resto:
        frac = {pl: bruto[pl] - base[pl] for pl in w}
        for pl in sorted(frac, key=frac.get, reverse=True)[: int(resto / 0.01)]:
            base[pl] += 0.01
    return {pl: round(x, 2) for pl, x in base.items()}


def _dist_int(v: int, w: Dict[str, int]) -> Dict[str, int]:
    if v == 0 or not any(w.values()):
        return {pl: 0 for pl in w}
    total = sum(w.values())
    bruto = {pl: v * wgt / total for pl, wgt in w.items()}
    base  = {pl: int(floor(x)) for pl, x in bruto.items()}
    resto = v - sum(base.values())
    if resto:
        frac = {pl: bruto[pl] - base[pl] for pl in w}
        for pl in sorted(frac, key=frac.get, reverse=True)[:resto]:
            base[pl] += 1
    return base

# ───────────────────────────── Preparação ────────────────────────── #
def _prepare_general(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip()
    return df.dropna(subset=["pin_id", "date"]).reset_index(drop=True)


def _prepare_dimension(df: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
    df = df.copy()
    df.columns = df.columns.str.strip()

    dim_col = next((c for c in ("age", "gender", "region") if c in df.columns), None)
    if dim_col is None:
        raise KeyError("Aba demográfica não contém 'age', 'gender' ou 'region'.")

    df = df.dropna(subset=["pin_id", "date", dim_col]).reset_index(drop=True)
    df = _coerce_numeric(df, METRICS)
    return df, dim_col

# ───────────────────────────── Pivot & Merge ──────────────────────── #
def _pivot_general(df: pd.DataFrame) -> pd.DataFrame:
    id_vars = ["pin_id", "date"]
    vals    = [c for c in df.columns if c not in id_vars + ["placement"]]
    pivot = (
        df.pivot_table(
            index=id_vars,
            columns="placement",
            values=vals,
            aggfunc="sum",
            fill_value=0,
        )
        .rename_axis(None, axis=1)
    )
    pivot.columns = [f"{pl}_{m}" for m, pl in pivot.columns]
    return pivot.reset_index()


def _pivot_dimension(df: pd.DataFrame, dim_col: str) -> pd.DataFrame:
    return df.groupby(["pin_id", "date", dim_col], as_index=False)[METRICS].sum()


def _merge(df_gen: pd.DataFrame, df_dim: pd.DataFrame) -> pd.DataFrame:
    return pd.merge(df_dim, df_gen, on=["pin_id", "date"], how="inner")

# ───────────────────────────── Redistribuição ─────────────────────── #
def _redistribute(df_in: pd.DataFrame, dim_col: str) -> pd.DataFrame:
    pls = _placements(df_in)

    # Totais de salvaguarda
    for m in METRICS:
        if m not in df_in.columns:
            cols = [f"{pl}_{m}" for pl in pls if f"{pl}_{m}" in df_in.columns]
            df_in[m] = df_in[cols].sum(axis=1)

    out: list[dict] = []
    for _, row in df_in.iterrows():
        w, _ = _weights(row, pls)
        dist = {
            "cost":              _dist_cost(float(row["cost"]), w),
            "impressions":       _dist_int(int(row["impressions"]), w),
            "link_clicks":       _dist_int(int(row["link_clicks"]), w),
            "video_watched_100": _dist_int(int(row["video_watched_100"]), w),
        }
        for pl in pls:
            out.append(
                {
                    "pin_id":        row["pin_id"],
                    "date":          row["date"],
                    dim_col:         row[dim_col],
                    "_Placement":    pl,
                    **{m: dist[m][pl] for m in METRICS},
                }
            )

    # Consistência global
    df_out = pd.DataFrame(out)
    for m in METRICS:
        tol = 0.01 if m == "cost" else 0
        if abs(df_in[m].sum() - df_out[m].sum()) > tol:
            raise AssertionError(f"Soma divergente em {m}")
    return df_out

# ───────────────────────────── API pública ────────────────────────── #
def merge_pinterest_dimension(
    df_general: pd.DataFrame,
    df_dimension: pd.DataFrame,
) -> pd.DataFrame:
    """
    Une pinterestGeral + aba demográfica (Idade/Gênero/Região)
    devolvendo DataFrame pronto para continuar no pipeline.
    """
    df_gen   = _prepare_general(df_general)
    df_dim, dim_col = _prepare_dimension(df_dimension)

    merged   = _merge(_pivot_general(df_gen), _pivot_dimension(df_dim, dim_col))
    df_final = _redistribute(merged, dim_col)

    log.info("✅ merge_pinterest_dimension (%s) — %d linhas", dim_col, len(df_final))
    return df_final


__all__ = ["merge_pinterest_dimension"]
