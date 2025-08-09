"""
Funções utilitárias comuns aos ETLs do **Meta Ads** (Idade, Gênero, Região …).

•  carrega as abas *metaIdade* e *metaGeral* do Google‑Sheets
•  faz pivôs por placement / age
•  contém o algoritmo `distribute_age_metrics` – totalmente testado em
   `tests/test_meta_idade_algoritmo_extra.py`
"""

from __future__ import annotations

import logging
from collections import Counter
from decimal import ROUND_DOWN, Decimal
from math import floor
from typing import Dict, List, Tuple, Union

import pandas as pd
from utils.get_google_client import get_google_client
# -------------------------------------------------------------------- #
# Dependências auxiliares de Sheets
# -------------------------------------------------------------------- #
from utils.google_sheets import CREDS_PATH, SPREADSHEET_ID
from utils.read_sheet_as_dataframe import read_sheet_as_dataframe_range

# -------------------------------------------------------------------- #
# 0) Conjunto ÚNICO de métricas usadas em Meta‑Idade
# -------------------------------------------------------------------- #
METRICAS: List[str] = [
    "Impressions",
    "Link clicks",
    "Cost",
    "Video watches at 100%",
]

# -------------------------------------------------------------------- #
# 1) Carregamento e preparação das abas de origem
# -------------------------------------------------------------------- #
_CLIENT = get_google_client(CREDS_PATH)  #  re‑usa a sessão


def _read_sheet(sheet_name: str) -> pd.DataFrame:
    """Wrapper minimamente tipado para ler qualquer aba."""
    return read_sheet_as_dataframe_range(
        _CLIENT,
        SPREADSHEET_ID,
        sheet_name=sheet_name,
        range_str="A1:ZZ",
        header_row_index=0,
    )


def load_and_prepare_meta_age_data() -> pd.DataFrame:
    """Lê a aba **metaIdade** e garante colunas numéricas."""
    df = _read_sheet("metaIdade").dropna(subset=["Ad ID", "Date", "Age"])
    for col in METRICAS:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df


def load_and_prepare_meta_placement_data() -> pd.DataFrame:
    """Lê a aba **metaGeral** já com dados por placement."""
    df = _read_sheet("metaGeral").dropna(subset=["Ad ID", "Date"])
    return df


# -------------------------------------------------------------------- #
# 2) Pivot / merge helpers
# -------------------------------------------------------------------- #
def pivot_meta_age_data(df: pd.DataFrame) -> pd.DataFrame:
    """Agrupa ['Ad ID', 'Date', 'Age'] somando métricas."""
    return df.groupby(["Ad ID", "Date", "Age"], as_index=False)[METRICAS].sum()


def pivot_meta_placement_data(df: pd.DataFrame) -> pd.DataFrame:
    """Coloca métricas por placement no formato `<placement>_<metric>`."""
    id_vars = ["Ad ID", "Date"]
    value_vars = [c for c in df.columns if c not in id_vars + ["Placement"]]

    df_piv = df.pivot_table(
        index=id_vars,
        columns="Placement",
        values=value_vars,
        aggfunc="sum",
        fill_value=0,
    )
    df_piv.columns = [f"{pl}_{m}" for m, pl in df_piv.columns]
    df_piv.reset_index(inplace=True)
    return df_piv


def merge_placement_and_age_data(
    df_age: pd.DataFrame,
    df_placement: pd.DataFrame,
) -> pd.DataFrame:
    """Merge final por ['Ad ID', 'Date']."""
    return pd.merge(df_age, df_placement, on=["Ad ID", "Date"], how="inner")


# -------------------------------------------------------------------- #
# 3) Algoritmo de redistribuição por Age
# -------------------------------------------------------------------- #
def get_placements(df: pd.DataFrame) -> List[str]:
    """Extrai e devolve *placements* ordenados alfabeticamente."""
    cols = [c for c in df.columns if c.endswith("_Impressions")]
    return sorted({c.rsplit("_", 1)[0] for c in cols})


def compute_pesos_impressao(
    row: pd.Series, placements: List[str]
) -> Tuple[Dict[str, int], int]:
    logging.getLogger("common_meta.compute_pesos_impressao")
    pesos = {pl: max(int(row.get(f"{pl}_Impressions", 0)), 0) for pl in placements}
    if all(v == 0 for v in pesos.values()):
        top_pl, _ = max(
            (
                (pl, abs(float(row.get(f"{pl}_{m}", 0))))
                for pl in placements
                for m in METRICAS
            ),
            key=lambda t: t[1],
            default=(placements[0], 0),
        )
        pesos[top_pl] = 1
    return pesos, sum(pesos.values())


DISTRIBUICAO_LOGS: Counter[str] = Counter()


def _sanitize_metric_value(metric: str, raw):
    v = 0 if raw is None or pd.isna(raw) else raw
    v = max(v, 0)
    return float(v) if metric == "Cost" else int(round(v))


def _floor_cents(v: float) -> float:
    return float(Decimal(str(v)).quantize(Decimal("0.01"), rounding=ROUND_DOWN))


def _special_distribution(
    metric: str,
    valor_int: int,
    placements: List[str],
    peso_imp: Dict[str, int],
):
    if metric == "Cost" or valor_int > len(placements):
        return None, False

    dist = {pl: 0 for pl in placements}
    if valor_int < len(placements):
        top_pl = max(placements, key=lambda pl: peso_imp.get(pl, 0))
        dist[top_pl] = valor_int
    else:  # valor == len(placements)
        for pl in placements[:valor_int]:
            dist[pl] = 1
    DISTRIBUICAO_LOGS[f"{metric}_especial"] += 1
    return dist, True


def _distribute_proportional(
    metric: str, valor, placements, peso_imp
) -> Dict[str, Union[int, float]]:
    total_peso = sum(peso_imp.values())
    if valor == 0 or total_peso == 0:
        return {pl: 0 for pl in placements}

    quots = {pl: (peso_imp[pl] / total_peso) * valor for pl in placements}
    if metric == "Cost":
        base = {pl: _floor_cents(quots[pl]) for pl in placements}
        resto = round(valor - sum(base.values()), 2)
        inc = 0.01
    else:
        base = {pl: int(floor(quots[pl])) for pl in placements}
        resto = int(round(valor - sum(base.values())))
        inc = 1

    if resto > 0:
        frac = {pl: quots[pl] - base[pl] for pl in placements}
        ordem = sorted(placements, key=lambda pl: (-frac[pl], pl))
        for pl in ordem[: int(round(resto / inc))]:
            base[pl] += inc
    if metric == "Cost":
        base = {pl: round(base[pl], 2) for pl in base}
    return base


def _fix_inconsistencies_and_types(metric, distrib, peso_imp):
    log = logging.getLogger("common_meta.fix")
    out = {}
    for pl, v in distrib.items():
        if v > 0 and peso_imp.get(pl, 0) == 0:
            log.critical("Placement %s recebeu cota mas peso 0", pl)
        out[pl] = _floor_cents(v) if metric == "Cost" else int(round(v))
    return out


def distribute_age_metrics(df_in: pd.DataFrame) -> pd.DataFrame:
    log = logging.getLogger("common_meta.distribute_age_metrics")
    log.info("🚚  Distribuindo métricas por Age…")

    P = get_placements(df_in)

    # Garante métricas totais se não existirem
    for m in METRICAS:
        if m not in df_in.columns:
            df_in[m] = df_in[
                [f"{pl}_{m}" for pl in P if f"{pl}_{m}" in df_in.columns]
            ].sum(axis=1)

    out_rows: list[dict] = []

    for _, row in df_in.iterrows():
        peso_imp, _ = compute_pesos_impressao(row, P)
        dist_metric = {}
        for m in METRICAS:
            val = _sanitize_metric_value(m, row.get(m, 0))
            dist, used = _special_distribution(m, int(val), P, peso_imp)
            if not used:
                dist = _distribute_proportional(m, val, P, peso_imp)
                dist = _fix_inconsistencies_and_types(m, dist, peso_imp)
            dist_metric[m] = dist

        for pl in P:
            rec = {
                "Ad ID": row["Ad ID"],
                "Date": row["Date"],
                "Age": row["Age"],
                "_Plataforma": pl,
                **{m: dist_metric[m][pl] for m in METRICAS},
            }
            out_rows.append(rec)

    df_out = pd.DataFrame(out_rows)

    # Checagem de soma global
    for m in METRICAS:
        tol = 0.01 if m == "Cost" else 1e-6
        if abs(df_in[m].sum() - df_out[m].sum()) > tol:
            raise AssertionError(f"Soma global divergente em {m}")

    log.info("✅  Distribuição concluída — %s linhas", len(df_out))
    return df_out


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
