"""
Merge Pinterest – dimensões demográficas (idade/gênero/região)
=============================================================

Une:

* **pinterestGeral**  – métricas por *pin* (`pin_id`)  
* **pinterest{Idade|Genero|Regiao}** – métricas demográficas por *campanha*

Chave‐ponte: **campaign_id + date**

A lógica:

1. Prepara / saneia os dois DataFrames.
2. Calcula, para cada (campaign_id, date), o peso de cada *pin*
   (impressões do pin ÷ impressões totais da campanha no dia).
3. Para cada linha demográfica, redistribui suas métricas a todos
   os *pins* da campanha usando esses pesos (custos com centavos exatos;
   demais métricas como inteiros, preservando a soma).
4. Anexa metadados de cada pin e devolve no esquema do modelo.

Colunas obrigatórias presentes **antes** do merge  
------------------------------------------------
`df_general` : pin_id · campaign_id · date · impressions (+ demais métricas)  
`df_dimension`: campaign_id · date · (age | gender | region) · impressions (+ métricas)

Colunas devolvidas
------------------
date · account_name · ID_Veiculo · Veiculo · ID_Campanha · Campanha  
ad_group_name · ad_name · objective · <dim> · impressions · cost  
link_clicks · video_watched_100 · ID
"""
from __future__ import annotations

import logging
from collections import defaultdict
from math import floor
from typing import Dict, List, Tuple

import pandas as pd
from treat.utils.campos_calculados import (
    calcular_engajamento_total,
    gerar_id,
)
from treat.utils.normalize import normalize_age, normalize_gender
from treat.utils.geo_normalize import (
    obter_estado_de_regiao,
    carregar_caches_padrao,
)

log = logging.getLogger(__name__)

METRICS: List[str] = [
    "impressions",
    "link_clicks",
    "cost",
    "video_watched_100",
]

_CACHE_ESTADOS, _CACHE_MUNIS = carregar_caches_padrao()


# ───────────────────── helpers numéricos ──────────────────────
def _coerce_numeric(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    return df


def _floor_cents(v: float) -> float:
    return floor(v * 100) / 100.0


def _dist_float_total(valor: float, pesos: Dict[str, int]) -> Dict[str, float]:
    """Distribui `valor` em float(2 d.p.) proporcionalmente aos pesos."""
    if valor == 0 or not any(pesos.values()):
        return {k: 0.0 for k in pesos}
    total = sum(pesos.values())
    bruto = {k: v / total * valor for k, v in pesos.items()}
    base = {k: _floor_cents(x) for k, x in bruto.items()}
    resto = round(valor - sum(base.values()), 2)
    if resto:
        frac = {k: bruto[k] - base[k] for k in pesos}
        # distribui centavos restantes para os maiores resíduos
        for k in sorted(frac, key=frac.get, reverse=True)[: int(resto / 0.01)]:
            base[k] += 0.01
    return {k: round(x, 2) for k, x in base.items()}


def _dist_int_total(valor: int, pesos: Dict[str, int]) -> Dict[str, int]:
    """Distribui inteiros preservando soma."""
    if valor == 0 or not any(pesos.values()):
        return {k: 0 for k in pesos}
    total = sum(pesos.values())
    bruto = {k: v / total * valor for k, v in pesos.items()}
    base = {k: int(floor(x)) for k, x in bruto.items()}
    resto = valor - sum(base.values())
    if resto:
        frac = {k: bruto[k] - base[k] for k in pesos}
        for k in sorted(frac, key=frac.get, reverse=True)[: resto]:
            base[k] += 1
    return base


# ───────────────────── preparação de dados ────────────────────
def _prepare_general(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpa `pinterestGeral` – mantém linhas com pin_id, campaign_id e date,
    garante colunas numéricas e strip nos headers.
    """
    df = df.copy()
    df.columns = df.columns.str.strip()
    df = df.dropna(subset=["pin_id", "campaign_id", "date"])
    df = _coerce_numeric(df, METRICS)
    return df.reset_index(drop=True)


def _prepare_dimension(df: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
    """
    Limpa aba demográfica e devolve (df_limpo, nome_coluna_dimensão).
    Detecta coluna entre age, gender, region.
    Normaliza valores básicos.
    """
    df = df.copy()
    df.columns = df.columns.str.strip()
    dim_col = next((c for c in ("age", "gender", "region") if c in df.columns), None)
    if dim_col is None:
        raise KeyError("Aba demográfica não contém 'age', 'gender' ou 'region'.")

    # descarta impressões zero – não contribuem para redistribuição
    df = df.loc[df["impressions"].astype(float) > 0]

    df = df.dropna(subset=["campaign_id", "date", dim_col]).reset_index(drop=True)

    if dim_col == "age":
        df[dim_col] = df[dim_col].apply(normalize_age)
    elif dim_col == "gender":
        df[dim_col] = df[dim_col].apply(normalize_gender)
    else:  # region
        df[dim_col] = df[dim_col].apply(
            lambda v: obter_estado_de_regiao(v, _CACHE_MUNIS, _CACHE_ESTADOS)
        )

    df = _coerce_numeric(df, METRICS)
    return df, dim_col


# ───────────────── peso de cada pin dentro da campanha ─────────
def _build_weights(df_general: pd.DataFrame) -> Dict[Tuple[str, str], Dict[str, int]]:
    """
    Retorna:
        { (campaign_id, date) : { pin_id : impressions_int } }
    """
    grp = (
        df_general.groupby(["campaign_id", "date", "pin_id"], as_index=False)["impressions"]
        .sum()
    )
    weights: Dict[Tuple[str, str], Dict[str, int]] = defaultdict(dict)
    for row in grp.itertuples(index=False):
        key = (row.campaign_id, row.date)
        weights[key][row.pin_id] = int(row.impressions)

    # remove campanhas sem nenhuma impressão (>0 já garantido)
    return {k: v for k, v in weights.items() if any(v.values())}


# ───────────────────────── merge & redistribuição ──────────────
def merge_pinterest_dimension(
    *,
    df_general: pd.DataFrame,
    df_dimension: pd.DataFrame,
) -> pd.DataFrame:
    """
    Executa o merge e devolve o DataFrame pronto para seguir no pipeline.
    """
    df_gen = _prepare_general(df_general)
    df_dim, dim_col = _prepare_dimension(df_dimension)
    weights = _build_weights(df_gen)

    # metadados do pin (mantém apenas 1 linha por pin)
    meta_cols = [
        "pin_id",
        "account_name",
        "ID_Veiculo",
        "Veiculo",
        "ID_Campanha",
        "Campanha",
        "ad_group_name",
        "ad_name",
        "objective",
    ]
    df_meta = (
        df_gen[meta_cols]
        .drop_duplicates(subset=["pin_id"])
        .set_index("pin_id")
    )

    out_rows: list[dict] = []

    for row in df_dim.itertuples(index=False):
        key = (row.campaign_id, row.date)
        if key not in weights:
            # não há pins para essa campanha-data – ignora
            continue
        w_pins = weights[key]

        # distribuições
        dist_cost = _dist_float_total(float(row.cost), w_pins)
        dist_ints = {
            "impressions": _dist_int_total(int(row.impressions), w_pins),
            "link_clicks": _dist_int_total(int(row.link_clicks), w_pins),
            "video_watched_100": _dist_int_total(int(row.video_watched_100), w_pins),
        }

        for pin_id in w_pins:
            meta = df_meta.loc[pin_id] if pin_id in df_meta.index else {}

            out_rows.append(
                {
                    # dimensões chave
                    "date":          row.date,
                    dim_col:         getattr(row, dim_col),
                    "pin_id":        pin_id,
                    # métricas redistribuídas
                    "impressions":        dist_ints["impressions"][pin_id],
                    "cost":               dist_cost[pin_id],
                    "link_clicks":        dist_ints["link_clicks"][pin_id],
                    "video_watched_100":  dist_ints["video_watched_100"][pin_id],
                    # ---- metadados herdados do pin -----------------
                    **{c: meta.get(c, "") for c in meta_cols if c != "pin_id"},
                }
            )

    df_out = pd.DataFrame(out_rows)

    # ------------ pós-processo / ordem de colunas ------------
    final_cols = [
        "date",
        "account_name",
        "ID_Veiculo",
        "Veiculo",
        "ID_Campanha",
        "Campanha",
        "ad_group_name",
        "ad_name",
        "objective",
        dim_col,
        "impressions",
        "cost",
        "link_clicks",
        "video_watched_100",
        "pin_id",
    ]
    for c in final_cols:
        if c not in df_out.columns:
            df_out[c] = 0 if c in ("impressions", "link_clicks") else ""

    df_out = df_out[final_cols].rename(columns={"pin_id": "ID"}).copy()

    # calcula engajamento & ID final
    df_out = calcular_engajamento_total(df_out)
    df_out["ID"] = df_out.apply(gerar_id, axis=1)

    log.info(
        "✅ merge_pinterest_dimension — %s linhas (%s dimensão)",
        len(df_out),
        dim_col,
    )
    return df_out


__all__ = ["merge_pinterest_dimension"]
