from __future__ import annotations

"""Pinterest demographic merge (Idade/Gênero/Região) — versão corrigida
------------------------------------------------------------------------
Une pinterestGeral (métricas por pin) com pinterestIdade / pinterestGenero /
pinterestRegiao (métricas demográficas por campanha), redistribuindo
proporcionalmente as métricas aos pins e PRESERVANDO colunas de contexto.
"""

import logging
from math import floor
from typing import Dict, List, Tuple

import pandas as pd
from treat.utils.normalize import convert_numeric_columns

log = logging.getLogger(__name__)

# --------------------------------------------------------------------- #
#  Constantes                                                           #
# --------------------------------------------------------------------- #
_METRICS: Tuple[str, ...] = (
    "impressions",
    "link_clicks",
    "cost",
    "video_watched_100",
)

# Campos que queremos manter no resultado final
_DESIRED_BASE: Tuple[str, ...] = (
    "pin_id",
    "campaign_id",
    "campaign_name",
    "account_name",
    "ID_Veiculo",
    "Veiculo",
    "ID_Campanha",
    "Campanha",
    "ad_group_name",
    "ad_name",
    "objective",
    "utm_content",
)

# Campos obrigatórios (precisam existir no output, mesmo que vazios)
_MANDATORY: Tuple[str, ...] = (
    "account_name",
    "Veiculo",
    "ID_Veiculo",
    "ID_Campanha",
    "objective",
)

# Layout desejado para cada dimensão  (já inclui os obrigatórios)
_FINAL_ORDER: dict[str, List[str]] = {
    "age": [
                "date",
        *_MANDATORY,
        "Campanha",
        "ad_group_name",
        "ad_name",
        "age",
        *_METRICS,
        "campaign_id",
        "campaign_name",
        "pin_id",
    ],
    "gender": [
        "date",
        *_MANDATORY,
        "Campanha",
        "ad_group_name",
        "ad_name",
        "gender",
        *_METRICS,
        "campaign_id",
        "campaign_name",
        "pin_id",
    ],

    "region": [
        "Numero",
        "date",
        *_MANDATORY,
        "Campanha",
        "ad_group_name",
        "ad_name",
        "region",
        *_METRICS,
        "campaign_id",
        "campaign_name",
        "pin_id",
    ],
}

# --------------------------------------------------------------------- #
#  Helpers                                                              #
# --------------------------------------------------------------------- #
def _coerce_numeric(df: pd.DataFrame, cols: Tuple[str, ...]) -> pd.DataFrame:
    """Converte colunas numéricas respeitando vírgula decimal."""
    return convert_numeric_columns(df, list(cols))


def _floor_cents(v: float) -> float:
    return floor(v * 100) / 100.0


def _dist_float_total(valor: float, pesos: Dict[str, float]) -> Dict[str, float]:
    """Distribui valor monetário preservando centavos."""
    if valor == 0 or not any(pesos.values()):
        return {k: 0.0 for k in pesos}
    total = sum(pesos.values())
    bruto = {k: v / total * valor for k, v in pesos.items()}
    base = {k: _floor_cents(x) for k, x in bruto.items()}
    resto = round(valor - sum(base.values()), 2)
    if resto:
        frac = {k: bruto[k] - base[k] for k in pesos}
        for k in sorted(frac, key=frac.get, reverse=True)[: int(resto / 0.01)]:
            base[k] += 0.01
    return {k: round(x, 2) for k, x in base.items()}


def _dist_int_total(valor: int, pesos: Dict[str, float]) -> Dict[str, int]:
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


# --------------------------------------------------------------------- #
#  Preparação das abas                                                  #
# --------------------------------------------------------------------- #
def _prepare_general(df: pd.DataFrame) -> pd.DataFrame:
    keep = set(_DESIRED_BASE) | {"date", *_METRICS}
    df = df.copy()
    df.columns = df.columns.str.strip()
    df = df[[c for c in df.columns if c in keep]]

    # Garante coluna 'utm_content' vazia se inexistente na origem
    if "utm_content" not in df.columns:
        df["utm_content"] = ""
    

    # 🔑 normaliza chaves antes do dropna
    for c in ("campaign_id", "date"):
        if c in df.columns:
            s = df[c].astype(str).str.strip()
            s = s.str.replace(r"\.0+$", "", regex=True)
            df[c] = s.replace({"": pd.NA})

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df.dropna(subset=["pin_id", "campaign_id", "date"]).reset_index(drop=True)
    return _coerce_numeric(df, _METRICS)


def _prepare_dimension(df: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    """Normaliza aba demográfica sem descartar linhas com métricas zeradas."""
    dim_col = next((c for c in ("age", "gender", "region") if c in df.columns), None)
    if dim_col is None:
        raise KeyError("Dimensão sem coluna age/gender/region.")

    df = df.copy()
    df.columns = df.columns.str.strip()

    # ── 1) Converter strings vazias em NaN para evitar dropna indevido ────
    for c in ("campaign_id", "date", dim_col):
        if c in df.columns:
            s = df[c].astype(str).str.strip()
            s = s.str.replace(r"\.0+$", "", regex=True)
            df[c] = s.replace({"": pd.NA})

    # ── 2) Garante que todas as métricas existam, preenchendo com 0 se faltar ─
    for m in _METRICS:
        if m not in df.columns:
            df[m] = 0

        # ── 3) Preenche valores ausentes da dimensão e descarta apenas chaves nulas ─
    if dim_col in df.columns:
        df[dim_col] = df[dim_col].fillna("Não classificado")

    df = df.dropna(subset=["campaign_id", "date"]).reset_index(drop=True)


    if "campaign_id" in df.columns:
         df["campaign_id"] = df["campaign_id"].astype(str).str.strip()
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    if dim_col in df.columns:
        df[dim_col] = df[dim_col].astype(str).str.strip()



    # ── 4) Coerce as métricas para numérico (sem descartar linhas) ─
    df = _coerce_numeric(df, _METRICS)
    keep = {"campaign_id", "date", dim_col, *_METRICS}
    df = df[[c for c in df.columns if c in keep]]


    log.info("📊 %s → %d linhas após _prepare_dimension", dim_col, len(df))
    return df, dim_col

# --------------------------------------------------------------------- #
#  Cálculo de pesos por pin                                             #
# --------------------------------------------------------------------- #
def _build_weights(df_gen: pd.DataFrame) -> Dict[Tuple[str, str], Dict[str, float]]:
    """Retorna pesos por pin dentro de cada campanha e data."""
    weights: Dict[Tuple[str, str], Dict[str, float]] = {}
    for (cid, dt), grp in df_gen.groupby(["campaign_id", "date"]):
        total_imp = grp["impressions"].sum()
        if total_imp > 0:
            base = grp.set_index("pin_id")["impressions"].astype(float)
        else:
            total_cost = grp["cost"].sum()
            if total_cost > 0:
                base = grp.set_index("pin_id")["cost"].astype(float)
            else:
                base = pd.Series(0.0, index=grp["pin_id"])
        weights[(cid, dt)] = base.to_dict()
    return weights
# --------------------------------------------------------------------- #
#  Merge principal                                                      #
# --------------------------------------------------------------------- #
def merge_pinterest_dimension(
    *, df_general: pd.DataFrame, df_dimension: pd.DataFrame
) -> pd.DataFrame:
    
    # 1) normalização
    df_gen = _prepare_general(df_general)
    df_dim, dim_col = _prepare_dimension(df_dimension)
    # 2) metadados por pin e por campanha
    meta_pin = (
        df_gen[
            [
                "pin_id",
                "campaign_id",
                "campaign_name",
                "ad_group_name",
                "ad_name",
                "utm_content",
            ]
        ]
        .drop_duplicates("pin_id")
        .set_index("pin_id")
    )
    meta_campaign = (
        df_gen[[
            "campaign_id",
            "account_name",
            "Veiculo",
            "ID_Veiculo",
            "ID_Campanha",
            "Campanha",
            "objective",
        ]]
        .drop_duplicates("campaign_id")
        .set_index("campaign_id")

    )

    # 3) pesos por pin
    weights = _build_weights(df_gen)

    out_rows: List[dict] = []

    for row in df_dim.itertuples(index=False):
        key = (row.campaign_id, row.date)
        if key not in weights:
            continue
        w_pins = weights[key]

        dist_cost = _dist_float_total(float(row.cost), w_pins)
        dist_ints = {
            "impressions": _dist_int_total(int(row.impressions), w_pins),
            "link_clicks": _dist_int_total(int(row.link_clicks), w_pins),
            "video_watched_100": _dist_int_total(int(row.video_watched_100), w_pins),
        }

        for pin_id in w_pins:
            pin_meta = meta_pin.loc[pin_id] if pin_id in meta_pin.index else {}

            out_rows.append(
                {
                    "campaign_id": row.campaign_id,
                    "date": row.date,
                    dim_col: getattr(row, dim_col),
                    "pin_id": pin_id,
                    "campaign_name": pin_meta.get("campaign_name", ""),
                    "ad_group_name": pin_meta.get("ad_group_name", ""),
                    "ad_name": pin_meta.get("ad_name", ""),
                    "utm_content": pin_meta.get("utm_content", ""),
                    "impressions": dist_ints["impressions"][pin_id],
                    "cost": dist_cost[pin_id],
                    "link_clicks": dist_ints["link_clicks"][pin_id],
                    "video_watched_100": dist_ints["video_watched_100"][pin_id],
                }
            )

    df_temp = pd.DataFrame(out_rows)
    if df_temp.empty:
        log.warning("Demographic sheet produced no usable rows.")
        return pd.DataFrame(columns=_FINAL_ORDER[dim_col])

    # 4) junta metadados de campanha
    df = df_temp.merge(
        meta_campaign,
        left_on="campaign_id",
        right_index=True,
        how="left",
        validate="many_to_one",
    )

     # preenche valores ausentes das colunas BI
    for c in meta_campaign.columns:
        if c in df.columns:
            if pd.api.types.is_numeric_dtype(meta_campaign[c]):
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
            else:
                df[c] = df[c].fillna("Não identificado")


    if "campaign_id" in df.columns:
        df["campaign_id"] = df["campaign_id"].astype(str).str.strip()

    # validação de soma por campanha
    try:
        orig_tot = df_gen.groupby(["campaign_id", "date"])[list(_METRICS)].sum()
        new_tot = df.groupby(["campaign_id", "date"])[list(_METRICS)].sum()
        for idx, row in orig_tot.iterrows():
            if idx not in new_tot.index:
                continue
            for m in _METRICS:
                o = row[m]
                d = new_tot.loc[idx, m]
                if o == 0:
                    continue
                if abs(o - d) / o > 0.001:
                    log.warning(
                        "Divergência %s em %s: %s vs %s", m, idx, o, d
                    )
        log.info("Validação de soma concluída para %d grupos", len(orig_tot))
    except Exception as exc:
        log.warning("Validação de soma falhou: %s", exc)

    # Mantém linhas mesmo que todas as métricas sejam zero –
    # a ausência de valores será tratada a jusante

    # 8) region → Numero
    if dim_col == "region":
        df.insert(0, "Numero", range(1, len(df) + 1))

    # garantia final de que 'utm_content' exista para evitar warnings no pipeline
    if "utm_content" not in df.columns:
        df["utm_content"] = ""

    log.info("✅ merge_pinterest_dimension – %d linhas (%s)", len(df), dim_col)
    return df

__all__ = ["merge_pinterest_dimension"]
