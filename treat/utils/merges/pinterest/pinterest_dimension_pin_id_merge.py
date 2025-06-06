import logging
from math import floor
from typing import Dict, List, Tuple

import pandas as pd
from treat.utils.campos_calculados import calcular_engajamento_total, gerar_id
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

_MANDATORY: Tuple[str, ...] = (
    "account_name",
    "Veiculo",
    "ID_Veiculo",
    "ID_Campanha",
    "objective",
)

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
    return convert_numeric_columns(df, list(cols))


def _floor_cents(v: float) -> float:
    return floor(v * 100) / 100.0


def _dist_float_total(valor: float, pesos: Dict[str, float]) -> Dict[str, float]:
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


def _prepare_general(df: pd.DataFrame) -> pd.DataFrame:
    keep = set(_DESIRED_BASE) | {"date", *_METRICS}
    df = df.copy()
    df.columns = df.columns.str.strip()
    df = df[[c for c in df.columns if c in keep]]

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
    dim_col = next((c for c in ("age", "gender", "region") if c in df.columns), None)
    if dim_col is None:
        raise KeyError("Dimensão sem coluna age/gender/region.")

    df = df.copy()
    df.columns = df.columns.str.strip()

    for c in ("campaign_id", "date", dim_col):
        if c in df.columns:
            s = df[c].astype(str).str.strip()
            s = s.str.replace(r"\.0+$", "", regex=True)
            df[c] = s.replace({"": pd.NA})

    for m in _METRICS:
        if m not in df.columns:
            df[m] = 0

    df = df.dropna(subset=["campaign_id", "date", dim_col]).reset_index(drop=True)

    df["campaign_id"] = df["campaign_id"].astype(str).str.strip()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df[dim_col] = df[dim_col].astype(str).str.strip()

    df = _coerce_numeric(df, _METRICS)
    keep = {"campaign_id", "date", dim_col, *_METRICS}
    df = df[[c for c in df.columns if c in keep]]

    log.info("📊 %s → %d linhas após _prepare_dimension", dim_col, len(df))
    return df, dim_col


def _build_weights(df_gen: pd.DataFrame) -> Dict[Tuple[str, str], Dict[str, float]]:
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


def _weights_to_dataframe(
    weights: Dict[Tuple[str, str], Dict[str, float]]
) -> pd.DataFrame:
    rows: List[dict] = []
    for (campaign_id, date), pin_weights in weights.items():
        if not pin_weights:
            continue
        for pin_id, weight in pin_weights.items():
            rows.append({
                "campaign_id": str(campaign_id).strip(),
                "date": str(date).strip(),
                "pin_id": str(pin_id).strip(),
                "weight": float(weight),
            })

    if not rows:
        return pd.DataFrame(columns=["campaign_id", "date", "pin_id", "weight"])

    df_weights = pd.DataFrame(rows)
    df_weights["campaign_id"] = df_weights["campaign_id"].astype(str).str.strip()
    df_weights["pin_id"] = df_weights["pin_id"].astype(str).str.strip()
    df_weights["date"] = pd.to_datetime(df_weights["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df_weights["weight"] = pd.to_numeric(df_weights["weight"], errors="coerce").fillna(0.0)

    sums = df_weights.groupby(["campaign_id", "date"])["weight"].sum()
    invalid = sums[abs(sums - 1.0) > 1e-6]
    if not invalid.empty:
        for (cid, dt), total in invalid.items():
            log.warning(
                "Pesos somam %s (≠ 1) para campaign_id=%s, date=%s", 
                total, cid, dt
            )

    return df_weights


# --------------------------------------------------------------------- #
#  Merge principal                                                      #
# --------------------------------------------------------------------- #
def merge_pinterest_dimension(
    *, df_general: pd.DataFrame, df_dimension: pd.DataFrame
) -> pd.DataFrame:
    # 1) Normalização das duas abas
    df_gen = _prepare_general(df_general)
    df_dim_raw, dim_col = _prepare_dimension(df_dimension)

    # 2) Extrair metadados de pin e campanha
    meta_pin = (
        df_gen[["pin_id", "campaign_id", "campaign_name", "ad_group_name", "ad_name"]]
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

    # 3) Calcular pesos e converter em DataFrame
    weights = _build_weights(df_gen)
    df_weights = _weights_to_dataframe(weights)

    # 4) Preparar df_dim para vetorização: renomear colunas de totais
    df_dim = df_dim_raw.rename(
        columns={
            "impressions": "impressions_total",
            "cost": "cost_total",
            "link_clicks": "link_clicks_total",
            "video_watched_100": "video_watched_total",
        }
    )

    # 5) Merge vetorizado entre df_dim e df_weights
    df_dim2 = df_dim.merge(
        df_weights,
        on=["campaign_id", "date", "pin_id"],
        how="left",
        validate="many_to_one"
    )

    # 6) Calcular métricas demográficas proporcionais
    # 6a) Inteiros (impressions, link_clicks, video_watched_100)
    df_dim2["impressions_dem"] = (
        df_dim2["impressions_total"] * df_dim2["weight"]
    ).round().astype(int)
    df_dim2["link_clicks_dem"] = (
        df_dim2["link_clicks_total"] * df_dim2["weight"]
    ).round().astype(int)
    df_dim2["video_watched_100_dem"] = (
        df_dim2["video_watched_total"] * df_dim2["weight"]
    ).round().astype(int)

    # 6b) Float (cost) com preservação de centavos
    def _calc_cost_group(grp: pd.DataFrame) -> pd.Series:
        total_cost = grp["cost_total"].iloc[0]
        pesos_dict = grp.set_index("pin_id")["weight"].to_dict()
        dist = _dist_float_total(float(total_cost), pesos_dict)
        return grp["pin_id"].map(dist)

    df_dim2["cost_dem"] = df_dim2.groupby(["campaign_id", "date"])\
        .apply(_calc_cost_group)\
        .reset_index(level=[0, 1], drop=True)

    # 7) Montar df_temp com as colunas finais antes de juntar BI
    df_temp = df_dim2.rename(
        columns={
            "impressions_dem": "impressions",
            "cost_dem": "cost",
            "link_clicks_dem": "link_clicks",
            "video_watched_100_dem": "video_watched_100",
        }
    )[
        [
            "campaign_id",
            "date",
            dim_col,
            "pin_id",
            "impressions",
            "cost",
            "link_clicks",
            "video_watched_100",
        ]
    ]

    if df_temp.empty:
        log.warning("Demographic sheet gerou 0 linhas para %s.", dim_col)
        return pd.DataFrame(columns=_FINAL_ORDER[dim_col])

    # 8) Merge com meta_campaign para adicionar colunas BI
    df = df_temp.merge(
        meta_campaign,
        left_on="campaign_id",
        right_index=True,
        how="left",
        validate="many_to_one",
    )

    # 9) Preencher NaN das colunas BI
    for c in meta_campaign.columns:
        if c in df.columns:
            if pd.api.types.is_numeric_dtype(meta_campaign[c]):
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
            else:
                df[c] = df[c].fillna("Não identificado")

    df["campaign_id"] = df["campaign_id"].astype(str).str.strip()

    # 10) Validação de soma por campanha/data
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
                        "Divergência %s em %s: original=%s vs demográfico=%s",
                        m, idx, o, d
                    )
        log.info("Validação de soma concluída para %d grupos", len(orig_tot))
    except Exception as exc:
        log.warning("Validação de soma falhou: %s", exc)

    # 11) Remover linhas sem nenhuma métrica
    df = df[df[list(_METRICS)].sum(axis=1) > 0].copy()

    # 12) Se dimensão for region, inserir coluna “Numero”
    if dim_col == "region":
        df.insert(0, "Numero", range(1, len(df) + 1))

    # 13) Calcular engajamento total e gerar ID
    df = calcular_engajamento_total(df)
    df["ID"] = df.apply(gerar_id, axis=1)

    # 14) Garantir colunas obrigatórias
    for col in _MANDATORY:
        if col not in df.columns:
            df[col] = ""

    # 15) Reordenar colunas conforme _FINAL_ORDER
    df_final = df.reindex(columns=_FINAL_ORDER[dim_col], fill_value="")

    log.info("✅ merge_pinterest_dimension – %d linhas (%s)", len(df_final), dim_col)
    return df_final


__all__ = ["merge_pinterest_dimension"]
