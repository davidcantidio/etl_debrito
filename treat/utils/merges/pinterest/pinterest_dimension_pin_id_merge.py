from __future__ import annotations

"""Pinterest demographic merge (Idade/Gênero/Região)
----------------------------------------------------
Une a aba **pinterestGeral** (métricas por pin) com
**pinterestIdade | pinterestGenero | pinterestRegiao** (métricas demográficas
por campanha), redistribuindo proporcionalmente as métricas aos pins.
Todo o pré-processamento demográfico já deve ter ocorrido in-place.
"""
import logging
from typing import List, Tuple

import pandas as pd
from treat.utils.campos_calculados import calcular_engajamento_total, gerar_id

log = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
#  Constantes                                                                  #
# --------------------------------------------------------------------------- #
_METRICS: Tuple[str, ...] = (
    "impressions",
    "link_clicks",
    "cost",
    "video_watched_100",
)

_DESIRED_BASE: Tuple[str, ...] = (
    "pin_id", "campaign_id", "campaign_name", "account_name",
    "ID_Veiculo", "Veiculo", "ID_Campanha", "Campanha",
    "ad_group_name", "ad_name", "objective", "utm_content",
)

_FINAL_ORDER: dict[str, List[str]] = {
    "age": [
        "date", "account_name", "ID_Veiculo", "Veiculo", "ID_Campanha",
        "Campanha", "ad_group_name", "ad_name", "objective", "age",
        "impressions", "cost", "link_clicks", "video_watched_100",
        "campaign_id", "campaign_name", "pin_id",
    ],
    "gender": [
        "date", "account_name", "ID_Veiculo", "Veiculo", "ID_Campanha",
        "Campanha", "ad_group_name", "ad_name", "objective", "gender",
        "impressions", "cost", "link_clicks", "video_watched_100",
        "campaign_id", "campaign_name", "pin_id",
    ],
    "region": [
        "Numero", "date", "account_name", "ID_Veiculo", "Veiculo",
        "ID_Campanha", "Campanha", "ad_group_name", "ad_name", "objective",
        "region", "impressions", "cost", "link_clicks", "video_watched_100",
        "campaign_id", "campaign_name", "pin_id",
    ],
}

# --------------------------------------------------------------------------- #
#  Helpers                                                                     #
# --------------------------------------------------------------------------- #

def _coerce_numeric(df: pd.DataFrame, cols: Tuple[str, ...]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    return df

# --------------------------------------------------------------------------- #
#  Preparação das abas                                                         #
# --------------------------------------------------------------------------- #

def _prepare_general(df: pd.DataFrame) -> pd.DataFrame:
    keep = set(_DESIRED_BASE) | {"date", * _METRICS}
    df = df.copy()
    df.columns = df.columns.str.strip()
    df = df[[c for c in df.columns if c in keep]]
    df = df.dropna(subset=["pin_id", "campaign_id", "date"]).reset_index(drop=True)
    return _coerce_numeric(df, _METRICS)


def _prepare_dimension(df: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    dim_col = next((c for c in ("age", "gender", "region") if c in df.columns), None)
    if dim_col is None:
        raise KeyError("Dimensão sem coluna age/gender/region.")
    df = df.copy()
    df.columns = df.columns.str.strip()
    # assumimos normalização in-place; só garantimos numeric
    for m in _METRICS:
        if m not in df.columns:
            df[m] = 0
    df = df.dropna(subset=["campaign_id", "date", dim_col]).reset_index(drop=True)
    df = _coerce_numeric(df, _METRICS)
    # só dimensões com impressões > 0
    df = df[df["impressions"] > 0]
    return df.reset_index(drop=True), dim_col

# --------------------------------------------------------------------------- #
#  Cálculo de pesos por pin                                                   #
# --------------------------------------------------------------------------- #

def _weights(df_gen: pd.DataFrame) -> pd.DataFrame:
    grp = (
        df_gen.groupby(["campaign_id", "date"], as_index=False)["impressions"].sum()
        .rename(columns={"impressions": "total_impressions"})
    )
    df = df_gen.merge(grp, on=["campaign_id", "date"], how="left")
    df["weight"] = df["impressions"] / df["total_impressions"]
    return df[["campaign_id", "date", "pin_id", "weight"]]

# --------------------------------------------------------------------------- #
#  Merge principal                                                             #
# --------------------------------------------------------------------------- #

def merge_pinterest_dimension(*, df_general: pd.DataFrame, df_dimension: pd.DataFrame) -> pd.DataFrame:
    """Une pinterestGeral + dimensão demográfica vetorizadamente."""
    # prepara ambos DataFrames
    df_gen = _prepare_general(df_general)
    df_dim, dim_col = _prepare_dimension(df_dimension)
    # evita duplicação de pin_id caso esteja presente em df_dim
    df_dim = df_dim.drop(columns=["pin_id"], errors="ignore")

    # metadados de pin
    meta_cols = [c for c in _DESIRED_BASE if c in df_gen.columns]
    df_meta = df_gen[meta_cols].drop_duplicates("pin_id").set_index("pin_id")

    # pesos vetorizados
    df_w = _weights(df_gen)
    # renomeia métricas demográficas
    dim_renamed = df_dim.rename(columns={m: f"{m}_dim" for m in _METRICS})

    # merge dimension + pesos (pin_id virá apenas de df_w)
    df_merge = pd.merge(
        dim_renamed,
        df_w,
        on=["campaign_id", "date"],
        how="inner",
        validate="many_to_many",
    )
    # se aparecerem pin_id_x ou pin_id_y, unifica em pin_id
    if "pin_id_y" in df_merge.columns:
        df_merge["pin_id"] = df_merge["pin_id_y"]
    df_merge = df_merge.drop(columns=["pin_id_x", "pin_id_y"], errors="ignore")

    # merge com metadados de pin (many_to_one)
    df = pd.merge(
        df_merge,
        df_meta,
        left_on="pin_id",
        right_index=True,
        how="left",
        validate="many_to_one",
    )

    # redistribuir métricas
    for m in _METRICS:
        src = f"{m}_dim"
        if m == "cost":
            df[m] = (df[src] * df["weight"]).round(2)
        else:
            df[m] = (df[src] * df["weight"]).round().astype(int)

    # descarta linhas sem métrica
    mask = df[list(_METRICS)].sum(axis=1) > 0
    df = df.loc[mask].copy()

    # campo Numero para region
    if dim_col == "region":
        df.insert(0, "Numero", range(1, len(df) + 1))

    # engajamento + ID sintético
    df = calcular_engajamento_total(df)
    df["ID"] = df.apply(gerar_id, axis=1)

    # ordena colunas finais e retorna
    df_final = df.reindex(columns=_FINAL_ORDER[dim_col], fill_value="")
    log.info("✅ merge_pinterest_dimension – %s linhas (%s)", len(df_final), dim_col)
    return df_final

__all__ = ["merge_pinterest_dimension"]
