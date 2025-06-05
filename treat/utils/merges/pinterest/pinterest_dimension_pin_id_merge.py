from __future__ import annotations

"""Pinterest demographic merge (Idade/Gênero/Região) — versão corrigida
------------------------------------------------------------------------
Une pinterestGeral (métricas por pin) com pinterestIdade / pinterestGenero /
pinterestRegiao (métricas demográficas por campanha), redistribuindo
proporcionalmente as métricas aos pins e PRESERVANDO colunas de contexto.
"""

import logging
from typing import List, Tuple

import pandas as pd
from treat.utils.campos_calculados import calcular_engajamento_total, gerar_id

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
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    return df

# --------------------------------------------------------------------- #
#  Preparação das abas                                                  #
# --------------------------------------------------------------------- #
def _prepare_general(df: pd.DataFrame) -> pd.DataFrame:
    keep = set(_DESIRED_BASE) | {"date", *_METRICS}
    df = df.copy()
    df.columns = df.columns.str.strip()
    df = df[[c for c in df.columns if c in keep]]
    # 🔑 unifica tipos – evita mismatch no merge por campaign_id e date
    if "campaign_id" in df.columns:
       df["campaign_id"] = (
            pd.to_numeric(df["campaign_id"], errors="coerce").astype("Int64").astype(str)
        )
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

    # ── 3) Agora sim, removemos linhas onde campaign_id, date ou dim_col sejam NaN ─
    df = df.dropna(subset=["campaign_id", "date", dim_col]).reset_index(drop=True)


    if "campaign_id" in df.columns:
        df["campaign_id"] = (
            pd.to_numeric(df["campaign_id"], errors="coerce").astype("Int64").astype(str)
        )
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    if dim_col in df.columns:
        df[dim_col] = df[dim_col].astype(str).str.strip()


    # ── 4) Coerce as métricas para numérico (até aqui, sem filtrar por impressões) ─
    df = _coerce_numeric(df, _METRICS)
    keep = {"campaign_id", "date", dim_col, *_METRICS}
    df = df[[c for c in df.columns if c in keep]]

    log.info("📊 %s → %d linhas após _prepare_dimension", dim_col, len(df))
    return df, dim_col

# --------------------------------------------------------------------- #
#  Cálculo de pesos por pin                                             #
# --------------------------------------------------------------------- #
def _weights(df_gen: pd.DataFrame) -> pd.DataFrame:
    grp = (
        df_gen.groupby(["campaign_id", "date"], as_index=False)["impressions"]
        .sum()
        .rename(columns={"impressions": "total_impressions"})
    )
    df = df_gen.merge(grp, on=["campaign_id", "date"], how="left")
    df["weight"] = df["impressions"] / df["total_impressions"]
    out = df[["campaign_id", "date", "pin_id", "weight"]].copy()
    out["campaign_id"] = out["campaign_id"].astype(str)
    return out

# --------------------------------------------------------------------- #
#  Merge principal                                                      #
# --------------------------------------------------------------------- #
def merge_pinterest_dimension(
    *, df_general: pd.DataFrame, df_dimension: pd.DataFrame
) -> pd.DataFrame:
    
    # 1) normalização
    df_gen = _prepare_general(df_general)
    df_dim, dim_col = _prepare_dimension(df_dimension)

    # 2) metadados por pin_id  (já temos colunas BI aqui!)
    META_COLS = [
        "pin_id",
        "campaign_id",
        "campaign_name",
        "account_name",
        "Veiculo",
        "ID_Veiculo",
        "ID_Campanha",
        "Campanha",
        "ad_group_name",
        "ad_name",
        "objective",
    ]
    df_meta=(
    df_gen[META_COLS]
    .drop_duplicates("pin_id")
    .set_index("pin_id"))

    # 3) pesos por pin
    df_w = _weights(df_gen)                                # pin_id, campaign_id, date, weight
    dim_renamed = df_dim.rename(columns={m: f"{m}_dim" for m in _METRICS})

    # 4) dimensão × pesos  (traz pin_id do df_w)
    df_merge = dim_renamed.merge(
        df_w, on=["campaign_id", "date"], how="inner", validate="many_to_many"
    )

    # 4a) remove duplicados exceto pin_id e colunas relevantes
    dup = (
        set(df_merge.columns) & set(df_meta.columns)
    ) - {"pin_id", "ad_group_name", "ad_name"}
    if dup:
        df_merge = df_merge.drop(columns=list(dup))

    # 5) junta metadados (many-to-one via pin_id)
    df = df_merge.merge(
        df_meta,
        left_on="pin_id",
        right_index=True,
        how="left",
        validate="many_to_one",
        suffixes=("", "_meta"),
    )

    for col in ("ad_group_name", "ad_name"):
        meta_col = f"{col}_meta"
        if meta_col in df.columns:
            df[col] = df[col].fillna(df[meta_col])
            df = df.drop(columns=[meta_col])

    if "campaign_id" in df.columns:
        df["campaign_id"] = df["campaign_id"].astype(str)


    if "campaign_id" in df.columns:
        df["campaign_id"] = (
            pd.to_numeric(df["campaign_id"], errors="coerce").astype("Int64").astype(str)
        )


    # 6) redistribui métricas
    for m in _METRICS:
        src = f"{m}_dim"
        df[m] = (
            (df[src] * df["weight"])
            .round(2 if m == "cost" else 0)
            .astype(float if m == "cost" else int)
        )

    # 7) remove linhas sem métricas
    df = df[df[list(_METRICS)].sum(axis=1) > 0].copy()

    # 8) region → Numero
    if dim_col == "region":
        df.insert(0, "Numero", range(1, len(df) + 1))

    # 9) engajamento + ID sintético
    df = calcular_engajamento_total(df)
    df["ID"] = df.apply(gerar_id, axis=1)

    # 10) garante campos obrigatórios

    for col in _MANDATORY:
        if col not in df.columns:
            df[col] = ""

    # 11) ordena
    df_final = df.reindex(columns=_FINAL_ORDER[dim_col], fill_value="")

    log.info("✅ merge_pinterest_dimension – %d linhas (%s)", len(df_final), dim_col)
    return df_final

__all__ = ["merge_pinterest_dimension"]
