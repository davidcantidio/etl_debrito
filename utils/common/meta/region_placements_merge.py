# utils/common/meta/region_placements_merge.py

from __future__ import annotations
import logging
from collections import Counter
from math import floor
from typing import Dict, List, Tuple

import pandas as pd
from utils.google_sheets import CREDS_PATH, SPREADSHEET_ID
from utils.get_google_client import get_google_client
from utils.read_sheet_as_dataframe import read_sheet_as_dataframe_range
from utils.geo_normalize import carregar_caches_padrao, limpeza_basica, obter_estado_de_regiao

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

METRICAS: List[str] = ["Impressions", "Link clicks", "Cost", "Video watches at 100%"]

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
def load_and_prepare_meta_region_data() -> pd.DataFrame:
    """
    Lê a aba 'metaRegiao', trata Province name → Estado com limpeza e mapeamento geográfico,
    e garante colunas numéricas.
    """
    # 1) Leitura e strip de colunas
    client = get_google_client(CREDS_PATH)
    df = read_sheet_as_dataframe_range(
        client, SPREADSHEET_ID,
        sheet_name="metaRegiao",
        range_str="A1:ZZ",
        header_row_index=0
    )
    df.columns = df.columns.str.strip()
    log.debug("Colunas após strip: %s", list(df.columns))

    # 2) Verifica colunas mínimas
    required = {"Ad ID", "Date", "Province name"}
    missing = required - set(df.columns)
    if missing:
        log.error("Colunas obrigatórias faltando em metaRegiao: %s", missing)
        raise KeyError(f"Colunas faltando: {missing}")

    # 3) Remove linhas sem dados críticos
    before = len(df)
    df = df.dropna(subset=required)
    after = len(df)
    log.debug("Linhas antes do dropna: %d, após: %d", before, after)

    # 4) Carrega caches para geo-normalização
    cache_estados, cache_municipios = carregar_caches_padrao()

    # 5) Amostra dos valores brutos
    sample_raw = df["Province name"].astype(str).unique()[:10]
    log.debug("Amostra bruta de Province name: %s", sample_raw)

    # 6) Limpeza básica dos nomes
    df["__prov_clean"] = df["Province name"].astype(str).apply(limpeza_basica)
    sample_clean = df["__prov_clean"].unique()[:10]
    log.debug("Amostra após limpeza_basica: %s", sample_clean)

    # 7) Mapeamento para Estado
    df["Estado"] = df["__prov_clean"].apply(
        lambda txt: obter_estado_de_regiao(txt, cache_municipios, cache_estados)
    )
    sample_mapped = df["Estado"].unique()[:10]
    count_invalid = int((df["Estado"] == "Não identificado").sum())
    log.debug("Amostra após geo-normalização: %s", sample_mapped)
    log.debug("Total 'Não identificado': %d", count_invalid)

    # 8) Descarta coluna auxiliar
    df.drop(columns=["__prov_clean"], inplace=True)

    # 9) Converte Cost (case‑insensitive) e padroniza nome
    cost_col = next((c for c in df.columns if c.lower() == "cost"), None)
    if cost_col:
        s = (
            df[cost_col]
            .astype(str)
            .str.replace("\u00a0", "", regex=False)
            .str.replace(r"\.(?=\d{3}(?:\.|,))", "", regex=True)
            .str.replace(",", ".", regex=False)
        )
        df[cost_col] = pd.to_numeric(s, errors="coerce").fillna(0)
        df.rename(columns={cost_col: "Cost"}, inplace=True)
        log.debug("Coluna de custo padronizada para 'Cost'")

    # 10) Converte demais métricas para numérico
    for m in METRICAS:
        if m in df.columns:
            df[m] = pd.to_numeric(df[m], errors="coerce").fillna(0)
    log.debug("Colunas métricas convertidas para numérico: %s", [m for m in METRICAS if m in df.columns])

    return df
def load_and_prepare_meta_placement_data() -> pd.DataFrame:
    """
    Lê a aba 'metaGeral' e remove linhas sem Ad ID ou Date.
    """
    df = _read_sheet("metaGeral")
    df.columns = df.columns.str.strip()
    return df.dropna(subset=["Ad ID", "Date"])

# ------------------------------ PIVOTS -------------------------------- #
def pivot_meta_region_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrupa métricas por Ad ID, Date e Estado.
    """
    return df.groupby(["Ad ID", "Date", "Estado"], as_index=False)[METRICAS].sum()

def pivot_meta_placement_data(df_placement: pd.DataFrame) -> pd.DataFrame:
    """
    Pivota métricas de placement para colunas <placement>_<métrica>.
    """
    id_vars = ["Ad ID", "Date"]
    value_vars = [c for c in df_placement.columns if c not in id_vars + ["Placement"]]
    pivot_df = df_placement.pivot_table(
        index=id_vars,
        columns="Placement",
        values=value_vars,
        aggfunc="sum",
        fill_value=0,
    )
    pivot_df.columns.name = None
    pivot_df.columns = [f"{pl}_{m}" for m, pl in pivot_df.columns]
    return pivot_df.reset_index()

def merge_placement_and_region_data(
    df_region: pd.DataFrame,
    df_place: pd.DataFrame,
) -> pd.DataFrame:
    """
    Faz inner join entre dados de região e de placement por Ad ID e Date.
    """
    return pd.merge(df_region, df_place, on=["Ad ID", "Date"], how="inner")

# --------------------- REDISTRIBUIÇÃO DE MÉTRICAS --------------------- #
def get_placements(df: pd.DataFrame) -> List[str]:
    """
    Extrai a lista de placements únicos (baseado em colunas que terminam em _Impressions).
    """
    return sorted({c.rsplit("_", 1)[0] for c in df.columns if c.endswith("_Impressions")})

def compute_pesos_impressao(row: pd.Series, placements: List[str]) -> Tuple[Dict[str, int], int]:
    """
    Calcula pesos de impressão por placement; se todos zero, dá peso 1 ao placement com maior métrica.
    """
    pesos = {pl: max(int(row.get(f"{pl}_Impressions", 0)), 0) for pl in placements}
    if not any(pesos.values()):
        top = max(
            placements,
            key=lambda pl: max(abs(float(row.get(f"{pl}_{m}", 0))) for m in METRICAS)
        )
        pesos[top] = 1
    return pesos, sum(pesos.values())

def _floor_cents(v: float) -> float:
    return floor(v * 100) / 100.0

def _distribuir_cost(valor: float, pesos: Dict[str, int]) -> Dict[str, float]:
    """
    Distribui custos proporcionalmente e ajusta centavos.
    """
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
    """
    Distribui contagens (impressions, clicks, etc.) proporcionalmente.
    """
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

def distribute_region_metrics(df_in: pd.DataFrame) -> pd.DataFrame:
    """
    Para cada linha agregada por Estado, redistribui as métricas por placement,
    preservando soma global.
    """
    placements = get_placements(df_in)
    log.debug("Placements detectados: %s", placements)

    # Garante totais
    for m in METRICAS:
        if m not in df_in.columns:
            cols = [f"{pl}_{m}" for pl in placements if f"{pl}_{m}" in df_in.columns]
            df_in[m] = df_in[cols].sum(axis=1)
    log.debug("DataFrame antes da distribuição (primeiras 3 linhas):\n%s", df_in.head(3))

    output_rows: list[dict] = []
    for idx, row in df_in.iterrows():
        pesos, total_pesos = compute_pesos_impressao(row, placements)
        if idx < 3:
            log.debug("Linha %d - pesos: %s", idx, pesos)
        dist = {
            "Cost": _distribuir_cost(float(row["Cost"]), pesos),
            **{m: _distribuir_contagem(int(row[m]), pesos) for m in METRICAS if m != "Cost"}
        }
        if idx < 3:
            log.debug("Linha %d - distribuição: %s", idx, {pl: {m: dist[m][pl] for m in METRICAS} for pl in placements})
        for pl in placements:
            output_rows.append({
                "Ad ID":       row["Ad ID"],
                "Date":        row["Date"],
                "Estado":      row["Estado"],
                "_Plataforma": pl,
                **{m: dist[m][pl] for m in METRICAS},
            })

    df_out = pd.DataFrame(output_rows)
    log.debug("DataFrame após distribuição (primeiras 3 linhas):\n%s", df_out.head(3))

    # Valida soma global
    for m in METRICAS:
        tol = 0.01 if m == "Cost" else 0
        in_sum = df_in[m].sum()
        out_sum = df_out[m].sum()
        if abs(in_sum - out_sum) > tol:
            log.error("Soma divergente em '%s': %s → %s", m, in_sum, out_sum)
            raise AssertionError(f"Soma divergente em '{m}': {in_sum} → {out_sum}")

    log.info("✅ distribute_region_metrics — %s linhas", len(df_out))
    return df_out

__all__ = [
    "METRICAS",
    "load_and_prepare_meta_region_data",
    "load_and_prepare_meta_placement_data",
    "pivot_meta_region_data",
    "pivot_meta_placement_data",
    "merge_placement_and_region_data",
    "distribute_region_metrics",
    "get_placements",
    "compute_pesos_impressao",
]
