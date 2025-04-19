from __future__ import annotations
import logging
from collections import Counter
from math import floor
from decimal import Decimal, ROUND_DOWN
from typing import Dict, List, Tuple, Union

import pandas as pd
from utils.google_sheets import CREDS_PATH, SPREADSHEET_ID
from utils.get_google_client import get_google_client
from utils.read_sheet_as_dataframe import read_sheet_as_dataframe_range

# Logger setup
log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Métricas padrão para Meta Gênero
METRICAS: List[str] = [
    "Impressions",
    "Link clicks",
    "Cost",
    "Video watches at 100%",
]

# Contador de casos de distribuição especial
DISTRIBUICAO_LOGS: Counter[str] = Counter()

# Cliente e cache compartilhados
_client = get_google_client(CREDS_PATH)
_cache: dict[str, pd.DataFrame] = {}


def _read_sheet(sheet_name: str) -> pd.DataFrame:
    """
    Lê e armazena em cache a aba especificada do Google Sheets.
    """
    if sheet_name not in _cache:
        _cache[sheet_name] = read_sheet_as_dataframe_range(
            _client,
            SPREADSHEET_ID,
            sheet_name=sheet_name,
            range_str="A1:ZZ",
            header_row_index=0,
        )
    return _cache[sheet_name].copy()


def load_and_prepare_meta_gender_data() -> pd.DataFrame:
    """
    Lê a aba 'metaGenero' e garante colunas numéricas para as métricas.
    """
    df = _read_sheet("metaGenero")
    df.columns = df.columns.str.strip()
    df = df.dropna(subset=["Ad ID", "Date", "Gender"]) \
           .reset_index(drop=True)

    # Converte Cost (case-insensitive)
    cost_col = next((c for c in df.columns if c.strip().lower() == "cost"), None)
    if cost_col:
        s = df[cost_col].astype(str)
        s = s.str.replace("\u00a0", "", regex=False)
        s = s.str.replace(r"\.(?=\d{3}(?:\.|,))", "", regex=True)
        s = s.str.replace(",", ".", regex=False)
        df[cost_col] = pd.to_numeric(s, errors="coerce").fillna(0)
        df.rename(columns={cost_col: "Cost"}, inplace=True)

    # Converte demais métricas para numérico
    for col in ["Impressions", "Link clicks", "Video watches at 100%"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    return df


def load_and_prepare_meta_placement_data() -> pd.DataFrame:
    """
    Lê a aba 'metaGeral' e remove entradas sem Ad ID/Date.
    """
    return _read_sheet("metaGeral").dropna(subset=["Ad ID", "Date"]).reset_index(drop=True)


def pivot_meta_gender_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrupa por ['Ad ID', 'Date', 'Gender'] somando as métricas.
    """
    return df.groupby(["Ad ID", "Date", "Gender"], as_index=False)[METRICAS].sum()


def pivot_meta_placement_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pivot das métricas por 'Placement' transformando em colunas '<Placement>_<Métrica>'.
    """
    id_vars = ["Ad ID", "Date"]
    value_vars = [c for c in df.columns if c not in id_vars + ["Placement"]]

    df_piv = (
        df.pivot_table(
            index=id_vars,
            columns="Placement",
            values=value_vars,
            aggfunc="sum",
            fill_value=0,
        )
    )
    df_piv.columns.name = None
    df_piv.columns = [f"{pl}_{m}" for m, pl in df_piv.columns]
    df_piv.reset_index(inplace=True)
    return df_piv


def merge_placement_and_gender_data(
    df_placement: pd.DataFrame,
    df_gender: pd.DataFrame,
) -> pd.DataFrame:
    """
    Faz o merge final entre dados de placement e gênero por ['Ad ID', 'Date'].
    Usa 'inner' para manter somente correspondências.
    """
    return pd.merge(df_placement, df_gender, on=["Ad ID", "Date"], how="inner")


def get_placements(df: pd.DataFrame) -> List[str]:
    """
    Extrai lista de placements presentes (detectando colunas '*_Impressions').
    """
    return sorted({c.rsplit("_", 1)[0] for c in df.columns if c.endswith("_Impressions")})


def compute_pesos_impressao(
    row: pd.Series,
    placements: List[str],
) -> Tuple[Dict[str, int], int]:
    """
    Calcula peso de cada placement com base em impressões.
    Se todos forem zero, atribui peso=1 ao placement com maior métrica.
    """
    pesos = {pl: max(int(row.get(f"{pl}_Impressions", 0)), 0) for pl in placements}
    if not any(pesos.values()):
        top, _ = max(
            ((pl, abs(float(row.get(f"{pl}_{m}", 0))))
             for pl in placements for m in METRICAS),
            key=lambda t: t[1], default=(placements[0], 0)
        )
        pesos[top] = 1
    return pesos, sum(pesos.values())


def _floor_cents(v: float) -> float:
    """
    Arredonda para baixo com duas casas decimais usando Decimal para evitar erros de ponto flutuante.
    """
    return float(Decimal(str(v)).quantize(Decimal("0.01"), rounding=ROUND_DOWN))


def _special_distribution(
    metric: str,
    valor_int: int,
    placements: List[str],
    peso_imp: Dict[str, int],
) -> Tuple[Dict[str, Union[int, float]], bool]:
    """
    Distribuição especial quando valor < número de placements (exceto para Cost).
    """
    if metric == "Cost" or valor_int > len(placements):
        return {}, False

    dist = {pl: 0 for pl in placements}
    if valor_int < len(placements):
        top = max(placements, key=lambda pl: peso_imp.get(pl, 0))
        dist[top] = valor_int
    else:
        for pl in placements[:valor_int]:
            dist[pl] = 1

    DISTRIBUICAO_LOGS[f"{metric}_especial"] += 1
    return dist, True


def _distribute_proportional(
    metric: str,
    valor: Union[int, float],
    placements: List[str],
    peso_imp: Dict[str, int],
) -> Dict[str, Union[int, float]]:
    """
    Distribuição proporcional clássica baseada em peso de impressões.
    """
    total = sum(peso_imp.values())
    if valor == 0 or total == 0:
        return {pl: 0 for pl in placements}

    quots = {pl: (peso_imp[pl] / total) * valor for pl in placements}
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


def _fix_inconsistencies_and_types(
    metric: str,
    distrib: Dict[str, Union[int, float]],
    peso_imp: Dict[str, int],
) -> Dict[str, Union[int, float]]:
    """
    Garante consistência de tipos e detecta distribuições inválidas.
    """
    out: Dict[str, Union[int, float]] = {}
    for pl, v in distrib.items():
        if v > 0 and peso_imp.get(pl, 0) == 0:
            log.critical("Placement %s recebeu cota mas peso 0", pl)
        out[pl] = _floor_cents(v) if metric == "Cost" else int(round(v))
    return out


def distribute_gender_metrics(df_in: pd.DataFrame) -> pd.DataFrame:
    """
    Redistribui as métricas por Gender delegando ao algoritmo de Idade,
    preservando soma global e mantendo a interface para gênero.
    """
    log.info("🚚 Distribuindo métricas por Gender usando distribuidor de Age…")

    # Renomeia 'Gender' para 'Age' para reaproveitar o algoritmo de distribuição de idade
    df_age = df_in.rename(columns={"Gender": "Age"})

    # Importa a função de distribuição de idade
    from utils.common.meta.age_placements_merge import distribute_age_metrics as _dist_age

    # Aplica distribuição
    df_dist = _dist_age(df_age)

    # Renomeia de volta para 'Gender'
    df_dist = df_dist.rename(columns={"Age": "Gender"})

    # Mantém apenas as colunas essenciais
    cols = ["Ad ID", "Date", "Gender", "_Plataforma"] + METRICAS
    return df_dist[cols]
