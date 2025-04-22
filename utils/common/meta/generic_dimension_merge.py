from collections import Counter
import logging
from typing import List, Dict, Tuple
import pandas as pd
from utils.google_sheets import CREDS_PATH, SPREADSHEET_ID
from utils.get_google_client import get_google_client
from utils.read_sheet_as_dataframe import read_sheet_as_dataframe_range
from utils.normalize import parse_decimal_str_to_float, format_float_to_decimal_str
from math import floor

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

METRICAS: List[str] = ["Impressions", "Cost", "Video watches at 100%", "Link clicks"]

# ------------------------------- CACHE -------------------------------- #
_client = get_google_client(CREDS_PATH)
_cache: dict[str, pd.DataFrame] = {}

def _read_sheet(sheet: str, dimension_col: str = None) -> pd.DataFrame:
    """
    Lê a aba da planilha Google e retorna os dados, adaptando as colunas para diferentes dimensões.
    
    :param sheet: Nome da aba a ser lida (como "metaIdade", "metaGenero", etc.)
    :param dimension_col: (Opcional) Nome da coluna de dimensão (como 'Age', 'Gender', 'Region') para ajustar o DataFrame
    :return: DataFrame com os dados da aba
    """
    # Verifica se a aba já está em cache
    if sheet not in _cache:
        # Carrega os dados da planilha
        _cache[sheet] = read_sheet_as_dataframe_range(
            _client,
            SPREADSHEET_ID,
            sheet_name=sheet,
            range_str="A1:ZZ",
            header_row_index=0,
        )
    
    df = _cache[sheet].copy()

    # Se houver uma coluna de dimensão especificada (como 'Age', 'Gender', etc.), ajusta o DataFrame
    if dimension_col:
        if dimension_col not in df.columns:
            log.warning(f"A coluna de dimensão '{dimension_col}' não foi encontrada na planilha '{sheet}'!")
        else:
            # Normaliza os valores da coluna de dimensão
            df[dimension_col] = df[dimension_col].apply(normalize_nome)  # Supondo que 'normalize_nome' seja uma função definida

    return df

# ------------------------------- LOGGING DE DISTRIBUIÇÃO ---------------------- #
DISTRIBUICAO_LOGS: Counter[str] = Counter()  # Contador de casos especiais durante redistribuição

# ------------------------- PREPARAÇÃO DE DADOS ------------------------ #
def load_and_prepare_meta_dimension_data(dimension_col: str) -> pd.DataFrame:
    """
    Carrega e prepara os dados de uma dimensão (Age, Gender, Region), e os dados de placement.
    
    :param dimension_col: Nome da coluna de dimensão (como 'Age', 'Gender', 'Region').
    :return: DataFrame com os dados de dimension e placement preparados.
    """
    df = _read_sheet("metaIdade")  # Ou "metaGeral" dependendo da dimensão

    # 0) Strip em todos os nomes de coluna
    df.columns = df.columns.str.strip()
    
    # 1) Drop obrigatórios
    df = df.dropna(subset=["Ad ID", "Date", dimension_col])
    
    # 2) Processar Cost de forma dinâmica
    cost_col = next((c for c in df.columns if c.lower() == "cost"), None)
    if cost_col:
        s = df[cost_col].astype(str)
        s = s.str.replace("\u00a0", "")
        s = s.str.replace(r"\.(?=\d{3}(?:\.|,))", "", regex=True)
        s = s.str.replace(",", ".", regex=False)
        df[cost_col] = pd.to_numeric(s, errors="coerce").fillna(0)
        df.rename(columns={cost_col: "Cost"}, inplace=True)

    # 3) Outras métricas
    for col in ["Impressions", "Link clicks", "Video watches at 100%"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    
    return df


def load_and_prepare_meta_placement_data() -> pd.DataFrame:
    """
    Carrega e prepara os dados de placement (metaGeral).
    """
    df = _read_sheet("metaGeral")
    df = df.dropna(subset=["Ad ID", "Date"])  # Garantir que as colunas 'Ad ID' e 'Date' não estejam vazias   
    # Garantir que todas as métricas estejam presentes e sejam convertidas para numéricas
    for col in METRICAS:
        if col not in df.columns:
            log.warning(f"Coluna {col} não encontrada no DataFrame de metaGeral!")
        else:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df

def compute_impression_weights(row: pd.Series, placements: list[str]) -> Tuple[dict[str, int], int]:
    """
    Calcula os pesos com base nas Impressoes por placement.

    - A função calcula os pesos das impressões de cada placement.
    - Se todos os pesos forem zero, atribui peso 1 ao placement com maior métrica.
    
    Parâmetros:
    - row: linha do DataFrame, contendo os valores das impressões.
    - placements: lista com os nomes dos placements, extraídos do DataFrame.
    
    Retorna:
    - pesos: dicionário contendo o peso de cada placement.
    - total: soma dos pesos.
    """
    
    pesos = {pl: max(int(row.get(f"{pl}_Impressions", 0)), 0) for pl in placements}

    # Se todos os pesos forem zero, atribui peso 1 ao placement com a maior métrica
    if not any(pesos.values()):
        top_pl = max(
            placements,
            key=lambda pl: max(abs(float(row.get(f"{pl}_{m}", 0))) for m in METRICAS),
        )
        pesos[top_pl] = 1
    return pesos, sum(pesos.values())

# ------------------------------ PIVOTS -------------------------------- #
def pivot_meta_dimension_data(df: pd.DataFrame, dimension_col: str) -> pd.DataFrame:
    """
    Agrupa os dados por Ad ID, Date e a coluna de dimensão fornecida (Age, Gender, Region) e soma as métricas.
    """
    # Verificando se a coluna de métricas está presente
    for col in METRICAS:
        if col not in df.columns:
            log.warning(f"Coluna '{col}' não encontrada para o agrupamento!")
            df[col] = 0  # Adiciona uma coluna com valores 0 caso não exista

    # Agrupando pelos valores de 'Ad ID', 'Date' e a dimensão específica (Age, Gender, Region)
    return df.groupby(["Ad ID", "Date", dimension_col], as_index=False)[METRICAS].sum()


def pivot_meta_placement_data(df_placement: pd.DataFrame) -> pd.DataFrame:
    id_vars = ["Ad ID", "Date"]
    value_vars = [c for c in df_placement.columns if c not in id_vars + ["Placement"]]

    pivot_df = (
        df_placement.pivot_table(
            index=id_vars,
            columns="Placement",
            values=value_vars,
            aggfunc="sum",
            fill_value=0,
        )
    )
    # Remove o name do eixo de colunas
    pivot_df.columns.name = None
    # Achata MultiIndex  ->  "<placement>_<metric>"
    pivot_df.columns = [f"{pl}_{m}" for m, pl in pivot_df.columns]
    return pivot_df.reset_index()

def get_placements(df: pd.DataFrame) -> List[str]:
    """
    Extrai os placements a partir dos nomes das colunas com a sufixação '_Impressions'.
    """
    return sorted({c.rsplit("_", 1)[0] for c in df.columns if c.endswith("_Impressions")})


# ------------------------- MERGE E DISTRIBUIÇÃO ------------------------ #
def merge_placement_and_dimension_data(
    df_dimension: pd.DataFrame, 
    df_place: pd.DataFrame, 
    dimension_col: str
) -> pd.DataFrame:
    """
    Realiza o merge entre os dados de uma dimensão (como Age, Gender, Region) e dados de placement.
    
    :param df_dimension: DataFrame contendo os dados da dimensão (Age, Gender, Region).
    :param df_place: DataFrame contendo os dados de placement (com métricas como Impressions, Cost, etc.).
    :param dimension_col: Nome da coluna que representa a dimensão, como 'Age', 'Gender', ou 'Region'.
    :return: DataFrame resultante do merge.
    """
    required_columns_dimension = [dimension_col, "Ad ID", "Date"]
    required_columns_place = ["Ad ID", "Date", "Impressions", "Cost", "Link clicks", "Video watches at 100%"]
    
    # Log de verificação para as colunas de dimensão
    log.debug(f"Verificando colunas em df_dimension: {df_dimension.columns}")
    log.debug(f"Verificando colunas em df_place: {df_place.columns}")
    
    for col in required_columns_dimension:
        if col not in df_dimension.columns:
            raise ValueError(f"Coluna '{col}' não encontrada no DataFrame de dimensão!")

    for col in required_columns_place:
        if col not in df_place.columns:
            raise ValueError(f"Coluna '{col}' não encontrada no DataFrame de placement!")
    
    # Log antes do merge
    log.debug(f"Linhas antes do merge - df_dimension: {df_dimension.shape[0]}")
    log.debug(f"Linhas antes do merge - df_place: {df_place.shape[0]}")
    
    # Realizar o merge, agora considerando a coluna de dimensão dinâmica (Age

def _floor_cents(v: float) -> float:
    """
    Função auxiliar para arredondar os valores para 2 casas decimais.
    """
    return floor(v * 100) / 100.0

def _distribute_costs(valor: float, pesos: Dict[str, int]) -> Dict[str, float]:
    """Distribui o custo entre os placements, com base nos pesos."""
    if valor == 0 or not any(pesos.values()):
        return {pl: 0.0 for pl in pesos}
    
def _distribute_counts(valor: int, pesos: Dict[str, int]) -> Dict[str, int]:
    """Distribui a contagem de métricas entre os placements."""
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
        DISTRIBUICAO_LOGS["resto_counts"] += 1

    return base

def distribute_dimension_metrics(df_in: pd.DataFrame, dimension_col: str) -> pd.DataFrame:
    """
    Redistribui as métricas agregadas por uma dimensão (Age, Region, Gender) para cada placement,
    preservando a soma global.
    """
    placements = get_placements(df_in)

    # Garante colunas‑totais (Impressions, Cost, etc.)
    for m in METRICAS:
        if m not in df_in.columns:
            cols = [f"{pl}_{m}" for pl in placements if f"{pl}_{m}" in df_in.columns]
            df_in[m] = df_in[cols].sum(axis=1)

    output_rows: list[dict] = []  # Lista para armazenar os dados de saída

    for _, row in df_in.iterrows():
        pesos, _ = compute_impression_weights(row, placements)

        # Distribuições por métrica
        dist: Dict[str, Dict[str, float | int]] = {}
        dist["Cost"] = _distribute_cost(float(row["Cost"]), pesos)
        
        for m in ("Impressions", "Link clicks", "Video watches at 100%"):
            dist[m] = _distribute_counts(int(row[m]), pesos)

        # Gera uma linha por placement
        for pl in placements:
            output_rows.append(
                {
                    "Ad ID": row["Ad ID"],
                    "Date": row["Date"],
                    dimension_col: row[dimension_col],  # Pode ser "Age", "Region", ou "Gender"
                    "_Plataforma": pl,
                    **{m: dist[m][pl] for m in METRICAS},
                }
            )

    df_out = pd.DataFrame(output_rows)

    # ---------- Checagem de integridade global ----------
    for m in METRICAS:
        soma_in, soma_out = df_in[m].sum(), df_out[m].sum()
        tol = 0.01 if m == "Cost" else 0
        if abs(soma_in - soma_out) > tol:
            raise AssertionError(
                f"Soma global divergente em '{m}': {soma_in} → {soma_out}"
            )

    log.info(f"✅ distribute_dimension_metrics ({dimension_col}) — {len(df_out)} linhas")
    return df_out
