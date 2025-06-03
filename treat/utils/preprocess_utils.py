import pandas as pd
from typing import Optional, Tuple, Dict
from gspread import Worksheet
from treat.utils.geo_normalize import normalize_region
from treat.utils.substitute_origin_values import apply_all_origin_substitutions
from treat.utils.atribuicoes_via_lookup import (
    atribuir_veiculo_e_id_meta,
    atribuir_veiculo_por_prefixo,
    atribuir_id_veiculo_generico,
    atribuir_veiculo_por_criativo,
    SourceLookup,
)
from extract.sheets_fetcher import SheetsFetcher
from treat.bi_param_utils import BIParamLookup

__all__ = [
    "normalize_region_column",
    "apply_origin_substitutions",
    "preprocess_origin",
]


def normalize_region_column(
    df: pd.DataFrame,
    col_name: str = "region",
    output_col: Optional[str] = None,
) -> pd.DataFrame:
    """
    Se existir a coluna col_name, normaliza cada valor geográfico usando
    geo_normalize.normalize_region. Escreve em output_col se fornecido,
    caso contrário sobrescreve col_name.
    Retorna um novo DataFrame.
    """
    if col_name not in df.columns:
        return df
    out = df.copy()
    dst = output_col or col_name
    out[dst] = (
        out[col_name]
        .astype(str)
        .str.strip()
        .apply(normalize_region)
    )
    return out


def apply_origin_substitutions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Executa todas as transformações de texto “origem → padrão” sem gravar no Sheets.
    """
    return apply_all_origin_substitutions(
        df,
        write_back=False,
        inplace=False
    )


def preprocess_origin(
    df: pd.DataFrame,
    *,
    worksheet: Optional[Worksheet] = None,
    write_back: bool = True,
) -> pd.DataFrame:
    """
    Pipeline de pré-processamento para abas de origem:
      1) substituições de origem (texto) — grava se write_back=True
      2) normalização de região (se existir coluna 'region')

    Retorna apenas o DataFrame processado.
    """
    # 1) aplica substituições e, se solicitado, faz o write-back
    df2 = apply_all_origin_substitutions(
        df,
        worksheet=worksheet,
        write_back=write_back,
        inplace=True,
    )

    # 2) normaliza a coluna 'region', se existir
    df2 = normalize_region_column(df2, col_name="region")
    return df2


def assign_vehicle_and_id(
    df: pd.DataFrame,
    sheet_name: str,
    fetcher,               # instância de SheetsFetcher
    bi_lookup: BIParamLookup,
) -> pd.DataFrame:
    """
    Preenche 'Veiculo' e 'ID_Veiculo' em df, sem abrir SOURCE/BI de novo.

    - sheet_name: nome da aba (ex.: "metaGeral", "linkedinGeral", etc.).
    - fetcher: a mesma instância de SheetsFetcher usada no pipeline.
    - bi_lookup: instância de BIParamLookup inicializada no pipeline.
    """
    lower = sheet_name.lower()
    source_map = SourceLookup.get_mapping(fetcher)

    if lower.startswith("meta"):
        if "placement" in df.columns:
            return atribuir_veiculo_e_id_meta(df, source_map)
        return atribuir_veiculo_por_prefixo(df, "meta", source_map)

    if lower.startswith("linkedin"):
        df = atribuir_veiculo_por_criativo(df, bi_lookup)
        return atribuir_id_veiculo_generico(df, source_map)

    if lower.startswith("pinterest"):
        df["Veiculo"] = "Pinterest"
        return atribuir_id_veiculo_generico(df, source_map)

    # fallback (TikTok, Twitter, etc.)
    df = atribuir_veiculo_por_prefixo(df, lower, source_map)
    return atribuir_id_veiculo_generico(df, source_map)


def get_sibling_sheet(
    name: str,
    fetcher: SheetsFetcher,
    cache: Dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """
    Lê uma aba “irmã” (outro sheet_name) usando o mesmo fetcher e armazena em cache.

    Parâmetros:
    - name: nome da aba a buscar (ex.: "pinterestGeral").
    - fetcher: instância única de SheetsFetcher que já está autenticada.
    - cache: dicionário que mapeia {nome_aba: DataFrame} para reutilização.

    Retorna:
    - DataFrame lido da aba (armazenado em cache para chamadas futuras).
    """
    if name in cache:
        return cache[name]

    df = fetcher.get([name])[name]
    cache[name] = df
    return df