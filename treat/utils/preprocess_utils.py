from __future__ import annotations

import re
from typing import Dict, Optional

import pandas as pd
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
    "apply_origin_substitutions",
    "normalize_region_column",
    "preprocess_origin",
    "assign_vehicle_and_id",
    "get_sibling_sheet",
]

# ────────────────────────────────────────────────────────────────────────────
# Funções auxiliares de texto
# ────────────────────────────────────────────────────────────────────────────


def apply_origin_substitutions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Executa todas as substituições de texto definidas nos mapeamentos
    (utm_content, ad_name, etc.) **sem** gravar no Google Sheets.
    """
    return apply_all_origin_substitutions(df, write_back=False, inplace=False)


def normalize_region_column(
    df: pd.DataFrame,
    col_name: str = "region",
    output_col: Optional[str] = None,
) -> pd.DataFrame:
    """
    Se existir a coluna *col_name*, normaliza cada valor geográfico usando
    ``geo_normalize.normalize_region``.  
    - Se *output_col* for fornecido, grava o resultado nessa nova coluna;  
      caso contrário, sobrescreve *col_name*.
    """
    if col_name not in df.columns:
        return df

    dst = output_col or col_name
    out = df.copy()
    out[dst] = (
        out[col_name]
        .astype(str)
        .str.strip()
        .apply(normalize_region)
    )
    return out


def _extract_platform(sheet_name: str) -> str:
    """
    Extrai o prefixo da plataforma a partir do *nome da aba*.

    ```
    'tiktokGeral'   -> 'tiktok'
    'metaRegiao'    -> 'meta'
    'linkedinIdade' -> 'linkedin'
    ```

    A regex ``^[a-z]+`` casa apenas a sequência inicial de minúsculas
    **antes** do primeiro caractere maiúsculo – evitando o erro
    “tiktokgeral” que ocorria anteriormente.
    """
    m = re.match(r"^[a-z]+", sheet_name)
    return m.group(0).lower() if m else sheet_name.lower()


# ────────────────────────────────────────────────────────────────────────────
# Pipeline de pré-processamento (substituições + região)
# ────────────────────────────────────────────────────────────────────────────


def preprocess_origin(
    df: pd.DataFrame,
    *,
    worksheet: Optional[Worksheet] = None,
    write_back: bool = True,
) -> pd.DataFrame:
    """
    1. Executa substituições de texto (apply_all_origin_substitutions).  
       – Se *write_back* for ``True``, grava no próprio *worksheet*.

    2. Normaliza a coluna ``region`` (se existir) via geo-normalize.

    Retorna um **novo** DataFrame com as alterações.
    """
    df2 = apply_all_origin_substitutions(
        df,
        worksheet=worksheet,
        write_back=write_back,
        inplace=True,
    )
    df2 = normalize_region_column(df2, col_name="region")
    return df2


# ────────────────────────────────────────────────────────────────────────────
# Enriquecimento de Veiculo / ID_Veiculo (100 % in-memory)
# ────────────────────────────────────────────────────────────────────────────


def assign_vehicle_and_id(
    df: pd.DataFrame,
    *,
    sheet_name: str,
    fetcher: SheetsFetcher,
    bi_lookup: BIParamLookup,
) -> pd.DataFrame:
    """
    Preenche as colunas **Veiculo** e **ID_Veiculo** sem abrir novas
    conexões ao Google Sheets.

    Parâmetros
    ----------
    df : pd.DataFrame
        DataFrame a enriquecer.
    sheet_name : str
        Nome completo da aba (ex.: ``"tiktokGeral"``).
    fetcher : SheetsFetcher
        Instância única reutilizada no pipeline.
    bi_lookup : BIParamLookup
        Lookup já inicializado no pipeline.
    """
    lower = sheet_name.lower()
    platform = _extract_platform(sheet_name)          # ← correção essencial
    source_map = SourceLookup.get_mapping(fetcher)

    # ── Meta (Facebook / Instagram) ───────────────────────────────────────
    if lower.startswith("meta"):
        if "placement" in df.columns:
            return atribuir_veiculo_e_id_meta(df, source_map)
        return atribuir_veiculo_por_prefixo(df, "meta", source_map)

    # ── LinkedIn ──────────────────────────────────────────────────────────
    if lower.startswith("linkedin"):
        df = atribuir_veiculo_por_criativo(df, bi_lookup)
        return atribuir_id_veiculo_generico(df, source_map)

    # ── Pinterest ─────────────────────────────────────────────────────────
    if lower.startswith("pinterest"):
        df["Veiculo"] = "Pinterest"
        return atribuir_id_veiculo_generico(df, source_map)

    # ── Fallback (TikTok, Twitter, YouTube, …) ────────────────────────────
    df = atribuir_veiculo_por_prefixo(df, platform, source_map)
    return atribuir_id_veiculo_generico(df, source_map)


# ────────────────────────────────────────────────────────────────────────────
# Utilitário para ler “abas irmãs” (Pinterest merge, etc.)
# ────────────────────────────────────────────────────────────────────────────


def get_sibling_sheet(
    name: str,
    fetcher: SheetsFetcher,
    cache: Dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """
    Carrega uma aba *name* usando o mesmo ``SheetsFetcher`` já autenticado
    e armazena-a em *cache* para chamadas futuras.
    """
    if name in cache:
        return cache[name]

    df = fetcher.get([name])[name]
    cache[name] = df
    return df
