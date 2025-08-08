from typing import Optional

import pandas as pd

from transform.utils.geo_normalize import normalize_region
from transform.utils.substitute_origin_values import apply_all_origin_substitutions

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
    out[dst] = out[col_name].astype(str).str.strip().apply(normalize_region)
    return out


def apply_origin_substitutions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Executa todas as transformações de texto “origem → padrão” sem gravar no Sheets.
    """
    return apply_all_origin_substitutions(df, write_back=False, inplace=False)


def preprocess_origin(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pipeline de pré-processamento para abas de origem:
      1) substituições de origem (texto)
      2) normalização de região (se existir coluna 'region')

    Retorna apenas o DataFrame processado.
    """
    df2 = apply_origin_substitutions(df)
    df2 = normalize_region_column(df2, col_name="region")
    return df2
