# utils/numeracao.py
from typing import List, Optional

import pandas as pd


def next_numbers(
    df_dest: Optional[pd.DataFrame], count: int, column: str = "Numero"
) -> List[int]:
    """
    Gera uma lista de números sequenciais iniciando após o maior valor presente
    em df_dest[column]. Se df_dest não contiver a coluna ou estiver vazio,
    inicia em 1.

    Parâmetros:
        df_dest (pd.DataFrame | None): DataFrame destino com a coluna de numeração.
        count (int): quantidade de números a gerar.
        column (str): nome da coluna de numeração.

    Retorna:
        List[int]: sequência de números.
    """
    # Determina ponto de partida
    start = 1
    if df_dest is not None and not df_dest.empty:
        # Localiza coluna ignorando case
        col_dest = None
        for col in df_dest.columns:
            if col.strip().lower() == column.strip().lower():
                col_dest = col
                break
        if col_dest:
            serie = pd.to_numeric(df_dest[col_dest], errors="coerce")
            max_val = serie.max()
            if pd.notna(max_val):
                start = int(max_val) + 1

    # Gera sequência
    return list(range(start, start + count))


def gerar_numeracao(
    df: pd.DataFrame, df_destino: Optional[pd.DataFrame] = None, coluna: str = "Numero"
) -> pd.DataFrame:
    """
    Adiciona ao DataFrame uma coluna de numeração sequencial usando next_numbers.

    Parâmetros:
        df (pd.DataFrame): DataFrame de novos registros.
        df_destino (pd.DataFrame | None): DataFrame já existente com números.
        coluna (str): nome da coluna a ser criada.

    Retorna:
        pd.DataFrame: mesmo df com a coluna de numeração adicionada.
    """
    count = len(df)
    # Gera os números
    numbers = next_numbers(df_destino, count, coluna)
    # Atribui ao DataFrame
    df = df.copy()
    df[coluna] = numbers
    return df
