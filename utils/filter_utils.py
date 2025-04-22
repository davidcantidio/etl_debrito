import pandas as pd


def remove_zero_impressoes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove todas as linhas cujo valor na coluna 'Impressoes' seja zero ou nulo.

    Parâmetros:
        df (pd.DataFrame): DataFrame de entrada com a coluna 'Impressoes'.

    Retorna:
        pd.DataFrame: DataFrame filtrado, sem linhas onde 'Impressoes' == 0.
    """
    if 'Impressoes' not in df.columns:
        # Se não existir, nada a fazer
        return df

    # Garante que 'Impressoes' é numérico
    df = df.copy()
    df['Impressoes'] = pd.to_numeric(df['Impressoes'], errors='coerce').fillna(0)

    # Filtra somente valores > 0
    return df[df['Impressoes'] > 0]
