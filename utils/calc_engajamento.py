import pandas as pd

def calcular_engajamento_total(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adiciona a coluna 'Engajamento_Total' ao DataFrame com a soma de:
    - Post reactions (Reacoes)
    - Post shares (Compartilhamentos)
    - Post comments (Comentarios)

    Parâmetros:
    - df: DataFrame com colunas de engajamento

    Retorna:
    - DataFrame com a nova coluna 'Engajamento_Total'
    """
    colunas_necessarias = ['Post reactions', 'Post shares', 'Post comments']

    for col in colunas_necessarias:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    df['Engajamento_Total'] = (
        df['Post reactions'] + df['Post shares'] + df['Post comments']
    )

    return df
