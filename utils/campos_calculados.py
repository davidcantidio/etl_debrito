import pandas as pd
import logging

def calcular_engajamento_total(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula a coluna 'Engajamento_Total' como a soma de:
    - Reacoes
    - Compartilhamentos
    - Comentarios

    Caso os campos 'Compartilhamentos' ou 'Comentarios' estejam ausentes ou vazios,
    assume valor 0.
    """
    for col in ['Compartilhamentos', 'Comentarios']:
        if col not in df.columns:
            df[col] = 0
        else:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    if 'Reacoes' not in df.columns:
        df['Reacoes'] = 0
    else:
        df['Reacoes'] = pd.to_numeric(df['Reacoes'], errors='coerce').fillna(0)

    df['Engajamento_Total'] = (
        df['Reacoes'] + df['Compartilhamentos'] + df['Comentarios']
    )

    return df


def inicializar_colunas_auxiliares(df: pd.DataFrame) -> pd.DataFrame:
    """
    Garante que as colunas auxiliares 'Numero' e 'ID' existam no DataFrame.
    """
    logging.debug(">>> In inicializar_colunas_auxiliares")
    df['Numero'] = df.get('Numero', pd.NA)
    df['ID'] = df.get('ID', pd.NA)
    return df


def remover_colunas_indesejadas(self):
    for col in ['Placement', 'Campaign_ID', 'Campaign_name', 'Content_utm']:
        if col in self.df.columns:
            self.df.drop(columns=col, inplace=True)

def gerar_id(df: pd.DataFrame) -> pd.DataFrame:
    """
    Gera a coluna 'ID' a partir da concatenação de campos-chave.
    """
    df = df.copy()
    df['ID'] = df.apply(
        lambda row: f"{row['Data']}-{row['Campanha']}-{row['Impressoes']}-{row['Investimento']}-{row['Cliques_no_Link']}",
        axis=1
    )
    return df