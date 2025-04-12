import pandas as pd
import logging

def calcular_engajamento_total(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula o campo 'Engajamento_Total' como a soma de Reacoes, Compartilhamentos e Comentarios.
    """
    logging.debug(">>> In calcular_engajamento_total")
    if all(col in df.columns for col in ['Reacoes', 'Compartilhamentos', 'Comentarios']):
        df['Engajamento_Total'] = df['Reacoes'] + df['Compartilhamentos'] + df['Comentarios']
    else:
        df['Engajamento_Total'] = 0
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