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
    for col in ['placement', 'campaign_id', 'campaign_name', 'utm_content']:
        if col in self.df.columns:
            self.df.drop(columns=col, inplace=True)


def gerar_id(row: pd.Series) -> str:
    """
    ID = {data}-{Campanha}-{impressions}-{cost}-{link_clicks}
    Aceita tanto colunas snake_case (inglês) quanto as
    antigas em PT-BR com iniciais maiúsculas.
    """
    def pick(*candidatos: str) -> str:
        for c in candidatos:
            if c in row and str(row[c]).strip():
                return str(row[c]).strip()
        return ""

    parts = [
        pick("date",      "Data"),
        pick("Campanha"),               # só existe em PT mesmo
        pick("impressions", "Impressoes"),
        pick("cost",        "Investimento"),
        pick("link_clicks", "Cliques_no_Link"),
    ]
    return "-".join(parts)

