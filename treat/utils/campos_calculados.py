import pandas as pd
import logging

def calcular_engajamento_total(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula a coluna 'Engajamento_Total' como a soma de:
    - post_reactions
    - post_shares
    - post_comments

    Caso os campos 'post_shares' ou 'post_comments' estejam ausentes ou vazios,
    assume valor 0.
    """
    for col in ['post_shares', 'post_comments']:
        if col not in df.columns:
            df[col] = 0
        else:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    if 'post_reactions' not in df.columns:
        df['post_reactions'] = 0
    else:
        df['post_reactions'] = pd.to_numeric(df['post_reactions'], errors='coerce').fillna(0)

    df['Engajamento_Total'] = (
        df['post_reactions'] + df['post_shares'] + df['post_comments']
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


def make_id_ponto_de_controle(row: pd.Series) -> str:
    """
    Concatena os campos transformados para gerar um ID único de deduplicação:
    Data|Campanha|Veiculo|Link|Periodo|Agencia|Editoria|Objetivo.
    """
    parts = [
        row.get("Data", ""),
        row.get("Campanha", ""),
        row.get("Veiculo", ""),
        row.get("Link conteúdos impulsionados", ""),
        row.get("Período", ""),
        row.get("Agência", ""),
        row.get("Editoria", ""),
        row.get("Objetivo (aumentar seguidores, melhorar engajamento, etc)", ""),
    ]
    return "|".join(str(p) for p in parts)