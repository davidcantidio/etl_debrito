# utils/normalize.py

import unicodedata
import pandas as pd

def normalize_campaign_name(value):
    if not isinstance(value, str):
        return value
    return value.strip().upper()

def normalize_nome(nome):
    if not isinstance(nome, str):
        return ""
    nome = nome.strip().lower()
    nome = unicodedata.normalize("NFKD", nome)
    nome = ''.join([c for c in nome if not unicodedata.combining(c)])
    return nome

def normalize_columns(columns: pd.Index) -> pd.Index:
    """
    Normaliza nomes de colunas:
    - Converte para string
    - Remove espaços extras
    - Substitui quebras de linha
    - Remove acentos
    - Converte para lowercase
    """
    return (
        columns.astype(str)
        .str.strip()
        .str.replace('\n', ' ', regex=False)
        .str.lower()
        .map(lambda x: unicodedata.normalize("NFKD", x))
        .map(lambda x: ''.join(c for c in x if not unicodedata.combining(c)))
    )

def normalize_parametrizacao_values(df: pd.DataFrame, cols: list[str] = None) -> pd.DataFrame:
    """
    Aplica normalização (lowercase, sem acento, strip) nas colunas indicadas.
    Se 'cols' for None, aplica em todas as colunas do DataFrame.
    """
    df = df.copy()

    def normaliza_valor(val):
        if not isinstance(val, str):
            return ""
        val = val.strip().lower()
        val = unicodedata.normalize("NFKD", val)
        return ''.join(c for c in val if not unicodedata.combining(c))

    target_cols = cols if cols else df.columns
    for col in target_cols:
        if col in df.columns:
            df[col] = df[col].apply(normaliza_valor)

    return df

def normalizar_faixa_etaria(idade: str) -> str:
    """
    Normaliza a faixa etária: transforma '55-64' e '65+' em '55+'
    """
    if idade in ["55-64", "65+"]:
        return "55+"
    return idade


def inferir_veiculo_meta_por_placement(df: pd.DataFrame) -> pd.DataFrame:
    """
    Define a coluna 'Veiculo' com base no conteúdo da coluna 'Placement'.
    Regras:
        - Se contiver 'facebook' ou 'audience': 'Facebook'
        - Se contiver 'instagram': 'Instagram'
        - Caso contrário: 'Meta'
    """
    def extrair_veiculo(placement):
        if not isinstance(placement, str):
            return "Meta"
        placement = placement.lower()
        if "facebook" in placement or "audience" in placement:
            return "Facebook"
        elif "instagram" in placement:
            return "Instagram"
        return "Facebook"

    df = df.copy()
    df['Veiculo'] = df.get('Placement', "").apply(extrair_veiculo)
    return df

