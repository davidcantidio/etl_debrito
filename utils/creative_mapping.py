import logging
import pandas as pd

def carregar_mapeamento_nome_creativo(df_parametrizacao: pd.DataFrame) -> dict:
    """
    Cria e retorna um dicionário {utm_content -> criativo},
    baseado nas colunas 'utm_content' e 'criativo' na aba BI_PARAMETRIZAÇÃO.
    """
    if df_parametrizacao.empty:
        logging.warning("DataFrame de parametrização vazio. Não foi possível construir o mapeamento.")
        return {}

    required_cols = {'utm_content', 'criativo'}
    colunas_atuais = set(df_parametrizacao.columns)
    if not required_cols.issubset(colunas_atuais):
        logging.warning("Colunas 'utm_content' e/ou 'criativo' não foram encontradas na parametrização.")
        return {}

    mapping = {}
    for _, row in df_parametrizacao.iterrows():
        utm = str(row.get('utm_content', '')).strip()
        criativo = str(row.get('criativo', '')).strip()
        if utm:
            mapping[utm] = criativo

    return mapping


def obter_nome_por_utm_content(utm_content: str, mapping_criativo: dict) -> str:
    """
    Dado um valor de utm_content (ex.: 'abc123') e um dicionário {utm_content -> criativo},
    retorna o 'criativo' correspondente.

    Se não encontrar, retorna string vazia.
    """
    if not isinstance(utm_content, str):
        return ""
    return mapping_criativo.get(utm_content.strip(), "")
