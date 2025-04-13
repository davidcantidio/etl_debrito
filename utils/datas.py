# utils/datas.py

from datetime import datetime, date
import pandas as pd


def transformar_para_date(valor):
    """
    Transforma um valor de data no formato 'YYYY-MM-DD HH:MM:SS' ou
    um objeto datetime em um objeto date (YYYY-MM-DD).

    Parâmetros:
        valor (str ou datetime ou date): Data no formato "YYYY-MM-DD HH:MM:SS"
                                         ou já um objeto datetime ou date.

    Retorna:
        date: objeto da classe date (ex: 2024-04-15)
    """
    if not valor:
        return None

    if isinstance(valor, date) and not isinstance(valor, datetime):
        return valor

    if isinstance(valor, datetime):
        return valor.date()

    if isinstance(valor, str):
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(valor.strip(), fmt).date()
            except ValueError:
                continue
        raise ValueError(f"Formato de data não reconhecido: {valor}")
    
    raise ValueError(f"Tipo de valor não suportado: {type(valor)}")


def converter_data(df: pd.DataFrame, coluna: str = 'Data') -> pd.DataFrame:
    """
    Converte a coluna especificada do DataFrame para o tipo `date` (sem hora).

    Parâmetros:
        df (pd.DataFrame): DataFrame contendo a coluna de data.
        coluna (str): Nome da coluna a ser convertida.

    Retorna:
        pd.DataFrame: DataFrame com a coluna convertida para `date`.
    """
    if coluna in df.columns:
        df[coluna] = pd.to_datetime(df[coluna], errors='coerce').dt.date
    return df


def generate_pinterest_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preenche as colunas 'Inicio_da_Campanha' e 'Fim_da_Campanha' convertendo
    as colunas 'start' e 'end' (datetime) para datas (YYYY-MM-DD) no Pinterest.
    """
    if 'start' in df.columns:
        df['Inicio_da_Campanha'] = df['start'].apply(transformar_para_date)
    else:
        df['Inicio_da_Campanha'] = ""

    if 'end' in df.columns:
        df['Fim_da_Campanha'] = df['end'].apply(transformar_para_date)
    else:
        df['Fim_da_Campanha'] = ""

    return df
