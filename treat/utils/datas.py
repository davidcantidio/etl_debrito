# treat/utils/datas.py

import pandas as pd
from datetime import date, datetime
from typing import Optional, Any


try:
    from .datas_utils import transformar_para_date
except ImportError:  # pragma: no cover - optional dependency

    def transformar_para_date(val):
        """Converte string/data para date, ou retorna None."""
        return pd.to_datetime(val, errors="coerce").date()


from treat.bi_param_utils import BIParamLookup


def fill_missing_start_end_from_params(
    df: pd.DataFrame,
    lookup: BIParamLookup,
    *,
    coluna_utm: str = "ad_name",
    coluna_start: str = "start",
    coluna_end: str = "end",
    inplace: bool = True,
) -> pd.DataFrame:
    """
    Preenche as colunas 'start' e 'end' em memória usando BIParamLookup.
    Não faz nenhuma chamada ao Google Sheets nem write-back aqui: apenas
    delega o preenchimento ao lookup e devolve o DataFrame resultante.

    Parâmetros:
    - df: DataFrame de origem (preenchido ou não).
    - lookup: instância de BIParamLookup (já inicializada no pipeline).
    - coluna_utm: nome da coluna usada como chave para lookup (ex: "ad_name").
    - coluna_start: nome da coluna 'start' a ser preenchida.
    - coluna_end: nome da coluna 'end' a ser preenchida.
    - inplace: se False, trabalha em uma cópia de df; caso contrário, altera df diretamente.
    """
    if not inplace:
        df = df.copy()

    # Delega o preenchimento em memória a BIParamLookup
    # write_back=False garante que não ocorra batch_update aqui
    df_result = lookup.fill_missing_start_end_from_utm(
        df,
        coluna_utm=coluna_utm,
        coluna_start=coluna_start,
        coluna_end=coluna_end,
        sheet_name=None,
        write_back=False,
    )

    return df_result


def transformar_para_date(valor):
    """
    Transforma um valor de data no formato 'YYYY-MM-DD HH:MM:SS' ou
    um objeto datetime em um objeto date (YYYY-MM-DD).
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


def converter_data(df: pd.DataFrame, coluna: str) -> pd.DataFrame:
    if coluna in df.columns:
        parsed = pd.to_datetime(df[coluna], errors="coerce")
        df[coluna] = parsed.dt.date.where(~parsed.isna(), "")
    return df


def generate_pinterest_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preenche as colunas 'Inicio_da_Campanha' e 'Fim_da_Campanha' convertendo
    as colunas 'start' e 'end' (datetime) para datas (YYYY-MM-DD) no Pinterest.
    """
    if "start" in df.columns:
        df["Inicio_da_Campanha"] = df["start"].apply(transformar_para_date)
    else:
        df["Inicio_da_Campanha"] = ""

    if "end" in df.columns:
        df["Fim_da_Campanha"] = df["end"].apply(transformar_para_date)
    else:
        df["Fim_da_Campanha"] = ""

    return df


def unify_campaign_dates(
    df: pd.DataFrame,
    camp_col: str = "campaign_name",
    start_col: str = "start",
    end_col: str = "end",
    *,
    inplace: bool = True,
) -> pd.DataFrame:
    """Padroniza start/end por ``campaign_name`` usando menor/maior datas.

    Para cada ``campaign_name`` presente em ``camp_col`` calcula a data mais
    antiga encontrada em ``start_col`` e a mais recente em ``end_col``. Todas as
    linhas daquela campanha recebem esses valores, mantendo vazios quando não
    houver datas válidas.
    """

    if not inplace:
        df = df.copy()

    if camp_col not in df.columns:
        return df

    key = df[camp_col].astype(str).str.strip().str.lower()

    if start_col in df.columns:
        start_dates = pd.to_datetime(df[start_col], errors="coerce")
        earliest = start_dates.groupby(key).transform("min")
        earliest_dates = earliest.dt.date
        df[start_col] = df[start_col].where(earliest.isna(), earliest_dates)

    if end_col in df.columns:
        end_dates = pd.to_datetime(df[end_col], errors="coerce")
        latest = end_dates.groupby(key).transform("max")
        latest_dates = latest.dt.date
        df[end_col] = df[end_col].where(latest.isna(), latest_dates)

    return df


def normalize_date_to_str_DD_M_YYYY(value) -> str:
    """Normaliza ``value`` para a string ``DD/M/YYYY``.

    Meses são exibidos sem zero à esquerda. Valores vazios retornam ``""``.
    """
    if value is None or value == "" or (isinstance(value, float) and pd.isna(value)):
        return ""

    if isinstance(value, pd.Timestamp):
        value = value.date()

    if isinstance(value, str):
        try:
            value = transformar_para_date(value)
        except Exception:
            return ""

    if isinstance(value, datetime):
        value = value.date()

    if isinstance(value, date):
        return f"{value.day:02d}/{value.month}/{value.year}"

    return ""


def concat_period(start: Any, end: Any) -> str:
    """
    Gera um período formatado "DD/M/YYYY a DD/M/YYYY" a partir de duas datas,
    ou string vazia se faltar um dos valores.
    """
    if not start or not end:
        return ""
    s = normalize_date_to_str_DD_M_YYYY(start)
    e = normalize_date_to_str_DD_M_YYYY(end)
    return f"{s} a {e}"
