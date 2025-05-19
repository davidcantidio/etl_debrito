from extract.sheets_fetcher import SheetsFetcher
from utils.get_missing_records import get_missing_records
from utils.numeracao import gerar_numeracao
from treat.utils.organizar_dataframe import (
    remover_colunas_indesejadas,
    reordenar_colunas_para_modelo,
)
from treat.utils.fields_lists import GENERAL_MODEL_COLUMN_ORDER
from utils.append_records_to_sheet import append_records_to_sheet
import pandas as pd
import logging
from utils.campos_calculados import gerar_id

log = logging.getLogger(__name__)


def default_fetcher(spreadsheet_id: str, creds_path: str) -> SheetsFetcher:
    """
    Retorna uma instância padrão de SheetsFetcher.
    """
    return SheetsFetcher(spreadsheet_id=spreadsheet_id, creds_path=creds_path)


def default_appender(creds_path: str, spreadsheet_id: str):
    """
    Retorna uma função appender que insere registros na planilha.
    """
    def _append(sheet_name: str, df: pd.DataFrame):
        append_records_to_sheet(
            creds_path=creds_path,
            spreadsheet_id=spreadsheet_id,
            sheet_name=sheet_name,
            df=df
        )
    return _append


def fetch_data(
    fetcher: SheetsFetcher,
    origem_sheet: str,
    destino_sheet: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Extrai DataFrames de origem e destino usando o fetcher.
    """
    df_src = fetcher.get([origem_sheet], as_frame=True)[origem_sheet]
    df_dest = fetcher.get([destino_sheet], as_frame=True)[destino_sheet]
    return df_src, df_dest


def prepare_new_records(
    df_src: pd.DataFrame,
    df_dest: pd.DataFrame
) -> pd.DataFrame:
    """
    Identifica registros faltantes, gera numeração e organiza colunas.
    """
    df_new = get_missing_records(df_src, df_dest)
    if df_new.empty:
        log.info("Nenhum registro novo encontrado para inserir.")
        return df_new

    # Numeração sequencial incremental
    df_new = gerar_numeracao(df_new, df_destino=df_dest)
    # Limpeza de colunas e ordenação
    df_new = remover_colunas_indesejadas(df_new)
    df_new = reordenar_colunas_para_modelo(df_new, GENERAL_MODEL_COLUMN_ORDER)

    return df_new


def append_new_records(
    appender,
    destino_sheet: str,
    df_new: pd.DataFrame
) -> None:
    """
    Insere (append) novos registros na aba de destino.
    """
    if df_new.empty:
        log.info("DataFrame vazio — nada a inserir no destino.")
        return

    log.info("Inserindo %d novos registros em '%s'...", len(df_new), destino_sheet)
    appender(destino_sheet, df_new)
    log.info("Inserção finalizada.")


def load_missing_records(
    spreadsheet_id: str,
    creds_path: str,
    origem_sheet: str,
    destino_sheet: str,
    fetcher=None,
    appender=None
) -> None:
    """
    Pipeline de carga: extrai dados, prepara registros faltantes e faz append.

    Parâmetros:
        spreadsheet_id: ID da planilha Google.
        creds_path: caminho pro JSON de credenciais.
        origem_sheet: nome da aba de origem.
        destino_sheet: nome da aba de destino.
        fetcher: instancia de SheetsFetcher (opcional, para injeção).
        appender: função append (opcional, para injeção).
    """
    fetcher = fetcher or default_fetcher(spreadsheet_id, creds_path)
    appender = appender or default_appender(creds_path, spreadsheet_id)

    # 1. Extrair dados
    df_src, df_dest = fetch_data(fetcher, origem_sheet, destino_sheet)

    df_src = gerar_id(df_src)
    df_dest = gerar_id(df_dest)

    # 2. Preparar novos registros
    df_new = prepare_new_records(df_src, df_dest)

    # 3. Inserir no destino
    append_new_records(appender, destino_sheet, df_new)


if __name__ == "__main__":
    import os
    # Exemplo de uso com variáveis de ambiente
    load_missing_records(
        spreadsheet_id=os.getenv("SPREADSHEET_ID"),
        creds_path=os.getenv("GOOGLE_CREDS_PATH", "creds.json"),
        origem_sheet="Origem",
        destino_sheet="ModeloGeral"
    )