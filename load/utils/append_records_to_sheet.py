# utils/append_records_to_sheet.py

import logging
from typing import Optional

import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from tenacity import retry, wait_exponential, stop_after_attempt

log = logging.getLogger(__name__)


def _build_service(
    creds_path: str,
    scopes: Optional[list[str]] = None,
):
    """
    Cria a instância da Google Sheets API v4.
    """
    scopes = scopes or ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


@retry(
    wait=wait_exponential(multiplier=1, max=32),
    stop=stop_after_attempt(5),
    reraise=True,
)
def append_records_to_sheet(
    creds_path: str,
    spreadsheet_id: str,
    sheet_name: str,
    df: pd.DataFrame,
    *,
    start_row: int = 2,
    start_column: str = "A",
    value_input_option: str = "RAW",
    service: Optional = None,
) -> int:
    """
    Faz APPEND de `df` em `sheet_name`, começando em start_column+start_row,
    num único batch, com insertDataOption='INSERT_ROWS'.

    Retorna o número de linhas efetivamente inseridas.
    """
    if df.empty:
        log.info("DataFrame vazio: nada a gravar em '%s'.", sheet_name)
        return 0

    service = service or _build_service(creds_path)

    # Prepara valores (sem cabeçalho)
    values = df.fillna("").astype(str).values.tolist()

    # Range de início ex.: "ModeloGeral!A2"
    range_start = f"{sheet_name}!{start_column}{start_row}"
    body = {"values": values}

    try:
        result = (
            service.spreadsheets()
            .values()
            .append(
                spreadsheetId=spreadsheet_id,
                range=range_start,
                valueInputOption=value_input_option,
                insertDataOption="INSERT_ROWS",
                body=body,
            )
            .execute()
        )
    except HttpError as err:
        log.error("Erro ao inserir em '%s': %s", sheet_name, err)
        raise

    updated = result.get("updates", {}).get("updatedRows", 0)
    log.info("✅ %d linhas adicionadas em '%s' a partir de %s%d",
             updated, sheet_name, start_column, start_row)
    return updated
