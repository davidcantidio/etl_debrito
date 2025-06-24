import os
import json
import logging
from typing import Optional

import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials

log = logging.getLogger(__name__)


def write_dataframe_to_sheet(
    spreadsheet_id: str,
    sheet_name: str,
    df: pd.DataFrame,
    *,
    start_row: int = 1,
    google_credentials_json: Optional[str] = None,
    include_header: bool = True,
) -> None:
    """Escreve ``df`` na aba ``sheet_name`` iniciando em ``start_row``.

    Mantém tipos do DataFrame e redimensiona a aba conforme o número de linhas
    e colunas resultantes.
    """
    if df.empty:
        log.info("DataFrame vazio: nada a gravar em '%s'.", sheet_name)
        return

    google_credentials_json = google_credentials_json or os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not google_credentials_json:
        raise ValueError("Missing Google credentials JSON")

    scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_info = json.loads(google_credentials_json)
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    client = gspread.authorize(creds)

    ws = client.open_by_key(spreadsheet_id).worksheet(sheet_name)

    set_with_dataframe(
        ws,
        df,
        row=start_row,
        col=1,
        include_column_header=include_header,
        resize=False,
    )

    total_rows = start_row + df.shape[0]
    if include_header:
        total_rows += 1
    total_cols = df.shape[1]
    ws.resize(rows=total_rows, cols=total_cols)
    log.info("Gravados %d registros em '%s'", df.shape[0], sheet_name)

