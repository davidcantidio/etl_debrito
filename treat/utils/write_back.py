# File: treat/utils/write_back.py
import pandas as pd
from treat.utils.sheets_cache import get_worksheet


def write_back_df(
    df: pd.DataFrame,
    creds_path: str,
    spreadsheet_id: str,
    sheet_name: str,
    a1_range: str = "A1",
    value_input_option: str = "USER_ENTERED",
) -> None:
    """
    Grava `df` em lote no Google Sheets usando a worksheet cacheada.
    """
    ws = get_worksheet(creds_path, spreadsheet_id, sheet_name)

    # cabeçalho + linhas
    values = [df.columns.tolist()] + df.values.tolist()

    ws.batch_update(
        [{"range": a1_range, "values": values}],
        value_input_option=value_input_option,
    )
