# treat/utils/write_back.py
import pandas as pd
from treat.utils.get_google_client import get_google_client

def write_back_df(
    df: pd.DataFrame,
    creds_path: str,
    spreadsheet_id: str,
    sheet_name: str,
    a1_range: str = "A1",
    value_input_option: str = "USER_ENTERED"
) -> None:
    client = get_google_client(creds_path)
    ws = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
    values = [df.columns.tolist()] + df.values.tolist()
    ws.batch_update(
        [{"range": a1_range, "values": values}],
        value_input_option=value_input_option
    )
