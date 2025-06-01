# File: treat/utils/write_back.py
import pandas as pd
from treat.utils.sheets_cache import get_worksheet
import re
import datetime

def _col_idx_to_letter(idx: int) -> str:
    """Converte índice 1-based em letra A1 (1→A, 27→AA)."""
    letters = ""
    while idx > 0:
        idx, rem = divmod(idx-1, 26)
        letters = chr(65 + rem) + letters
    return letters


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

    # cabeçalho + linhas em batches
    cols = df.columns.tolist()
    rows = df.values.tolist()

    # envia só o cabeçalho
    ws.batch_update(
        [{"range": a1_range, "values": [cols]}],
        value_input_option=value_input_option,
    )

    # determina colunas A1
    n_cols = len(cols)
    # extrai letras iniciais de a1_range (ex.: "A" de "A1"); default "A"
    m = re.match(r"([A-Z]+)", a1_range.upper())
    first_col = m.group(1) if m else "A"
    # encontra índice numérico da primeira coluna (A→1, B→2, …)
    first_idx = sum((ord(c)-64)*(26**i) for i,c in enumerate(reversed(first_col)))
    last_idx = first_idx + n_cols - 1
    last_col = _col_idx_to_letter(last_idx)

    # calcula chunk_size dinâmico (até 95% de 10M células)
    MAX_CELLS = 10_000_000
    safe = int(MAX_CELLS * 0.95)
    chunk_size = max(1, safe // n_cols)

    total = len(rows)
    for start in range(0, total, chunk_size):
        chunk = rows[start : start + chunk_size]

        # 🔧 NOVO: sanitiza valores p/ JSON (datas → ISO, NaN → "")
        clean_chunk = []
        for r in chunk:
            clean_row = []
            for v in r:
                if pd.isna(v):
                    clean_row.append("")
                elif isinstance(v, (pd.Timestamp, datetime.datetime, datetime.date)):
                    clean_row.append(v.isoformat())
                else:
                    clean_row.append(v)
            clean_chunk.append(clean_row)

        row_start = 2 + start
        row_end   = row_start + len(clean_chunk) - 1
        range_str = f"{first_col}{row_start}:{last_col}{row_end}"

        ws.batch_update(
            [{"range": range_str, "values": clean_chunk}],
            value_input_option=value_input_option,
        )