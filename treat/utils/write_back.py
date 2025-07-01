import datetime
import re

import pandas as pd

from treat.utils.sheets_cache import get_worksheet


def _col_idx_to_letter(idx: int) -> str:
    """Converte índice 1-based em letra A1 (1→A, 27→AA)."""
    letters = ""
    while idx > 0:
        idx, rem = divmod(idx - 1, 26)
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
    Grava `df` em lote no Google Sheets usando a worksheet cacheada, unindo
    cabeçalho e linhas em um único batch_update por aba (ou o mínimo de chunks).

    Parâmetros:
    - df: DataFrame a ser gravado.
    - creds_path: caminho/variável de credenciais.
    - spreadsheet_id: ID da planilha no Google Sheets.
    - sheet_name: nome da aba de destino.
    - a1_range: célula inicial para o cabeçalho (ex.: "A1").
    - value_input_option: modo de inserção de valores ("USER_ENTERED", "RAW" etc.).
    """
    # Obtém a worksheet via cache
    ws = get_worksheet(creds_path, spreadsheet_id, sheet_name)

    # Prepara cabeçalho + dados
    cols = df.columns.tolist()
    rows = df.values.tolist()

    # Junta cabeçalho + linhas numa única lista de listas
    data_rows = [cols] + rows  # cabeçalho na primeira linha

    # Determina colunas A1
    n_cols = len(cols)
    # Extrai letras iniciais de a1_range (ex.: "A" de "A1"); default "A"
    m = re.match(r"([A-Z]+)", a1_range.upper())
    first_col = m.group(1) if m else "A"
    # Encontra índice numérico da primeira coluna (A→1, B→2, …)
    first_idx = sum((ord(c) - 64) * (26**i) for i, c in enumerate(reversed(first_col)))
    last_idx = first_idx + n_cols - 1
    last_col = _col_idx_to_letter(last_idx)

    # Calcula chunk_size dinâmico (até 95% de 10M células)
    MAX_CELLS = 10_000_000
    safe_cells = int(MAX_CELLS * 0.95)
    # Número de linhas totais (incluindo cabeçalho)
    total_rows = len(data_rows)
    # Define o tamanho de cada chunk em número de linhas
    chunk_size = max(1, safe_cells // n_cols)

    # Loop para enviar em poucos batch_updates
    for start in range(0, total_rows, chunk_size):
        chunk = data_rows[start : start + chunk_size]

        # Sanitiza valores para JSON (datas → ISO, NaN → "")
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

        # Calcula o intervalo de escrita: começa em a1_range (linha 1) + offset
        row_start = int(re.sub(r"\D+", "", a1_range)) + start
        row_end = row_start + len(clean_chunk) - 1
        range_str = f"{first_col}{row_start}:{last_col}{row_end}"

        # Envia o chunk (cabeçalho incluído no primeiro chunk)
        ws.batch_update(
            [{"range": range_str, "values": clean_chunk}],
            value_input_option=value_input_option,
        )
