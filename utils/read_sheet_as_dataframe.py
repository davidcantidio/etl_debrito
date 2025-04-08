def read_sheet_as_dataframe_range(
    client,
    spreadsheet_id: str,
    sheet_name: str,
    range_str: str = "A1:ZZ",
    header_row_index: int = 0
):
    """
    Lê um intervalo específico (ex.: 'A1:AM') e converte em DataFrame,
    garantindo que todas as linhas tenham o mesmo tamanho, preenchendo com ''
    ou cortando ao máximo.
    """
    from googleapiclient.errors import HttpError
    import pandas as pd

    try:
        sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
        data = sheet.get(range_str)  # matriz de strings
    except HttpError as e:
        print(f"Erro ao ler '{sheet_name}': {e}")
        return pd.DataFrame()

    if not data or len(data) <= header_row_index:
        return pd.DataFrame()

    # 1) Determina o número máximo de colunas
    max_cols = max(len(row) for row in data)

    # 2) Ajusta cada linha para ter exat. 'max_cols'
    for i, row in enumerate(data):
        if len(row) < max_cols:
            # preenche com strings vazias
            data[i] = row + [""] * (max_cols - len(row))
        elif len(row) > max_cols:
            # corta o excedente
            data[i] = row[:max_cols]

    # 3) Extrai a linha de cabeçalho
    headers = data[header_row_index]
    body = data[header_row_index + 1 :]

    # 4) Constrói o DataFrame
    df = pd.DataFrame(body, columns=headers)

    return df
