import pandas as pd

def read_sheet_as_dataframe(client, spreadsheet_id, sheet_name, offset_col=0):
    """
    Lê uma aba da planilha Google e retorna um DataFrame limpo.

    Parâmetros:
        client (gspread.Client): Cliente autenticado do Google Sheets.
        spreadsheet_id (str): ID da planilha.
        sheet_name (str): Nome da aba a ser lida.
        offset_col (int, opcional): Se maior que 0, indica que os dados começam a partir dessa coluna. Padrão: 0.

    Retorna:
        pandas.DataFrame: DataFrame contendo os dados da aba, com as linhas vazias removidas
                          e, se presente, filtrando registros com a coluna "ID" vazia.
    """
    sh = client.open_by_key(spreadsheet_id)
    worksheet = sh.worksheet(sheet_name)
    data = worksheet.get_all_values()
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data[1:], columns=data[0])
    if offset_col > 0 and df.columns[0].strip() == "":
        df = df.iloc[:, offset_col:]
    df = df.dropna(how="all")
    if "ID" in df.columns:
        df["ID"] = df["ID"].astype(str)
        df = df[df["ID"].str.strip() != ""]
    return df
