import pandas as pd

def get_missing_records(df_origin, df_target):
    """
    Compara os DataFrames de origem e destino utilizando a coluna 'ID' com contagem cumulativa
    para tratar duplicatas, e retorna os registros da origem que não estão presentes no destino.
    
    Parâmetros:
        df_origin (pandas.DataFrame): DataFrame de origem.
        df_target (pandas.DataFrame): DataFrame de destino.
    
    Retorna:
        pandas.DataFrame: Registros faltantes encontrados em df_origin.
    """
    origin = df_origin.copy()
    target = df_target.copy()
    
    # Se não houver a coluna "ID" no destino ou o DataFrame estiver vazio, retorna toda a origem
    if "ID" not in target.columns or target.empty:
        return origin.copy()
    
    # Cria uma coluna de índice para tratar duplicatas
    origin["ID_index"] = origin.groupby("ID").cumcount() + 1
    target["ID_index"] = target.groupby("ID").cumcount() + 1
    
    merged = pd.merge(origin, target[["ID", "ID_index"]],
                      on=["ID", "ID_index"], how="left", indicator=True)
    
    missing = merged[merged["_merge"] == "left_only"].drop(columns=["_merge", "ID_index"])
    return missing
