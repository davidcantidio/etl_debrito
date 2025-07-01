import logging
from utils.fields_lists import GENERAL_MODEL_COLUMN_ORDER


def remover_colunas_indesejadas(df):
    """
    Remove colunas que não fazem parte do modelo final.
    """
    colunas_para_remover = [
        "Placement",
        "Campaign_ID",
        "Campaign_name",
    ]
    for col in colunas_para_remover:
        if col in df.columns:
            df.drop(columns=col, inplace=True)
            logging.debug(f"Coluna removida: {col}")
    return df


def reordenar_colunas_para_modelo(df, column_order_list):
    for col in column_order_list:
        if col not in df.columns:
            df[col] = ""
            logging.debug(f"Coluna adicionada (vazia): {col}")
    df = df[column_order_list]
    return df
