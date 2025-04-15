import logging
import pandas as pd
from utils.google_sheets import carregar_aba_google_sheets, CREDS_PATH, SPREADSHEET_URL
from utils.datas import converter_data
from utils.normalize import converter_colunas_numericas

# ====================================================
#                   GÊNERO FUNCTIONS
# ====================================================

def merge_placement_and_gender_data(df_placement_pivot, df_gender_pivot):
    """
    Realiza o merge dos DataFrames pivotados dos dados de metaGeral (com Placement)
    e metaGenero (com Gender) utilizando as chaves 'Ad ID' e 'Date'.
    Apenas os registros com correspondência em ambos os conjuntos serão mantidos.
    """
    logging.debug("Starting merge_placement_and_gender_data()")
    merged_df = pd.merge(df_gender_pivot, df_placement_pivot, on=["Ad ID", "Date"], how="inner")
    if merged_df.empty:
        logging.warning("No matching records found during merge of placement and gender data.")
    logging.debug(f"Finished merge_placement_and_gender_data(), result shape: {merged_df.shape}")
    return merged_df


def load_and_prepare_meta_placement_data():
    logging.debug("Starting load_and_prepare_meta_placement_data()")
    sheet_name = "metaGeral"
    logging.debug(f"Loading metaGeral data from sheet: {sheet_name}")
    try:
        df = carregar_aba_google_sheets(CREDS_PATH, SPREADSHEET_URL, sheet_name)
        logging.debug(f"Loaded metaGeral data with shape: {df.shape}")
    except Exception as e:
        logging.error(f"Error loading metaGeral data: {e}")
        raise

    if 'Ad ID' not in df.columns:
        logging.debug("Column 'Ad ID' not found. Generating 'Ad ID' using 'Campaign name' and 'Ad name'.")
        if 'Campaign name' in df.columns and 'Ad name' in df.columns:
            df['Ad ID'] = df['Campaign name'].str.strip() + "_" + df['Ad name'].str.strip()
            logging.debug("Generated 'Ad ID' from 'Campaign name' and 'Ad name'.")
        else:
            df['Ad ID'] = df.index.astype(str)
            logging.debug("Generated 'Ad ID' from DataFrame index due to missing 'Campaign name' or 'Ad name'.")
    else:
        logging.debug("Column 'Ad ID' already exists.")

    if 'Placement' not in df.columns:
        logging.error("Missing required column 'Placement'.")
        raise KeyError("The dataset does not contain the 'Placement' column.")
    else:
        sample_placement = df['Placement'].dropna().unique()[:5]
        logging.debug(f"Sample unique placements: {sample_placement}")

    numeric_columns = ['Impressions', 'Link clicks', 'Cost', 'Video watches at 100%']
    logging.debug(f"Converting columns to numeric: {numeric_columns}")
    df = converter_colunas_numericas(df, numeric_columns)
    logging.debug("Converted numeric columns.")

    if 'Date' in df.columns:
        df = converter_data(df, 'Date')
        logging.debug("Converted 'Date' column to date format.")
    else:
        logging.warning("Column 'Date' not found in metaGeral.")

    logging.debug(f"Finished load_and_prepare_meta_placement_data(), final DataFrame shape: {df.shape}")
    return df


def load_and_prepare_meta_gender_data():
    logging.debug("Starting load_and_prepare_meta_gender_data()")
    sheet_name = "metaGenero"
    logging.debug(f"Loading metaGenero data from sheet: {sheet_name}")
    try:
        df = carregar_aba_google_sheets(CREDS_PATH, SPREADSHEET_URL, sheet_name)
        logging.debug(f"Loaded metaGenero data with shape: {df.shape}")
    except Exception as e:
        logging.error(f"Error loading metaGenero data: {e}")
        raise

    if 'Ad ID' not in df.columns:
        logging.debug("Column 'Ad ID' not found in metaGenero. Generating 'Ad ID' using 'Campaign name' and 'Ad name'.")
        if 'Campaign name' in df.columns and 'Ad name' in df.columns:
            df['Ad ID'] = df['Campaign name'].str.strip() + "_" + df['Ad name'].str.strip()
            logging.debug("Generated 'Ad ID' from 'Campaign name' and 'Ad name'.")
        else:
            df['Ad ID'] = df.index.astype(str)
            logging.debug("Generated 'Ad ID' from DataFrame index due to missing 'Campaign name' or 'Ad name'.")
    else:
        logging.debug("Column 'Ad ID' already exists in metaGenero.")

    if 'Gender' not in df.columns:
        logging.error("Missing required column 'Gender' in metaGenero.")
        raise KeyError("The dataset does not contain the 'Gender' column.")
    else:
        sample_gender = df['Gender'].dropna().unique()[:5]
        logging.debug(f"Sample unique genders: {sample_gender}")

    numeric_columns = ['Impressions', 'Link clicks', 'Cost', 'Video watches at 100%']
    logging.debug(f"Converting numeric columns in metaGenero: {numeric_columns}")
    df = converter_colunas_numericas(df, numeric_columns)
    logging.debug("Converted numeric columns for metaGenero.")

    if 'Date' in df.columns:
        df = converter_data(df, 'Date')
        logging.debug("Converted 'Date' column to date format in metaGenero.")
    else:
        logging.warning("Column 'Date' not found in metaGenero.")

    logging.debug(f"Finished load_and_prepare_meta_gender_data(), final DataFrame shape: {df.shape}")
    return df


def pivot_meta_placement_data(df_placement):
    logging.debug("Starting pivot_meta_placement_data()")
    numeric_columns = ["Impressions", "Link clicks", "Cost", "Video watches at 100%"]
    logging.debug(f"Using numeric columns for pivot: {numeric_columns}")
    pivot_df = pd.pivot_table(df_placement, index=["Ad ID", "Date"], columns="Placement",
                              values=numeric_columns, aggfunc="sum", fill_value=0)
    pivot_df = pivot_df.swaplevel(axis=1)
    pivot_df.sort_index(axis=1, level=0, inplace=True)
    pivot_df.columns = [f"{col[0]}_{col[1]}" for col in pivot_df.columns]
    logging.debug(f"Finished pivot_meta_placement_data(), result shape: {pivot_df.shape}")
    return pivot_df.reset_index()


def pivot_meta_gender_data(df_gender):
    """
    Processa os dados de metaGenero agrupando-os por 'Ad ID', 'Date' e 'Gender' e
    somando as métricas numéricas.
    """
    logging.debug("Starting pivot_meta_gender_data()")
    numeric_columns = ["Impressions", "Link clicks", "Cost", "Video watches at 100%"]
    group_columns = ["Ad ID", "Date", "Gender"]
    logging.debug(f"Grouping by {group_columns} and summing numeric columns: {numeric_columns}")
    df_grouped = df_gender.groupby(group_columns, as_index=False).agg({col: "sum" for col in numeric_columns})
    logging.debug(f"Finished pivot_meta_gender_data(), result shape: {df_grouped.shape}")
    return df_grouped


def distribute_gender_metrics(df_merged):
    """
    Distribui as métricas agregadas de gênero para cada plataforma (definida pelo pivot de Placement).
    
    Cria uma nova linha para cada (Ad ID, Date, Gender, _Plataforma) com métricas redistribuídas.
    """
    logging.debug("Starting distribute_gender_metrics()")
    metrics = ["Impressions", "Link clicks", "Cost", "Video watches at 100%"]
    placement_cols = [col for col in df_merged.columns if col.endswith("_Impressions")]
    placements = list({col.rsplit("_", 1)[0] for col in placement_cols})
    logging.debug(f"Identified platforms: {placements}")
    output_rows = []
    for idx, row in df_merged.iterrows():
        total_by_metric = {metric: sum(row.get(f"{pl}_{metric}", 0) for pl in placements) for metric in metrics}
        for platform in placements:
            distributed_metrics = {}
            for metric in metrics:
                col_name = f"{platform}_{metric}"
                total = total_by_metric[metric]
                distributed_metrics[metric] = round(row[metric] * (row.get(col_name, 0) / total)) if total > 0 else 0
            new_row = row[["Ad ID", "Date", "Gender"]].to_dict()
            new_row["_Plataforma"] = platform
            new_row.update(distributed_metrics)
            output_rows.append(new_row)
    output_df = pd.DataFrame(output_rows)
    logging.debug(f"Finished distribute_gender_metrics(), result shape: {output_df.shape}")
    return output_df


def preserve_placement_column(df):
    """
    Garante que o DataFrame possua a coluna 'Placement'. Caso contrário,
    se existir '_Plataforma', copia seus valores para 'Placement'.
    """
    df = df.copy()
    if "Placement" not in df.columns:
        if "_Plataforma" in df.columns:
            df["Placement"] = df["_Plataforma"]
            logging.debug("Column 'Placement' was missing; '_Plataforma' values copied into 'Placement'.")
        else:
            raise KeyError("Neither 'Placement' nor '_Plataforma' exists in the DataFrame.")
    else:
        logging.debug("Column 'Placement' is present; no need to preserve it.")
    return df


# ====================================================
#                   REGIÃO FUNCTIONS
# ====================================================

def merge_placement_and_region_data(df_placement_pivot, df_region_pivot):
    """
    Realiza o merge dos DataFrames pivotados dos dados de metaGeral (com Placement)
    e metaRegiao (com Province name) utilizando as chaves 'Ad ID' e 'Date'.
    """
    logging.debug("Starting merge_placement_and_region_data()")
    merged_df = pd.merge(df_region_pivot, df_placement_pivot, on=["Ad ID", "Date"], how="inner")
    if merged_df.empty:
        logging.warning("No matching records found during merge of placement and region data.")
    logging.debug(f"Finished merge_placement_and_region_data(), result shape: {merged_df.shape}")
    return merged_df


def load_and_prepare_meta_region_data():
    logging.debug("Starting load_and_prepare_meta_region_data()")
    sheet_name = "metaRegiao"
    logging.debug(f"Loading metaRegiao data from sheet: {sheet_name}")
    try:
        df = carregar_aba_google_sheets(CREDS_PATH, SPREADSHEET_URL, sheet_name)
        logging.debug(f"Loaded metaRegiao data with shape: {df.shape}")
    except Exception as e:
        logging.error(f"Error loading metaRegiao data: {e}")
        raise

    if 'Ad ID' not in df.columns:
        logging.debug("Column 'Ad ID' not found in metaRegiao. Generating 'Ad ID' using 'Campaign name' and 'Ad name'.")
        if 'Campaign name' in df.columns and 'Ad name' in df.columns:
            df['Ad ID'] = df['Campaign name'].str.strip() + "_" + df['Ad name'].str.strip()
            logging.debug("Generated 'Ad ID' from 'Campaign name' and 'Ad name'.")
        else:
            df['Ad ID'] = df.index.astype(str)
            logging.debug("Generated 'Ad ID' from DataFrame index due to missing 'Campaign name' or 'Ad name'.")
    else:
        logging.debug("Column 'Ad ID' already exists in metaRegiao.")

    if 'Province name' not in df.columns:
        logging.error("Missing required column 'Province name' in metaRegiao.")
        raise KeyError("The dataset does not contain the 'Province name' column.")
    else:
        sample_region = df['Province name'].dropna().unique()[:5]
        logging.debug(f"Sample unique provinces: {sample_region}")

    numeric_columns = ['Impressions', 'Link clicks', 'Cost', 'Video watches at 100%']
    logging.debug(f"Converting numeric columns in metaRegiao: {numeric_columns}")
    df = converter_colunas_numericas(df, numeric_columns)
    logging.debug("Converted numeric columns for metaRegiao.")

    if 'Date' in df.columns:
        df = converter_data(df, 'Date')
        logging.debug("Converted 'Date' column to date format in metaRegiao.")
    else:
        logging.warning("Column 'Date' not found in metaRegiao.")

    logging.debug(f"Finished load_and_prepare_meta_region_data(), final DataFrame shape: {df.shape}")
    return df


def pivot_meta_region_data(df_region):
    """
    Agrupa os dados de metaRegiao por 'Ad ID', 'Date' e 'Province name'
    somando as métricas numéricas.
    """
    logging.debug("Starting pivot_meta_region_data()")
    numeric_columns = ["Impressions", "Link clicks", "Cost", "Video watches at 100%"]
    group_columns = ["Ad ID", "Date", "Province name"]
    logging.debug(f"Grouping by {group_columns} and summing numeric columns: {numeric_columns}")
    df_grouped = df_region.groupby(group_columns, as_index=False).agg({col: "sum" for col in numeric_columns})
    logging.debug(f"Finished pivot_meta_region_data(), result shape: {df_grouped.shape}")
    return df_grouped


def distribute_region_metrics(df_merged):
    """
    Distribui as métricas agregadas de região para cada plataforma (definida pelo pivot de Placement)
    com base nas proporções dos valores obtidos por Placement.
    
    Cria uma nova linha para cada (Ad ID, Date, Province name, _Plataforma) com as métricas redistribuídas.
    """
    logging.debug("Starting distribute_region_metrics()")
    metrics = ["Impressions", "Link clicks", "Cost", "Video watches at 100%"]
    placement_cols = [col for col in df_merged.columns if col.endswith("_Impressions")]
    placements = list({col.rsplit("_", 1)[0] for col in placement_cols})
    logging.debug(f"Identified platforms: {placements}")
    output_rows = []
    for idx, row in df_merged.iterrows():
        total_by_metric = {metric: sum(row.get(f"{pl}_{metric}", 0) for pl in placements) for metric in metrics}
        for platform in placements:
            distributed_metrics = {}
            for metric in metrics:
                col_name = f"{platform}_{metric}"
                total = total_by_metric[metric]
                if total > 0:
                    distributed_value = round(row[metric] * (row.get(col_name, 0) / total))
                else:
                    distributed_value = 0
                distributed_metrics[metric] = distributed_value
            new_row = {
                "Ad ID": row.get("Ad ID"),
                "Date": row.get("Date"),
                "Province name": row.get("Province name"),
                "_Plataforma": platform,
                **distributed_metrics
            }
            output_rows.append(new_row)
    output_df = pd.DataFrame(output_rows)
    logging.debug(f"Finished distribute_region_metrics(), result shape: {output_df.shape}")
    return output_df

# ====================================================
# FIM DO ARQUIVO
# ====================================================

if __name__ == "__main__":
    # Exemplo de execução de teste para cada bloco (opcional)
    logging.basicConfig(level=logging.DEBUG)
    # As funções podem ser testadas individualmente aqui, se necessário.
    pass
