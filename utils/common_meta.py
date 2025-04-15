import logging
import pandas as pd
from utils.google_sheets import carregar_aba_google_sheets, CREDS_PATH, SPREADSHEET_URL
from utils.datas import converter_data
from utils.normalize import converter_colunas_numericas


def merge_placement_and_gender_data(df_placement_pivot, df_gender_pivot):
    """
    Realiza o merge dos DataFrames pivotados dos dados de metaGeral (com Placement) e metaGenero (com Gender)
    utilizando como chave 'Ad ID' e 'Date'. Apenas os registros com correspondência em ambos os conjuntos
    serão mantidos.
    
    Parâmetros:
        df_placement_pivot (DataFrame): DataFrame resultante de pivot_meta_placement_data(), contendo colunas como
                                        "Facebook_Impressions", "Instagram_Impressions", etc.
        df_gender_pivot (DataFrame): DataFrame resultante de pivot_meta_gender_data(), contendo "Ad ID", "Date", "Gender"
                                     e as métricas agregadas (Impressions, Link clicks, Cost, Video watches at 100%).
    
    Retorna:
        DataFrame: Resultado do merge, combinando as informações de gênero com as métricas por Placement.
    """

    logging.debug("Starting merge_placement_and_gender_data()")
    
    # Realiza merge utilizando inner join em "Ad ID" e "Date"
    merged_df = pd.merge(df_gender_pivot, df_placement_pivot, on=["Ad ID", "Date"], how="inner")
    
    if merged_df.empty:
        logging.warning("No matching records found during merge of placement and gender data.")
    
    logging.debug(f"Finished merge_placement_and_gender_data(), result shape: {merged_df.shape}")
    return merged_df


def load_and_prepare_meta_placement_data():
    logging.debug("Starting load_and_prepare_meta_placement_data()")
    
    # Define o nome da aba que contém o dataset metaGeral
    sheet_name = "metaGeral"
    logging.debug(f"Loading metaGeral data from sheet: {sheet_name}")
    
    try:
        df = carregar_aba_google_sheets(CREDS_PATH, SPREADSHEET_URL, sheet_name)
        logging.debug(f"Loaded metaGeral data with shape: {df.shape}")
    except Exception as e:
        logging.error(f"Error loading metaGeral data: {e}")
        raise

    # Verifica se existe a coluna 'Ad ID'. Caso não exista, gera-a a partir de 'Campaign name' e 'Ad name'
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

    # Verifica a presença da coluna 'Placement' (essencial para o pivot)
    if 'Placement' not in df.columns:
        logging.error("Missing required column 'Placement'.")
        raise KeyError("The dataset does not contain the 'Placement' column.")
    else:
        sample_placement = df['Placement'].dropna().unique()[:5]
        logging.debug(f"Sample unique placements: {sample_placement}")

    # Converter colunas numéricas para garantir consistência
    # Atualizado para os nomes de coluna reais:
    numeric_columns = ['Impressions', 'Link clicks', 'Cost', 'Video watches at 100%']
    logging.debug(f"Converting columns to numeric: {numeric_columns}")
    df = converter_colunas_numericas(df, numeric_columns)
    logging.debug("Converted numeric columns.")

    # Converter a coluna de data para o formato date
    if 'Date' in df.columns:
        df = converter_data(df, 'Date')
        logging.debug("Converted 'Date' column to date format.")
    else:
        logging.warning("Column 'Date' not found in metaGeral.")

    logging.debug(f"Finished load_and_prepare_meta_placement_data(), final DataFrame shape: {df.shape}")
    return df


def load_and_prepare_meta_gender_data():
    logging.debug("Starting load_and_prepare_meta_gender_data()")
    
    # Define o nome da aba que contém o dataset metaGenero
    sheet_name = "metaGenero"
    logging.debug(f"Loading metaGenero data from sheet: {sheet_name}")
    
    try:
        df = carregar_aba_google_sheets(CREDS_PATH, SPREADSHEET_URL, sheet_name)
        logging.debug(f"Loaded metaGenero data with shape: {df.shape}")
    except Exception as e:
        logging.error(f"Error loading metaGenero data: {e}")
        raise

    # Verifica se existe a coluna 'Ad ID'. Caso não exista, gera-a a partir de 'Campaign name' e 'Ad name'
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

    # Verifica a presença da coluna 'Gender'
    if 'Gender' not in df.columns:
        logging.error("Missing required column 'Gender' in metaGenero.")
        raise KeyError("The dataset does not contain the 'Gender' column.")
    else:
        sample_gender = df['Gender'].dropna().unique()[:5]
        logging.debug(f"Sample unique genders: {sample_gender}")

    # Converter colunas numéricas para garantir consistência
    numeric_columns = ['Impressions', 'Link clicks', 'Cost', 'Video watches at 100%']
    logging.debug(f"Converting numeric columns in metaGenero: {numeric_columns}")
    df = converter_colunas_numericas(df, numeric_columns)
    logging.debug("Converted numeric columns for metaGenero.")

    # Converter a coluna de data para o formato date
    if 'Date' in df.columns:
        df = converter_data(df, 'Date')
        logging.debug("Converted 'Date' column to date format in metaGenero.")
    else:
        logging.warning("Column 'Date' not found in metaGenero.")

    logging.debug(f"Finished load_and_prepare_meta_gender_data(), final DataFrame shape: {df.shape}")
    return df

def pivot_meta_placement_data(df_placement):
    import logging
    import pandas as pd

    logging.debug("Starting pivot_meta_placement_data()")

    # Definir apenas as colunas numéricas que serão pivotadas.
    numeric_columns = ["Impressions", "Link clicks", "Cost", "Video watches at 100%"]
    logging.debug(f"Using numeric columns for pivot: {numeric_columns}")

    # Cria a tabela pivô: agrupa estritamente por 'Ad ID' e 'Date'
    # Assim, não utiliza ou modifica colunas de dimensão como 'Account name', 'Ad name', etc.
    pivot_df = pd.pivot_table(
        df_placement,
        index=["Ad ID", "Date"],
        columns="Placement",
        values=numeric_columns,
        aggfunc="sum",
        fill_value=0
    )
    logging.debug("Pivot table created with MultiIndex columns.")

    # Manipula os níveis para que as colunas resultantes fiquem no formato "<Placement>_<Metric>".
    pivot_df = pivot_df.swaplevel(axis=1)
    pivot_df.sort_index(axis=1, level=0, inplace=True)
    logging.debug("Swapped MultiIndex levels on columns.")

    # Achata (flatten) o MultiIndex concatenando os níveis com um underscore.
    pivot_df.columns = [f"{col[0]}_{col[1]}" for col in pivot_df.columns]
    logging.debug("Flattened pivot table columns.")
    
    logging.debug(f"Finished pivot_meta_placement_data(), result shape: {pivot_df.shape}")
    
    # Retorna somente as chaves 'Ad ID' e 'Date' juntamente com as métricas pivotadas;
    # as colunas de dimensão originais serão adicionadas posteriormente, se necessário.
    return pivot_df.reset_index()


def pivot_meta_gender_data(df_gender):
    """
    Processa os dados de metaGenero agrupando-os por 'Ad ID', 'Date' e 'Gender'.
    As métricas numéricas (Impressions, Link clicks, Cost e Video watches at 100%) são somadas
    para cada grupo. Essa função garante que não haja linhas duplicadas para as mesmas chaves.
    
    Parâmetros:
        df_gender (DataFrame): DataFrame preparado de metaGenero.
        
    Retorna:
        DataFrame agrupado com as colunas 'Ad ID', 'Date', 'Gender' e as métricas agregadas.
    """
    
    logging.debug("Starting pivot_meta_gender_data()")
    
    # Definir as colunas numéricas a serem agregadas
    numeric_columns = ["Impressions", "Link clicks", "Cost", "Video watches at 100%"]
    # Definir as chaves de agrupamento
    group_columns = ["Ad ID", "Date", "Gender"]
    logging.debug(f"Grouping by {group_columns} and summing numeric columns: {numeric_columns}")
    
    # Agrega as métricas numéricas. Se houver duplicatas para as chaves, elas serão somadas.
    df_grouped = df_gender.groupby(group_columns, as_index=False).agg({col: "sum" for col in numeric_columns})
    
    logging.debug(f"Finished pivot_meta_gender_data(), result shape: {df_grouped.shape}")
    return df_grouped


def distribute_gender_metrics(df_merged):
    """
    Recebe o DataFrame resultante do merge entre os dados de gênero (pivoted) e os dados de placement (pivoted),
    e distribui as métricas agregadas de gênero para cada plataforma (determinado pelo pivot placement),
    criando uma nova linha para cada plataforma com os valores distribuídos proporcionalmente.

    Para cada linha de df_merged, para cada métrica numérica (Impressions, Link clicks, Cost, Video watches at 100%):
      - Calcula o total das métricas de placement somando, por exemplo, Facebook_Impressions e Instagram_Impressions.
      - Determina a proporção de cada plataforma (p.ex.: prop_fb = Facebook_Impressions / total).
      - Multiplica a métrica agregada de gênero (coluna "Impressions" do df_merged) por essa proporção e aplica round().

    O resultado é um DataFrame final com as colunas:
      "Ad ID", "Date", "Gender", "_Plataforma", "Impressions", "Link clicks", "Cost", "Video watches at 100%".

    Parâmetros:
        df_merged (DataFrame): resultado do merge, contendo colunas chave ("Ad ID", "Date", "Gender"), 
            as métricas de gênero agregadas (Impressions, Link clicks, Cost, Video watches at 100%)
            e também as colunas pivotadas do placement, formatadas como "<Platform>_<Metric>".
    
    Retorna:
        DataFrame: DataFrame com uma linha para cada (Ad ID, Date, Gender, Plataforma) contendo os valores distribuídos.
    """
    import logging
    import pandas as pd

    logging.debug("Starting distribute_gender_metrics()")
    
    # Lista de métricas que serão distribuídas
    metrics = ["Impressions", "Link clicks", "Cost", "Video watches at 100%"]

    # Identifica as plataformas presentes a partir das colunas do df_merged que terminam com, por exemplo, "_Impressions"
    placement_cols = [col for col in df_merged.columns if col.endswith("_Impressions")]
    # Extraímos o nome da plataforma (antes do underscore)
    placements = list({col.rsplit("_", 1)[0] for col in placement_cols})
    logging.debug(f"Identified platforms: {placements}")

    output_rows = []

    # Itera sobre cada linha do df_merged para calcular a distribuição
    for idx, row in df_merged.iterrows():
        # Para cada métrica, calcular o total dos valores pivotados para essa métrica
        total_by_metric = {}
        for metric in metrics:
            total = 0
            for platform in placements:
                col_name = f"{platform}_{metric}"
                if col_name in df_merged.columns:
                    total += row[col_name]
            total_by_metric[metric] = total

        # Para cada plataforma, calcula os valores distribuídos se houver total > 0; caso contrário, assume 0.
        for platform in placements:
            distributed_metrics = {}
            for metric in metrics:
                col_name = f"{platform}_{metric}"
                total = total_by_metric[metric]
                if total > 0:
                    ratio = row[col_name] / total
                    distributed_metrics[metric] = round(row[metric] * ratio)
                else:
                    distributed_metrics[metric] = 0

            # Cria um novo registro com as informações chave e a plataforma
            new_row = row[["Ad ID", "Date", "Gender"]].to_dict()
            new_row["_Plataforma"] = platform
            new_row.update(distributed_metrics)
            output_rows.append(new_row)

    output_df = pd.DataFrame(output_rows)
    logging.debug(f"Finished distribute_gender_metrics(), result shape: {output_df.shape}")
    return output_df

import logging
import pandas as pd

def preserve_placement_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Verifica se o DataFrame contém a coluna 'Placement'. Se não houver,
    e se a coluna '_Plataforma' existir, copia os valores da '_Plataforma'
    para uma nova coluna 'Placement'. Caso nenhum dos dois exista, lança um erro.
    
    Parâmetros:
        df (DataFrame): DataFrame a ser verificado.
        
    Retorna:
        DataFrame: O mesmo DataFrame, com a coluna 'Placement' garantida.
    
    Exceções:
        KeyError: se nem 'Placement' nem '_Plataforma' estiverem presentes.
    """
    df = df.copy()  # Trabalha com uma cópia para não alterar o original
    if "Placement" not in df.columns:
        if "_Plataforma" in df.columns:
            df["Placement"] = df["_Plataforma"]
            logging.debug("Column 'Placement' was missing; '_Plataforma' values copied into 'Placement'.")
        else:
            raise KeyError("Neither 'Placement' nor '_Plataforma' exists in the DataFrame.")
    else:
        logging.debug("Column 'Placement' is present; no need to preserve it.")
    return df
