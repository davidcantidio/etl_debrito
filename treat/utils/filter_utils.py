import logging

import pandas as pd
from utils.filter_lists import CAMPAIGN_NAME_FILTER_LIST


def remove_zero_impressoes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove todas as linhas cujo valor na coluna 'Impressoes' seja zero ou nulo.

    Parâmetros:
        df (pd.DataFrame): DataFrame de entrada com a coluna 'Impressoes'.

    Retorna:
        pd.DataFrame: DataFrame filtrado, sem linhas onde 'Impressoes' == 0.
    """
    if "Impressoes" not in df.columns:
        # Se não existir, nada a fazer
        return df

    # Garante que 'Impressoes' é numérico
    df = df.copy()
    df["Impressoes"] = pd.to_numeric(df["Impressoes"], errors="coerce").fillna(0)

    # Filtra somente valores > 0
    return df[df["Impressoes"] > 0]


def filter_campaign_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove rows where the 'Campanha' column matches any name in CAMPAIGN_NAME_FILTER_LIST.

    Logs:
      • INFO: total number of rows removed
      • INFO: the unique list of campaign names filtered
      • If no filtering occurred, logs that as well.
    """
    logging.debug("Applying campaign name filter: %s", CAMPAIGN_NAME_FILTER_LIST)
    if "Campanha" not in df.columns:
        logging.warning("'Campanha' column not found; skipping campaign name filter.")
        return df

    mask = df["Campanha"].isin(CAMPAIGN_NAME_FILTER_LIST)
    removed_count = int(mask.sum())
    filtered_campaigns = df.loc[mask, "Campanha"].drop_duplicates().tolist()

    if removed_count > 0:
        logging.info(
            "🚫 Filtered out %d row(s) matching blocked campaign names:", removed_count
        )
        for name in filtered_campaigns:
            logging.info("   • %s", name)
    else:
        logging.info("✅ No campaign names matched the filter list; nothing removed.")

    # Return only the rows we keep
    return df.loc[~mask].copy()
