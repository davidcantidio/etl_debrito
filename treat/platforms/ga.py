#treat.platforms.ga
import pandas as pd

def transform_ga(df: pd.DataFrame, lookup=None) -> pd.DataFrame:
    """
    Ensures GA sheet has the necessary empty columns for downstream steps.
    """
    for col in ("ad_group_name", "ad_name"):
        if col not in df.columns:
            df[col] = ""
    return df
