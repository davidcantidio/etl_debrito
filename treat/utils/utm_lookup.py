import pandas as pd
import logging


def load_utm_mapping() -> dict[str, dict[str, str]]:
    """
    Loads the BI_PARAMETRIZAÇÃO sheet and returns a mapping:
    { utm_content_lower -> {"START": start_date_str, "END": end_date_str} }.
    """
    from utils.get_google_client import get_google_client
    from utils.read_sheet_as_dataframe import read_sheet_as_dataframe_range
    from utils.google_sheets import CREDS_PATH, SPREADSHEET_ID

    logger = logging.getLogger("load_utm_mapping")
    logger.debug("Loading UTM mapping from BI_PARAMETRIZAÇÃO")

    client = get_google_client(CREDS_PATH)
    df = read_sheet_as_dataframe_range(
        client,
        SPREADSHEET_ID,
        sheet_name="BI_PARAMETRIZAÇÃO",
        range_str="A2:ZZ",
        header_row_index=0,
    )

    # Safely get the utm_content column or create an empty one
    if "utm_content" in df.columns:
        utm_series = df["utm_content"].astype(str).str.strip().str.lower()
    else:
        logger.warning(
            "Column 'utm_content' not found in BI_PARAMETRIZAÇÃO; using empty keys"
        )
        utm_series = pd.Series([""] * len(df), index=df.index)

    df = df.copy()
    df["utm_content"] = utm_series

    mapping: dict[str, dict[str, str]] = {}
    for _, row in df.iterrows():
        key = row["utm_content"]
        if key:
            mapping[key] = {"start": row.get("start", ""), "end": row.get("end", "")}

    logger.info(f"Loaded {len(mapping)} UTM entries for start/end lookup")
    return mapping


log = logging.getLogger(__name__)


def fill_missing_start_end_from_utm(
    df: pd.DataFrame, utm_mapping: dict
) -> pd.DataFrame:
    """
    Fill missing 'start' / 'end' by looking-up the
    utm_content in *utm_mapping* (dict: utm -> {"START":…, "END":…}).

    • Ignora linhas cujo “Content (utm)” esteja vazio.
    • Mantém valores originais quando já preenchidos.
    • Faz log INFO com contagem de linhas realmente preenchidas.
    """
    if not utm_mapping:
        log.info("[utm_lookup] Empty mapping – nothing to fill.")
        return df

    df = df.copy()

    # 1) Normalised key
    df["_utm_key"] = df.get("Content (utm)", "").astype(str).str.strip().str.lower()

    # 2) Build lookup Series once
    start_lkp = pd.Series(
        {k: v["start"] for k, v in utm_mapping.items()},
        name="start_lookup",
    )
    end_lkp = pd.Series(
        {k: v["end"] for k, v in utm_mapping.items()},
        name="end_lookup",
    )

    # 3) Existing cols (may not exist yet)
    if "start" not in df.columns:
        df["start"] = ""
    if "end" not in df.columns:
        df["end"] = ""

    # 4) Only replace blanks / NA
    before_start_na = df["start"].eq("").sum()
    before_end_na = df["end"].eq("").sum()

    df.loc[df["start"].eq(""), "start"] = df.loc[df["start"].eq(""), "_utm_key"].map(
        start_lkp
    )
    df.loc[df["end"].eq(""), "end"] = df.loc[df["end"].eq(""), "_utm_key"].map(end_lkp)

    filled_start = before_start_na - df["start"].eq("").sum()
    filled_end = before_end_na - df["end"].eq("").sum()

    log.info(
        "[utm_lookup] Filled Start/End from UTM — Start:%s  End:%s",
        filled_start,
        filled_end,
    )

    return df.drop(columns="_utm_key")
