import logging
from typing import Mapping, Iterable, Optional, List, Dict, Any

import pandas as pd
from gspread import Worksheet
from gspread.utils import rowcol_to_a1

from utils.get_google_client import get_google_client
from utils.google_sheets import CREDS_PATH, SPREADSHEET_ID
from utils.substitutions_lists import (
    ID_CONTENT_REPLACEMENTS,
    CAMPAIGN_NAME_REPLACEMENTS,
    AD_GROUP_NAME_REPLACEMENTS,
    AD_NAME_REPLACEMENTS,
)

# specs de colunas a substituir: (nome original, mapping de normalizado->substituição)
_REPLACEMENT_SPECS: List[tuple[str, Mapping[str, str]]] = [
    ("Content (utm)", ID_CONTENT_REPLACEMENTS),
    ("Campaign name", CAMPAIGN_NAME_REPLACEMENTS),
    ("Ad group name", AD_GROUP_NAME_REPLACEMENTS),
    ("Ad name", AD_NAME_REPLACEMENTS),
]

# máximo de células por batch para write-back
MAX_CELLS = 10000


def _normalize(series: pd.Series) -> pd.Series:
    """Strip + lower (preserva NaN)."""
    # mantém NaN como NaN
    s = series.astype(object).where(series.notna(), None)
    return s.fillna("").str.strip().str.lower()


def _chunk(iterable: Iterable[Any], n: int) -> Iterable[List[Any]]:
    """Yield successive n-sized chunks from iterable."""
    it = iter(iterable)
    while True:
        block = []
        for _ in range(n):
            try:
                block.append(next(it))
            except StopIteration:
                break
        if not block:
            break
        yield block



def apply_all_origin_substitutions(
    df: pd.DataFrame,
    sheet_name: Optional[str] = None,
    *,
    worksheet: Worksheet | None = None,
    write_back: bool = True,
    inplace: bool = True,
) -> pd.DataFrame:
    """
    Aplica substituições origin-to-model list (_REPLACEMENT_SPECS).
    Se write_back=True, grava de volta no sheet.
    """
    log = logging.getLogger(__name__)
    if not inplace:
        df = df.copy()
    # Prepara worksheet se for write-back
    if write_back:
        if worksheet is None:
            if sheet_name is None:
                raise ValueError("sheet_name required when write_back=True and no worksheet provided.")
            client = get_google_client(CREDS_PATH)
            worksheet = client.open_by_key(SPREADSHEET_ID).worksheet(sheet_name)
        orig_headers = worksheet.row_values(1)
        header_sheet_lc = [h.strip().lower() for h in orig_headers]

    updates: List[Dict[str, Any]] = []
    df.reset_index(drop=True, inplace=True)

    for col, mapping in _REPLACEMENT_SPECS:
        if col not in df.columns:
            log.debug("[substitutions] '%s' missing, skipping", col)
            continue
        if not mapping:
            log.debug("[substitutions] mapping empty for '%s', skipping", col)
            continue
        orig = df[col].astype(str)
        norm = _normalize(orig)
        mask = norm.isin(mapping)
        if not mask.any():
            log.debug("[substitutions] no occurrences for '%s', skipping", col)
            continue
        # Aplica
        df.loc[mask, col] = norm[mask].map(mapping)
        log.info("[substitutions] applied %d substitutions in '%s'", int(mask.sum()), col)
        # Prepara batch write-back
        if write_back:
            lc = col.strip().lower()
            if lc in header_sheet_lc:
                idx = header_sheet_lc.index(lc) + 1
                for r in df.index[mask]:
                    cell = rowcol_to_a1(r+2, idx)
                    updates.append({"range": cell, "values": [[df.at[r, col]]]})
            else:
                log.warning("[substitutions] '%s' not in sheet header, skip write-back", col)

    if write_back and updates:
        for block in _chunk(updates, MAX_CELLS):
            worksheet.batch_update(block, value_input_option="RAW")
        log.info("apply_all_origin_substitutions: wrote %d cells", len(updates))

    return df
