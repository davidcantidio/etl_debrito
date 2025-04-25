import logging
from typing import Mapping, Iterable, Optional
import itertools

import pandas as pd
from gspread.utils import rowcol_to_a1
from gspread import Worksheet

from utils.substitutions_lists import (
    ID_CONTENT_REPLACEMENTS,
    CAMPAIGN_NAME_REPLACEMENTS,
    AD_GROUP_NAME_REPLACEMENTS,
)
from utils.get_google_client import get_google_client
from utils.google_sheets       import CREDS_PATH, SPREADSHEET_ID

_REPLACEMENT_SPECS: list[tuple[str, Mapping[str, str]]] = [
    ("Content (utm)", ID_CONTENT_REPLACEMENTS),
    ("Campaign name", CAMPAIGN_NAME_REPLACEMENTS),
    ("Ad group name", AD_GROUP_NAME_REPLACEMENTS),
]

MAX_CELLS = 10_000  # segurança para o payload da API

def _normalize(series: pd.Series) -> pd.Series:
    """strip + lower (preserva NaN)."""
    return series.astype(str).str.strip().str.lower()

def _chunk(iterable: Iterable, n: int):
    """
    Yield successive n-sized chunks, sem levantar StopIteration.
    """
    it = iter(iterable)
    while True:
        block = list(itertools.islice(it, n))
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
    Aplica substituições célula-a-célula em até três colunas dos dados de origem
    e, se write_back=True, grava as alterações na planilha Google Sheets via batchUpdate().

    Parâmetros
    ----------
    df : pd.DataFrame
        Dados carregados da aba de origem (linha 1 = header).
    sheet_name : str | None
        Nome da aba; obrigatório se write_back e worksheet não forem fornecidos.
    worksheet : gspread.Worksheet | None
        Worksheet já autenticado – facilita testes/mocks.
    write_back : bool
        Se False, só altera o DataFrame (útil em testes).
    inplace : bool
        Se False, devolve cópia em vez de alterar df em lugar.
    """
    log = logging.getLogger(__name__)
    if not inplace:
        df = df.copy()

    # prepara worksheet se for gravar de volta
    if write_back:
        if worksheet is None:
            if not sheet_name:
                raise ValueError("sheet_name deve ser informado para gravação no Sheets.")
            client = get_google_client(CREDS_PATH)
            worksheet = client.open_by_key(SPREADSHEET_ID).worksheet(sheet_name)
        header_sheet = [h.strip() for h in worksheet.row_values(1)]

    updates: list[dict] = []
    df.reset_index(drop=True, inplace=True)  # garante index contínuo

    for col, mapping in _REPLACEMENT_SPECS:
        if col not in df.columns:
            log.debug("[substitutions] coluna ausente → %s", col)
            continue
        if not mapping:
            log.debug("[substitutions] mapping vazio (%s) – nada a fazer", col)
            continue

        orig = df[col].astype(str)
        norm = _normalize(orig)
        mask = norm.isin(mapping.keys())
        n_replaced = int(mask.sum())
        if n_replaced == 0:
            log.debug("[substitutions] nenhum valor a substituir em '%s'", col)
            continue

        # amostra para debug
        sample_before = orig[mask].head(3).tolist()
        sample_after  = [mapping[k] for k in _normalize(pd.Series(sample_before))]
        log.debug(
            "[substitutions] %d/%d valores em '%s' serão trocados. Ex.: %s → %s",
            n_replaced, len(df), col, sample_before, sample_after
        )

        # aplica no DataFrame
        df.loc[mask, col] = norm[mask].map(mapping)

        # prepara payload de atualização
        if write_back and col in header_sheet:
            col_idx = header_sheet.index(col) + 1  # 1-based
            for row_idx in df.index[mask]:
                a1 = rowcol_to_a1(row_idx + 2, col_idx)
                updates.append({"range": a1, "values": [[df.at[row_idx, col]]]})
        log.info("[substitutions] %d valores substituídos em '%s'", n_replaced, col)

    # faz o batch_update em pedaços de MAX_CELLS
    if write_back and updates:
        log.debug("[substitutions] preparando %d updates…", len(updates))
        for batch in _chunk(updates, MAX_CELLS):
            worksheet.batch_update(batch, value_input_option="RAW")
        log.info("[substitutions] gravação concluída (%d células)", len(updates))

    return df
