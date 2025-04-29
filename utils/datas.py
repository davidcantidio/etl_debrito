# utils/datas.py

import logging
import pandas as pd
from typing import Optional, Any, Dict, List
from gspread import Worksheet
from gspread.utils import rowcol_to_a1
from utils.get_google_client import get_google_client
from utils.google_sheets import CREDS_PATH, SPREADSHEET_ID
from utils.normalize import normalize_campaign_series
from datetime import datetime, date   

MAX_CELLS = 500

def _chunk(updates: List[Dict[str, Any]], size: int):
    for i in range(0, len(updates), size):
        yield updates[i:i+size]

def fill_missing_start_end_from_params(
    df: pd.DataFrame,
    sheet_name: Optional[str] = None,
    *,
    worksheet: Worksheet | None = None,
    write_back: bool = True,
    inplace: bool = True,
) -> pd.DataFrame:
    """
    Preenche células vazias em 'Start' e 'End' via lookup em BI_PARAMETRIZAÇÃO usando o 'Ad name' ↔ 'CRIATIVO'.
    Retorna DataFrame alterado (ou cópia, se inplace=False).
    Se write_back=True, grava de volta no Google Sheet especificado.
    """
    log = logging.getLogger(__name__)
    if not inplace:
        df = df.copy()

    # Preparar worksheet para write-back
    if write_back:
        if worksheet is None:
            if sheet_name is None:
                raise ValueError(
                    "sheet_name must be provided when write_back=True and no worksheet is given."
                )
            client = get_google_client(CREDS_PATH)
            worksheet = client.open_by_key(SPREADSHEET_ID).worksheet(sheet_name)
        orig_headers = worksheet.row_values(1)
        header_sheet_lc = [h.strip().lower() for h in orig_headers]

    # Carrega BI_PARAMETRIZAÇÃO
    client = get_google_client(CREDS_PATH)
    sh = client.open_by_key(SPREADSHEET_ID)
    ws_param = sh.worksheet("BI_PARAMETRIZAÇÃO")
    data = ws_param.get_all_values()
    if len(data) < 3:
        log.error("BI_PARAMETRIZAÇÃO insuficiente (<3 linhas), abortando fill_missing_start_end")
        return df
    header_param = [c.strip().replace('\n', ' ').upper() for c in data[1]]
    df_param = pd.DataFrame(data[2:], columns=header_param)
    log.info("BI_PARAMETRIZAÇÃO loaded: %d rows", len(df_param))

    # Verifica colunas obrigatórias
    for key in ("CRIATIVO", "START", "END"):
        if key not in df_param.columns:
            log.error("Coluna '%s' faltando em BI_PARAMETRIZAÇÃO, skipping fill_missing_start_end", key)
            return df

    # Constroi mapas normalizados
    df_param["criativo_norm"] = normalize_campaign_series(df_param["CRIATIVO"])
    map_start = dict(zip(df_param["criativo_norm"], df_param["START"]))
    map_end   = dict(zip(df_param["criativo_norm"], df_param["END"]))

    updates: List[Dict[str, Any]] = []
    df.reset_index(drop=True, inplace=True)

    # Normaliza criativos do df de origem
    if "Ad name" not in df.columns:
        log.error("DataFrame origem sem coluna 'Ad name', não pode preencher datas")
        return df

    df["criativo_norm"] = normalize_campaign_series(df["Ad name"])

    # Preenche Start
    if "Start" in df.columns:
        empty_mask = df["Start"].astype(str).str.strip().eq("")
        fill_vals = df["criativo_norm"].map(map_start)
        to_fill = empty_mask & fill_vals.notna()

        filled = int(to_fill.sum())
        log.info("Preenchendo 'Start' via Ad name: %d células", filled)

        df.loc[to_fill, "Start"] = fill_vals[to_fill]

        if write_back and filled > 0:
            idx_col = header_sheet_lc.index("start") + 1 if "start" in header_sheet_lc else None
            if idx_col:
                for r in df.index[to_fill]:
                    cell = rowcol_to_a1(r + 2, idx_col)
                    updates.append({"range": cell, "values": [[df.at[r, "Start"]]]})

    # Preenche End
    if "End" in df.columns:
        empty_mask = df["End"].astype(str).str.strip().eq("")
        fill_vals = df["criativo_norm"].map(map_end)
        to_fill = empty_mask & fill_vals.notna()

        filled = int(to_fill.sum())
        log.info("Preenchendo 'End' via Ad name: %d células", filled)

        df.loc[to_fill, "End"] = fill_vals[to_fill]

        if write_back and filled > 0:
            idx_col = header_sheet_lc.index("end") + 1 if "end" in header_sheet_lc else None
            if idx_col:
                for r in df.index[to_fill]:
                    cell = rowcol_to_a1(r + 2, idx_col)
                    updates.append({"range": cell, "values": [[df.at[r, "End"]]]})

    # Executa write-back
    if write_back and updates:
        for block in _chunk(updates, MAX_CELLS):
            worksheet.batch_update(block, value_input_option="RAW")
        log.info("fill_missing_start_end: wrote %d cells", len(updates))
    else:
        log.info("fill_missing_start_end: no updates needed.")

    return df


def transformar_para_date(valor):
    """
    Transforma um valor de data no formato 'YYYY-MM-DD HH:MM:SS' ou
    um objeto datetime em um objeto date (YYYY-MM-DD).

    Parâmetros:
        valor (str ou datetime ou date): Data no formato "YYYY-MM-DD HH:MM:SS"
                                         ou já um objeto datetime ou date.

    Retorna:
        date: objeto da classe date (ex: 2024-04-15)
    """
    if not valor:
        return None

    if isinstance(valor, date) and not isinstance(valor, datetime):
        return valor

    if isinstance(valor, datetime):
        return valor.date()

    if isinstance(valor, str):
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(valor.strip(), fmt).date()
            except ValueError:
                continue
        raise ValueError(f"Formato de data não reconhecido: {valor}")
    
    raise ValueError(f"Tipo de valor não suportado: {type(valor)}")


def converter_data(df: pd.DataFrame, coluna: str) -> pd.DataFrame:
    if coluna in df.columns:
        parsed = pd.to_datetime(df[coluna], errors="coerce")
        df[coluna] = parsed.dt.date.where(~parsed.isna(), "")
    return df



def generate_pinterest_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preenche as colunas 'Inicio_da_Campanha' e 'Fim_da_Campanha' convertendo
    as colunas 'start' e 'end' (datetime) para datas (YYYY-MM-DD) no Pinterest.
    """
    if 'start' in df.columns:
        df['Inicio_da_Campanha'] = df['start'].apply(transformar_para_date)
    else:
        df['Inicio_da_Campanha'] = ""

    if 'end' in df.columns:
        df['Fim_da_Campanha'] = df['end'].apply(transformar_para_date)
    else:
        df['Fim_da_Campanha'] = ""

    return df

