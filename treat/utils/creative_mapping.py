# creative_mappging.py

import logging
import pandas as pd
from gspread.utils import rowcol_to_a1
from utils.get_google_client import get_google_client
from utils.google_sheets import CREDS_PATH as creds_path, SPREADSHEET_ID
from utils.normalize import normalize_columns
import unicodedata


def _normalize(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.strip().lower().replace("\n", " ")


def load_ad_name_mapping(df_parametrizacao: pd.DataFrame) -> dict[str, str]:
    """
    Devolve {utm_content -> CRIATIVO} normalizando cabeçalhos
    ('UTM_CONTENT', 'CRIATIVO', etc.) para lower-case.
    """
    if df_parametrizacao.empty:
        logging.warning("[load_ad_name_mapping] DataFrame vazio")
        return {}

    # ── normaliza cabeçalhos ──────────────────────────────────────────────────
    df_norm = df_parametrizacao.copy()
    df_norm.columns = [_normalize(c) for c in df_norm.columns]

    missing = {"utm_content", "CRIATIVO"} - set(df_norm.columns)
    if missing:
        logging.warning(
            "[load_ad_name_mapping] faltam colunas: %s", ", ".join(sorted(missing))
        )
        return {}

    mapping = (
        df_norm[["utm_content", "CRIATIVO"]].dropna().astype(str).applymap(str.strip)
    )  # remove espaços
    return dict(zip(mapping["utm_content"], mapping["CRIATIVO"]))


def get_ad_name_from_utm_content(utm_content: str, mapping_criativo: dict) -> str:
    """
    Dado um valor de utm_content (ex.: 'abc123') e um dicionário {utm_content -> CRIATIVO},
    retorna o 'CRIATIVO' correspondente.

    Se não encontrar, retorna string vazia.
    """
    if not isinstance(utm_content, str):
        return ""
    return mapping_criativo.get(utm_content.strip(), "")


def get_utm_content_from_ad_name(
    df: pd.DataFrame,
    mapping_criativo: dict[str, str],
    coluna_ad_name: str = "Ad name",
    coluna_destino: str = "Content (utm)",
    *,
    sheet_name: str | None = None,
    worksheet=None,
    write_back: bool = False
) -> pd.DataFrame:
    """
    Mapeia 'Ad name' → 'Content (utm)' invertendo o mapping_criativo (utm_content -> CRIATIVO)
    e, se write_back=True, grava somente as células vazias que receberam um novo utm.
    """
    log = logging.getLogger(__name__)
    df = df.copy()

    # Inverte mapping criativo→utm_content para utm_content lookup
    inv = {v: k for k, v in mapping_criativo.items()}

    # Prepara worksheet para write-back
    if write_back:
        if worksheet is None:
            if sheet_name is None:
                raise ValueError("sheet_name é obrigatório quando write_back=True")
            client = get_google_client(creds_path)
            worksheet = client.open_by_key(SPREADSHEET_ID).worksheet(sheet_name)
        header = worksheet.row_values(1)
        header_lc = [h.strip().lower() for h in header]

    updates: list[dict] = []
    # Itera somente para preencher vazios
    for idx, ad_name in df[coluna_ad_name].astype(str).str.strip().items():
        original = str(df.at[idx, coluna_destino]).strip()
        # só tenta mapear se estiver vazio
        if not original:
            utm = inv.get(ad_name, "")
            if utm:
                df.at[idx, coluna_destino] = utm
                if write_back:
                    try:
                        col_idx = header_lc.index(coluna_destino.lower()) + 1
                    except ValueError:
                        log.warning(
                            "Coluna '%s' não encontrada no header; pulando write-back",
                            coluna_destino,
                        )
                        continue
                    cell = rowcol_to_a1(idx + 2, col_idx)
                    updates.append({"range": cell, "values": [[utm]]})

    if write_back and updates:
        worksheet.batch_update(updates, value_input_option="RAW")
        log.info(
            "[get_utm_content_from_ad_name] %d células atualizadas em '%s'",
            len(updates),
            coluna_destino,
        )

    return df
