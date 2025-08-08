# utils/bi_param_utils.py
"""
Módulo único para todas as funções que leem e manipulam
conteúdo da aba BI_PARAMETRIZAÇÃO (lookup, preenchimento, preview etc.),
antes de dividirmos em treat/common/.
"""

import logging
import time
import unicodedata
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from gspread import Worksheet
from utils.get_google_client import get_google_client
from utils.google_sheets import CREDS_PATH, SPREADSHEET_ID
from utils.normalize import normalize_campaign_series, normalize_columns
from utils.read_sheet_as_dataframe import read_sheet_as_dataframe_range

log = logging.getLogger(__name__)


# ------------------------------------------------------------------------------
# 1) BIParamLookup: cache e geração de lookup dicts da planilha BI_PARAMETRIZAÇÃO
# ------------------------------------------------------------------------------
class BIParamLookup:
    SHEET_NAME = "BI_PARAMETRIZAÇÃO"
    HEADER_ROW = 1  # 0-indexed
    _TTL = 60 * 10  # 10 minutos

    def __init__(self, creds_path: str, spreadsheet_id: str):
        self.creds_path = creds_path
        self.spreadsheet_id = spreadsheet_id
        self._df: Optional[pd.DataFrame] = None
        self._last_load: float = 0.0
        self._col_cache: Dict[str, Optional[str]] = {}
        log.debug("Inicializado BIParamLookup for %s", spreadsheet_id)

    def _load_df(self) -> pd.DataFrame:
        client = get_google_client(self.creds_path)
        sheet = client.open_by_key(self.spreadsheet_id)
        rows = sheet.worksheet(self.SHEET_NAME).get_all_values()
        if len(rows) <= self.HEADER_ROW:
            raise ValueError("BI_PARAMETRIZAÇÃO sem dados suficientes.")
        raw_hdr = rows[self.HEADER_ROW]
        hdr = normalize_columns(pd.Index(raw_hdr))
        df = pd.DataFrame(rows[self.HEADER_ROW + 1 :], columns=hdr)
        log.info("BI_PARAM loaded %d rows", len(df))
        return df

    def _ensure_df(self):
        if self._df is None or time.time() - self._last_load > self._TTL:
            self._df = self._load_df()
            self._last_load = time.time()
            self._col_cache.clear()

    def _find_col(self, keyword: str) -> Optional[str]:
        key = keyword.lower()
        if key in self._col_cache:
            return self._col_cache[key]
        self._ensure_df()
        for c in self._df.columns:  # type: ignore
            if key in c:
                self._col_cache[key] = c
                return c
        self._col_cache[key] = None
        log.warning("Coluna p/ %s não encontrada", keyword)
        return None

    def _map_columns(
        self, key_kw: str, val_kws: List[str], upper_keys: bool = True
    ) -> Dict[str, Tuple[str, ...]]:
        self._ensure_df()
        key_col = self._find_col(key_kw)
        val_cols = [self._find_col(v) for v in val_kws]
        if key_col is None or any(vc is None for vc in val_cols):
            raise KeyError(f"Colunas para {key_kw} ou {val_kws} não encontradas")
        out: Dict[str, Tuple[str, ...]] = {}
        for row in self._df.itertuples(index=False):  # type: ignore
            raw = getattr(row, key_col)
            if pd.isna(raw):
                continue
            k = str(raw).strip()
            if not k:
                continue
            k = k.upper() if upper_keys else k.lower()
            vals = tuple(str(getattr(row, vc)).strip() for vc in val_cols)  # type: ignore
            out[k] = vals
        return out

    def get_taxonomy_camp_name_and_id_from_ad_name(
        self,
    ) -> Tuple[Dict[str, str], Dict[str, str]]:
        raw = self._map_columns(
            "ad_name", ["taxonomy_campaign_name", "utm_campaign"], upper_keys=True
        )
        return ({k: v[0] for k, v in raw.items()}, {k: v[1] for k, v in raw.items()})

    def utm_start_end(self) -> Dict[str, Dict[str, str]]:
        raw = self._map_columns("utm_content", ["start", "end"], upper_keys=False)
        return {k: {"start": v[0], "end": v[1]} for k, v in raw.items()}

    def get_criativo_mapping(self) -> Dict[str, str]:
        raw = self._map_columns("utm_content", ["criativo"], upper_keys=False)
        return {k: v[0] for k, v in raw.items()}

    def lookup_utm_for_ad_name(self, ad_name: str) -> str:
        if not isinstance(ad_name, str):
            return ""
        inv = {v: k for k, v in self.get_criativo_mapping().items()}
        return inv.get(ad_name.strip(), "")


def get_campaign_parameterization(
    creds_path: str, spreadsheet_id: str
) -> Tuple[Dict[str, str], Dict[str, str]]:
    return BIParamLookup(
        creds_path, spreadsheet_id
    ).get_taxonomy_camp_name_and_id_from_ad_name()


# ------------------------------------------------------------------------------
# 2) Creative mapping (LinkedIn utm_content ↔ Criativo)
# ------------------------------------------------------------------------------
def _normalize_str(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.strip().lower()


def load_ad_name_mapping(df_param: pd.DataFrame) -> Dict[str, str]:
    df = df_param.copy()
    df.columns = [_normalize_str(c) for c in df.columns]
    if not {"utm_content", "criativo"}.issubset(df.columns):
        log.warning(
            "Creative mapping: colunas faltando %s",
            set(("utm_content", "criativo")) - set(df.columns),
        )
        return {}
    df = df[["utm_content", "criativo"]].dropna().applymap(str.strip)
    return dict(zip(df["utm_content"], df["criativo"]))


def get_ad_name_from_utm_content(utm: str, mapping: Dict[str, str]) -> str:
    return mapping.get(utm.strip(), "")


def fill_utm_from_ad_name(
    df: pd.DataFrame,
    mapping: Dict[str, str],
    coluna_ad: str = "Ad name",
    coluna_dest: str = "Content (utm)",
    write_back: bool = False,
    sheet_name: Optional[str] = None,
) -> pd.DataFrame:
    inv = {v: k for k, v in mapping.items()}
    df = df.copy()
    updates: List[Dict[str, Any]] = []
    # prepare worksheet if write_back...
    for idx, ad in df[coluna_ad].astype(str).str.strip().items():
        if not df.at[idx, coluna_dest].strip():
            utm = inv.get(ad, "")
            if utm:
                df.at[idx, coluna_dest] = utm
                # se write_back, acumular updates...
    # se write_back: batch_update
    return df


# ------------------------------------------------------------------------------
# 3) Datas e parâmetros (fill_missing_start_end_from_params)
# ------------------------------------------------------------------------------
MAX_CELLS = 500


def _chunk(upd: List[Dict[str, Any]], size: int):
    for i in range(0, len(upd), size):
        yield upd[i : i + size]


def fill_missing_start_end_from_params(
    df: pd.DataFrame,
    sheet_name: Optional[str] = None,
    worksheet: Optional[Worksheet] = None,
    write_back: bool = True,
    inplace: bool = True,
) -> pd.DataFrame:
    if not inplace:
        df = df.copy()
    # carregar BI_PARAMETRIZAÇÃO raw (linha 2)
    client = get_google_client(CREDS_PATH)
    sh = client.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet("BI_PARAMETRIZAÇÃO")
    data = ws.get_all_values()
    hdr = [c.strip().upper() for c in data[1]]
    dfp = pd.DataFrame(data[2:], columns=hdr)
    # normalizar criativo, map_start, map_end
    df["criativo_norm"] = normalize_campaign_series(df["ad_name"])
    # preenche start e end, write_back se solicitado...
    return df


def transformar_para_date(v):
    if not v:
        return None
    if isinstance(v, date):
        return v
    if isinstance(v, datetime):
        return v.date()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(v.strip(), fmt).date()
        except:
            continue
    raise ValueError("Formato inválido")


def converter_data(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date.fillna("")
    return df


def generate_pinterest_dates(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["Inicio_da_Campanha"] = df.get("start", "").apply(transformar_para_date)
    df["Fim_da_Campanha"] = df.get("end", "").apply(transformar_para_date)
    return df


# ------------------------------------------------------------------------------
# 4) UTM-Lookup simplificado (via DataFrame)
# ------------------------------------------------------------------------------
def load_utm_mapping() -> Dict[str, Dict[str, str]]:
    client = get_google_client(CREDS_PATH)
    df = read_sheet_as_dataframe_range(
        client, SPREADSHEET_ID, "BI_PARAMETRIZAÇÃO", "A2:ZZ", 0
    )
    df["utm_content"] = df.get("utm_content", "").astype(str).str.strip().str.lower()
    out = {}
    for _, r in df.iterrows():
        k = r["utm_content"]
        if k:
            out[k] = {"start": r.get("start", ""), "end": r.get("end", "")}
    return out


def fill_missing_start_end_from_utm(
    df: pd.DataFrame, utm_map: Dict[str, Dict[str, str]]
) -> pd.DataFrame:
    df = df.copy()
    key = df.get("Content (utm)", "").astype(str).str.strip().str.lower()
    df["start"] = df["start"].mask(
        df["start"].eq(""), key.map({k: v["start"] for k, v in utm_map.items()})
    )
    df["end"] = df["end"].mask(
        df["end"].eq(""), key.map({k: v["end"] for k, v in utm_map.items()})
    )
    return df


# ------------------------------------------------------------------------------
# 5) Preview-links
# ------------------------------------------------------------------------------
def determine_meta_ad_preview_link(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "Preview Link FB": "Preview_Link_FB",
            "Preview Link IG": "Preview_Link_IG",
        }
    )
    df["URL_do_Anuncio"] = df.get("URL_do_Anuncio", "").mask(
        df.get("URL_do_Anuncio", "") != "",
        df.get("Preview_Link_IG", "").mask("", df.get("Preview_Link_FB", "")),
    )
    return df


def generate_linkedin_ad_preview_link_from_lookup(
    df_param: pd.DataFrame,
) -> Dict[str, str]:
    if not {"utm_content", "preview"}.issubset(df_param.columns):
        return {}
    m = df_param[["utm_content", "preview"]].dropna().drop_duplicates("utm_content")
    return dict(zip(m["utm_content"], m["preview"]))


def build_pinterest_preview_link(pin_id: Any) -> str:
    s = str(pin_id).strip()
    return f"https://www.pinterest.com/pin/{s}" if s else ""


def generate_pinterest_ad_preview_link(df: pd.DataFrame) -> pd.DataFrame:
    col = next((c for c in df.columns if c.strip().lower() == "preview link"), None)
    df["URL_do_Anúncio"] = (
        df.get(col, "").apply(build_pinterest_preview_link) if col else ""
    )
    return df
