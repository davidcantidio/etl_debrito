import logging
import time as _time
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd
from gspread.utils import rowcol_to_a1

from utils.get_google_client import get_google_client
from utils.normalize import normalize_columns

log = logging.getLogger(__name__)


class BIParamLookup:
    """
    Cache e lookup sobre a aba 'BI_PARAMETRIZAÇÃO'.

    Métodos principais:
      - get_taxonomy_camp_name_and_id_from_ad_name()
      - utm_start_end()
      - get_criativo_mapping()
      - lookup_utm_for_ad_name()
      - fill_utm_content_from_ad_name()
    """
    SHEET_NAME = "BI_PARAMETRIZAÇÃO"
    HEADER_ROW = 0     # cabeçalho real na primeira linha (índice 0)
    _TTL = 60 * 10     # 10 minutos de cache

    def __init__(self, creds_path: str, spreadsheet_id: str):
        self.creds_path = creds_path
        self.spreadsheet_id = spreadsheet_id
        self._df: Optional[pd.DataFrame] = None
        self._last_load: float = 0.0
        self._col_cache: Dict[str, Optional[str]] = {}
        log.debug("BIParamLookup initialized for %s", spreadsheet_id)

    def _load_df(self) -> pd.DataFrame:
        client = get_google_client(self.creds_path)
        sh = client.open_by_key(self.spreadsheet_id)
        rows = sh.worksheet(self.SHEET_NAME).get_all_values()
        if len(rows) <= self.HEADER_ROW:
            raise ValueError("BI_PARAMETRIZAÇÃO sem dados suficientes.")
        raw_hdr = rows[self.HEADER_ROW]
        cols = normalize_columns(pd.Index(raw_hdr))
        df = pd.DataFrame(rows[self.HEADER_ROW + 1 :], columns=cols)
        log.info("Loaded %d rows from BI_PARAMETRIZAÇÃO", len(df))
        return df



    def _ensure_df(self) -> None:
        if self._df is None or (_time.time() - self._last_load) > self._TTL:
            self._df = self._load_df()
            self._last_load = _time.time()
            self._col_cache.clear()

    def _find_col(self, keyword: str) -> Optional[str]:
        key = keyword.lower()
        if key in self._col_cache:
            return self._col_cache[key]
        self._ensure_df()
        for c in self._df.columns:  # type: ignore
            if key in c:
                self._col_cache[key] = c
                log.debug("Column '%s' matched for keyword '%s'", c, keyword)
                return c
        self._col_cache[key] = None
        log.warning("Column not found for keyword '%s'", keyword)
        return None

    def _map_columns(
        self, key_kw: str, val_kws: List[str], upper_keys: bool = True
    ) -> Dict[str, Tuple[str, ...]]:
        self._ensure_df()
        key_col = self._find_col(key_kw)
        val_cols = [self._find_col(v) for v in val_kws]
        if key_col is None or any(vc is None for vc in val_cols):
            raise KeyError(f"Columns for '{key_kw}' or {val_kws} not found")
        mapping: Dict[str, Tuple[str, ...]] = {}
        for row in self._df.itertuples(index=False):  # type: ignore
            raw_key = getattr(row, key_col)
            if pd.isna(raw_key):
                continue
            k = str(raw_key).strip()
            if not k:
                continue
            k = k.upper() if upper_keys else k.lower()
            vals = tuple(str(getattr(row, vc)).strip() for vc in val_cols)  # type: ignore
            mapping[k] = vals
        return mapping

    def get_taxonomy_camp_name_and_id_from_ad_name(
        self
    ) -> Tuple[Dict[str, str], Dict[str, str]]:
        """
        Retorna dois dicionários:
        - {TAXONOMY_AD_NAME_UPPER: taxonomy_campaign_name}
        - {TAXONOMY_AD_NAME_UPPER: utm_campaign}
        """
        raw = self._map_columns(
            key_kw="taxonomy_ad_name",
            val_kws=["taxonomy_campaign_name", "utm_campaign"],
            upper_keys=True
        )
        return ({k: v[0] for k, v in raw.items()}, {k: v[1] for k, v in raw.items()})

    def utm_start_end(self) -> Dict[str, Dict[str, str]]:
        """
        Retorna {utm_content_lower: {"start":..., "end":...}}
        """
        raw = self._map_columns(
            key_kw="utm_content",
            val_kws=["start", "end"],
            upper_keys=False
        )
        return {k: {"start": v[0], "end": v[1]} for k, v in raw.items()}

    def fill_missing_start_end_from_utm(
        self,
        df: pd.DataFrame,
        coluna_utm: str = "utm_content",
        coluna_start: str = "start",
        coluna_end: str = "end",
        *,
        sheet_name: Optional[str] = None,
        worksheet=None,
        write_back: bool = False
    ) -> pd.DataFrame:
        """
        Preenche in‐memory (e opcionalmente na própria aba, se write_back=True)
        as colunas coluna_start e coluna_end onde estiverem vazias,
        usando o lookup utm_content → {start,end} do cache.
        """
        out = df.copy()
        utm_map = self.utm_start_end()

        # prepara worksheet se for gravar de volta
        header_lc: List[str] = []
        updates: List[Dict[str, Any]] = []
        if write_back:
            if worksheet is None:
                if not sheet_name:
                    raise ValueError("Quando write_back=True, informe sheet_name")
                client = get_google_client(self.creds_path)
                worksheet = client.open_by_key(self.spreadsheet_id).worksheet(sheet_name)
            header_lc = [h.strip().lower() for h in worksheet.row_values(1)]

        # normaliza chave
        key_series = out.get(coluna_utm, "").astype(str).str.strip().str.lower()
        for idx, key in key_series.items():
            if not key:
                continue
            # start
            if not str(out.at[idx, coluna_start]).strip():
                novo = utm_map.get(key, {}).get("start", "")
                if novo:
                    out.at[idx, coluna_start] = novo
                    if write_back:
                        try:
                            col_idx = header_lc.index(coluna_start.lower()) + 1
                        except ValueError:
                            pass
                        else:
                            cell = rowcol_to_a1(idx + 2, col_idx)
                            updates.append({"range": cell, "values": [[novo]]})
            # end
            if not str(out.at[idx, coluna_end]).strip():
                novo = utm_map.get(key, {}).get("end", "")
                if novo:
                    out.at[idx, coluna_end] = novo
                    if write_back:
                        try:
                            col_idx = header_lc.index(coluna_end.lower()) + 1
                        except ValueError:
                            pass
                        else:
                            cell = rowcol_to_a1(idx + 2, col_idx)
                            updates.append({"range": cell, "values": [[novo]]})

        # dispara batch_update se houver algo para gravar
        if write_back and updates:
            worksheet.batch_update(updates, value_input_option="RAW")
            log.info(
                "[BIParamLookup] Preenchidas %d células de '%s'/'%s' via utm_content → start/end",
                len(updates), coluna_start, coluna_end
            )

        return out

    def get_criativo_mapping(self) -> Dict[str, str]:
        """
        Retorna {utm_content_lower: taxonomy_ad_name}
        """
        raw = self._map_columns(
            key_kw="utm_content",
            val_kws=["taxonomy_ad_name"],
            upper_keys=False
        )
        return {k: v[0] for k, v in raw.items()}

    def lookup_utm_for_ad_name(self, ad_name: str) -> str:
        """
        Inverte {utm:taxonomy_ad_name} para lookup por taxonomy_ad_name.
        """
        if not isinstance(ad_name, str):
            return ""
        inv = {v: k for k, v in self.get_criativo_mapping().items()}
        return inv.get(ad_name.strip(), "")

    def fill_utm_content_from_ad_name(
        self,
        df: pd.DataFrame,
        coluna_ad_name: str = "ad_name",
        coluna_destino: str = "utm_content",
        *,
        sheet_name: Optional[str] = None,
        worksheet=None,
        write_back: bool = False
    ) -> Tuple[pd.DataFrame, List[Dict[str,Any]]]:

        
        
        
        """
        Preenche 'utm_content' vazio mapeando de 'ad_name' via BI_PARAMETRIZAÇÃO.
        Se write_back=True, grava de volta somente as células atualizadas.
        """
        out = df.copy()
        criativo_map = self.get_criativo_mapping()
        inv_map = {v: k for k, v in criativo_map.items()}

        header_lc: List[str] = []
        if write_back:
            if worksheet is None:
                if not sheet_name:
                    raise ValueError(
                        "When write_back=True you must provide sheet_name"
                    )
                client = get_google_client(self.creds_path)
                worksheet = (
                    client.open_by_key(self.spreadsheet_id)
                    .worksheet(sheet_name)
                )
            header_lc = [h.strip().lower() for h in worksheet.row_values(1)]

        updates: List[Dict[str, Any]] = []
        for idx, ad in out[coluna_ad_name].astype(str).str.strip().items():
            if not out.at[idx, coluna_destino].strip() and ad:
                utm = inv_map.get(ad, "")
                if utm:
                    out.at[idx, coluna_destino] = utm
                    if write_back:
                        try:
                            col_idx = header_lc.index(
                                coluna_destino.lower()
                            ) + 1
                        except ValueError:
                            continue
                        cell = rowcol_to_a1(idx + 2, col_idx)
                        updates.append({"range": cell, "values": [[utm]]})

        if write_back and updates:
            worksheet.batch_update(
                updates, value_input_option="RAW"
            )
            log.info(
                "Filled %d cells of '%s' via ad_name → utm_content",
                len(updates), coluna_destino
            )

        return out


def get_campaign_parameterization(
    creds_path: str, spreadsheet_id: str
) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Atalho para o lookup de campaign_name e utm_campaign.
    """
    return BIParamLookup(creds_path, spreadsheet_id).get_taxonomy_camp_name_and_id_from_ad_name()


def load_utm_mapping(
    creds_path: str, spreadsheet_id: str
) -> Dict[str, Dict[str, str]]:
    """
    Thin-wrapper para utm_start_end() com cache.
    """
    return BIParamLookup(creds_path, spreadsheet_id).utm_start_end()

def determine_meta_ad_preview_link(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preenche a coluna 'URL_do_Anuncio' priorizando:
      1) valor já existente em URL_do_Anuncio,
      2) Preview_Link_IG,
      3) Preview_Link_FB.
    """
    df = df.rename(columns={
        "Preview Link FB": "Preview_Link_FB",
        "Preview Link IG": "Preview_Link_IG",
    })
    if "URL_do_Anuncio" not in df.columns:
        df["URL_do_Anuncio"] = ""
    def choose(row):
        if row["URL_do_Anuncio"].strip():
            return row["URL_do_Anuncio"]
        if row["Preview_Link_IG"].strip():
            return row["Preview_Link_IG"]
        return row["Preview_Link_FB"]
    df["URL_do_Anuncio"] = df.apply(choose, axis=1)
    return df


def generate_linkedin_ad_preview_link_from_lookup(df_param: pd.DataFrame) -> Dict[str, str]:
    """
    Gera mapping utm_content -> preview para LinkedIn.
    Último valor ganha em caso de duplicatas.
    """
    if not {"utm_content", "preview"}.issubset(df_param.columns):
        return {}
    df = df_param[["utm_content", "preview"]].dropna()
    # em duplicatas, manter ordem e última ganha
    mapping: Dict[str,str] = {}
    for utm, prev in zip(df["utm_content"], df["preview"]):
        mapping[utm] = prev
    return mapping


def build_pinterest_preview_link(pin_id: Any) -> str:
    """
    Constrói URL pública de preview do Pinterest a partir de ID de pin.
    """
    s = str(pin_id).strip()
    return f"https://www.pinterest.com/pin/{s}" if s else ""


def generate_pinterest_ad_preview_link(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preenche a coluna 'URL_do_Anúncio' baseado em 'Preview Link' aplicando build_pinterest_preview_link().
    """
    col = next((c for c in df.columns if c.strip().lower() == "preview link"), None)
    df["URL_do_Anúncio"] = df[col].apply(build_pinterest_preview_link) if col else ""
    return df

