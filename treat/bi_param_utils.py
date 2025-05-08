import logging
import time as _time
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd
from gspread.utils import rowcol_to_a1

from utils.get_google_client import get_google_client
from utils.normalize import normalize_columns

log = logging.getLogger(__name__)

class BIParamLookup:
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
        cols = rows[self.HEADER_ROW]
        return pd.DataFrame(rows[self.HEADER_ROW + 1 :], columns=cols)

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
        for c in self._df.columns:
            if key in c.lower():
                self._col_cache[key] = c
                return c
        self._col_cache[key] = None
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
        for row in self._df.itertuples(index=False):
            raw_key = getattr(row, key_col)
            if pd.isna(raw_key):
                continue
            k = str(raw_key).strip()
            if not k:
                continue
            k = k.upper() if upper_keys else k.lower()
            vals = tuple(str(getattr(row, vc)).strip() for vc in val_cols)
            mapping[k] = vals
        return mapping

    def get_taxonomy_camp_name_and_id_from_utm_content(
        self
    ) -> Tuple[Dict[str, str], Dict[str, str]]:
        """
        Retorna dois dicionários baseados em utm_content na planilha BI_PARAMETRIZAÇÃO:
        - {utm_content: campaign_name}
        - {utm_content: utm_campaign}
        Não faz nenhuma transformação de case ou strip.
        """
        raw = self._map_columns(
            key_kw="utm_content",
            val_kws=["campaign_name", "utm_campaign"],
            upper_keys=False      # mantém utm_content como está
        )
        return (
            {k: v[0] for k, v in raw.items()},
            {k: v[1] for k, v in raw.items()}
        )

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
        Preenche start/end vazios usando utm_content → start/end.
        """
        out = df.copy()
        # ── Se não existir utm_content, retorna sem alterações ────────────────
        if coluna_utm not in out.columns:
            return out

        # ── Garante que start e end existam (mesmo que vazias) ──────────────
        if coluna_start not in out.columns:
            out[coluna_start] = ""
        if coluna_end not in out.columns:
            out[coluna_end] = ""
        utm_map = self.utm_start_end()  # agora existe!

        header_lc: List[str] = []
        updates: List[Dict[str, Any]] = []
        if write_back:
            if worksheet is None:
                if not sheet_name:
                    raise ValueError("Quando write_back=True, informe sheet_name")
                worksheet = (
                    get_google_client(self.creds_path)
                    .open_by_key(self.spreadsheet_id)
                    .worksheet(sheet_name)
                )
            header_lc = [h.strip().lower() for h in worksheet.row_values(1)]

        for idx, key in out.get(coluna_utm, "").astype(str).str.strip().str.lower().items():
            if not key:
                continue
            # start
            if not str(out.at[idx, coluna_start]).strip():
                novo = utm_map.get(key, {}).get("start", "")
                if novo:
                    out.at[idx, coluna_start] = novo
                    if write_back and coluna_start.lower() in header_lc:
                        c = header_lc.index(coluna_start.lower()) + 1
                        updates.append({"range": rowcol_to_a1(idx+2, c), "values": [[novo]]})
            # end
            if not str(out.at[idx, coluna_end]).strip():
                novo = utm_map.get(key, {}).get("end", "")
                if novo:
                    out.at[idx, coluna_end] = novo
                    if write_back and coluna_end.lower() in header_lc:
                        c = header_lc.index(coluna_end.lower()) + 1
                        updates.append({"range": rowcol_to_a1(idx+2, c), "values": [[novo]]})

        if write_back and updates:
            worksheet.batch_update(updates, value_input_option="RAW")
            log.info(
                "[BIParamLookup] Preenchidas %d células de '%s'/'%s'",
                len(updates), coluna_start, coluna_end
            )

        return out

    def get_criativo_mapping(self) -> Dict[str, str]:
        """
        Retorna {taxonomy_ad_name: utm_content}, para uso em ad_name → utm_content.
        """
        raw = self._map_columns(
            key_kw="utm_content",
            val_kws=["taxonomy_ad_name"],
            upper_keys=False
        )
        # invertendo cada tupla (v[0] é taxonomy_ad_name)
        return {v[0]: k for k, v in raw.items()}
    
    def fill_utm_content_from_ad_name(
        self,
        df: pd.DataFrame,
        coluna_ad_name: str = "ad_name",
        coluna_destino: str = "utm_content",
        *,
        sheet_name: Optional[str] = None,
        worksheet=None,
        write_back: bool = False,
    ) -> pd.DataFrame:
        """
        Preenche 'utm_content' vazio fazendo ad_name → utm_content via BI.
        Se a aba não possui ad_name, retorna o DataFrame inalterado.
        """
        out = df.copy()

        # ─── GUARDA contra abas sem ad_name ──────────────────────────────────────
        if coluna_ad_name not in out.columns:
            log.debug("[BIParamLookup] coluna '%s' ausente; skip fill_utm_content", coluna_ad_name)
            return out
        # ------------------------------------------------------------------------

        # lookup invertido: taxonomy_ad_name -> utm_content
        raw = self._map_columns(
            key_kw="utm_content",
            val_kws=["taxonomy_ad_name"],
            upper_keys=False,
        )
        # v é tupla (utm_content → (taxonomy_ad_name,)), pegamos v[0]
        inv_map = {v[0].strip().upper(): k for k, v in raw.items()}

        # preparação write-back
        header_lc: List[str] = []
        updates: List[Dict[str, Any]] = []
        if write_back:
            if worksheet is None:
                if sheet_name is None:
                    raise ValueError("Quando write_back=True, informe sheet_name")
                worksheet = (
                    get_google_client(self.creds_path)
                    .open_by_key(self.spreadsheet_id)
                    .worksheet(sheet_name)
                )
            header_lc = [h.strip().lower() for h in worksheet.row_values(1)]

        # loop: preenche apenas vazios
        for idx, ad in out[coluna_ad_name].astype(str).str.strip().items():
            if str(out.at[idx, coluna_destino]).strip():
                continue
            utm = inv_map.get(ad.upper(), "")
            if not utm:
                continue
            out.at[idx, coluna_destino] = utm

            if write_back and coluna_destino.lower() in header_lc:
                c = header_lc.index(coluna_destino.lower()) + 1
                cell = rowcol_to_a1(idx + 2, c)
                updates.append({"range": cell, "values": [[utm]]})

        if write_back and updates:
            worksheet.batch_update(updates, value_input_option="RAW")
            log.info(
                "[BIParamLookup] Preenchidas %d células de '%s' via ad_name → %s",
                len(updates), coluna_ad_name, coluna_destino
            )

        return out



def enrich_with_bi_parametrizacao(
    df: pd.DataFrame,
    creds_path: str,
    spreadsheet_id: str,
    sheet_name: Optional[str] = None,
) -> pd.DataFrame:
    """
    1) Mapeia Campanha e ID_Campanha a partir de utm_content → BI  
    2) Preenche start/end (via utm_content)  
    3) Preenche utm_content vazio a partir de ad_name  
    """
    lookup = BIParamLookup(creds_path, spreadsheet_id)

    # 1) campaign_name & utm_campaign via utm_content
    camp_map, utm_map = lookup.get_taxonomy_camp_name_and_id_from_utm_content()

    # escolhe a coluna existente
    key_col = "utm_content" if "utm_content" in df.columns else "ID_Content"

    # backup de qualquer valor já em Campanha
    original_camp = df.get(
        "Campanha",
        pd.Series([""] * len(df), index=df.index)
    )

    # aplica o lookup direto, sem strip/upper/lower
    df["Campanha"]    = df[key_col].map(camp_map).combine_first(original_camp)
    df["ID_Campanha"] = df[key_col].map(utm_map)

    # 2) start/end faltantes
    df = lookup.fill_missing_start_end_from_utm(
        df, write_back=False, sheet_name=sheet_name
    )

    # 3) utm_content vazio ← ad_name
    df = lookup.fill_utm_content_from_ad_name(
        df,
        coluna_ad_name="ad_name",
        coluna_destino="utm_content",
        write_back=False
    )

    return df

def fill_missing_start_end_from_params(
    df: pd.DataFrame,
    creds_path: str,
    spreadsheet_id: str,
    sheet_name: Optional[str] = None,
    write_back: bool = True
) -> pd.DataFrame:
    """
    Wrapper que preenche start/end faltantes via utm_content:
     - df: DataFrame bruto ou preprocessado
     - creds_path, spreadsheet_id: conexão com o Sheets
     - sheet_name: nome da aba (necessário se write_back=True)
     - write_back: se True, grava as células faltantes na própria aba
    """
    lookup = BIParamLookup(creds_path, spreadsheet_id)
    # aqui chamamos o método que já existe internamente
    return lookup.fill_missing_start_end_from_utm(
        df,
        coluna_utm="utm_content",
        coluna_start="start",
        coluna_end="end",
        sheet_name=sheet_name,
        write_back=write_back
    )

def get_campaign_parameterization(
    creds_path: str, spreadsheet_id: str
) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Atalho para o lookup de campaign_name e utm_campaign.
    """
    return BIParamLookup(creds_path, spreadsheet_id).get_taxonomy_camp_name_and_id_from_utm_content()

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
