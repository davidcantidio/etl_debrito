import logging
import time as _time
from typing import Any, Dict, List, Optional, Tuple, Sequence
import pandas as pd
from gspread.utils import rowcol_to_a1

from treat.utils.get_google_client import get_google_client
from treat.utils.normalize import normalize_columns

log = logging.getLogger(__name__)

class BIParamLookup:
    """
    Acesso parametrizado à aba BI_PARAMETRIZAÇÃO com cache global.

    • A primeira instância carrega o DataFrame; as demais reutilizam.
    • TTL de 10 min (_TTL) continua valendo para todo o processo.
    """

    SHEET_NAME = "BI_PARAMETRIZAÇÃO"
    HEADER_ROW = 0
    _TTL = 60 * 10  # 10 min

    # ─── cache compartilhado entre TODAS as instâncias ──────────────────────
    _df: Optional[pd.DataFrame] = None
    _last_load: float = 0.0

    def __init__(self, creds_path: str, spreadsheet_id: str):
        self.creds_path = creds_path
        self.spreadsheet_id = spreadsheet_id
        # cache de correspondência de colunas continua por instância
        self._col_cache: Dict[str, Optional[str]] = {}

    # ────────────────────────────────────────────────────────────────────────
    # helpers internos
    # ────────────────────────────────────────────────────────────────────────
    def _load_df(self) -> pd.DataFrame:
        client = get_google_client(self.creds_path)
        sh = client.open_by_key(self.spreadsheet_id)
        rows = sh.worksheet(self.SHEET_NAME).get_all_values()
        cols = rows[self.HEADER_ROW]
        return pd.DataFrame(rows[self.HEADER_ROW + 1 :], columns=cols)

    def _ensure_df(self) -> None:
        if (
            BIParamLookup._df is None
            or (_time.time() - BIParamLookup._last_load) > self._TTL
        ):
            BIParamLookup._df = self._load_df()
            BIParamLookup._last_load = _time.time()
            # limpa apenas o cache de coluna da instância corrente
            self._col_cache.clear()

    def _find_col(self, keyword: str) -> Optional[str]:
        key = keyword.lower()
        if key in self._col_cache:
            return self._col_cache[key]

        self._ensure_df()
        for c in BIParamLookup._df.columns:
            if key in c.lower():
                self._col_cache[key] = c
                return c
        self._col_cache[key] = None
        return None

    # ────────────────────────────────────────────────────────────────────────
    # métodos públicos (demais código permanece inalterado)
    # ────────────────────────────────────────────────────────────────────────
    def _map_columns(
        self,
        key_kw: str,
        val_kws: list[str],
        *,
        upper_keys: bool = True,
    ) -> dict[str, tuple[str, ...]]:
        self._ensure_df()

        key_col = self._find_col(key_kw)
        val_cols = [self._find_col(v) for v in val_kws]
        if key_col is None or any(vc is None for vc in val_cols):
            raise KeyError(
                f"Colunas não encontradas — chave: '{key_kw}', valores: {val_kws}"
            )

        mapping: dict[str, tuple[str, ...]] = {}
        for row in BIParamLookup._df.itertuples(index=False):
            raw_key = getattr(row, key_col)
            if pd.isna(raw_key) or str(raw_key).strip() == "":
                continue

            k_norm = (str(raw_key).strip().upper() if upper_keys
                      else str(raw_key).strip().lower())
            vals = tuple(str(getattr(row, vc)).strip() for vc in val_cols)
            mapping[k_norm] = vals

        return mapping
    def get_campaign_maps(
    self,
    prefer_cols: Sequence[str] = ("utm_content", "taxonomy_campaign_name"),
) -> tuple[dict[str, str], dict[str, str]]:
        """
        Retorna (camp_map, utm_map) mesclando todas as colunas listadas
        em `prefer_cols`.  Último valor *não* sobrescreve caso a chave já exista.
        Todas as chaves saem normalizadas em lower/strip.
        """
        self._ensure_df()
        cols_lc = {c.lower(): c for c in self._df.columns}

        camp_map: dict[str, str] = {}
        id_map:   dict[str, str] = {}

        for key_kw in prefer_cols:
            col_name = cols_lc.get(key_kw.lower())
            if not col_name:
                continue

            raw = self._map_columns(
                key_kw   = key_kw,
                val_kws  = ["campaign_name", "utm_campaign"],
                upper_keys = False        # mantém em lower
            )
            for k, (camp, utm) in raw.items():
                k_norm = k.strip().lower()
                # só adiciona se ainda não existe (preserva prioridade da 1ª coluna)
                camp_map.setdefault(k_norm, camp)
                id_map.setdefault(k_norm, utm)

        return camp_map, id_map
    
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
        Preenche 'utm_content' vazio a partir de 'ad_name' usando BI_PARAMETRIZAÇÃO.
        Agora também loga quais ad_name não foram encontrados.
        """
        out = df.copy()

        if coluna_ad_name not in out.columns:
            log.debug("[BIParamLookup] coluna '%s' ausente; skip fill_utm_content", coluna_ad_name)
            return out

        # lookup invertido taxonomy_ad_name → utm_content
        raw = self._map_columns(
            key_kw="utm_content",
            val_kws=["taxonomy_ad_name", "taxonomy_ad_name_social"],
            upper_keys=False,
        )
        inv_map = {
            v[0].strip().upper(): k
            for k, v in raw.items()
            if v[0]
        }

        header_lc, updates = [], []
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

        not_found: list[tuple[int, str]] = []

        for idx, ad in out[coluna_ad_name].astype(str).str.strip().items():
            if not ad:
                continue  # ignora vazios
            if str(out.at[idx, coluna_destino]).strip():
                continue
            utm = inv_map.get(ad.upper(), "")
            if not utm:
                not_found.append((idx, ad))
                continue
            out.at[idx, coluna_destino] = utm
            if write_back and coluna_destino.lower() in header_lc:
                c = header_lc.index(coluna_destino.lower()) + 1
                updates.append({"range": rowcol_to_a1(idx + 2, c), "values": [[utm]]})

        if write_back and updates:
            worksheet.batch_update(updates, value_input_option="RAW")
            log.info(
                "[BIParamLookup] Preenchidas %d células de '%s' via ad_name → %s",
                len(updates), coluna_ad_name, coluna_destino
            )

        if not_found:
            preview = ", ".join(f"{i}:{v}" for i, v in not_found[:10])
            if len(not_found) > 10:
                preview += ", …"
            log.info(
                "[BIParamLookup] %d ad_name(s) sem correspondência em BI_PARAMETRIZAÇÃO: %s",
                len(not_found), preview
            )

        return out

    def get_linkedin_ad_name_map(self) -> dict[str,str]:
        if not hasattr(self, "_li_ad_name_map"):
            df_param = self._load_df()
            cols = {c.strip().lower(): c for c in df_param.columns}
            utm_col = cols.get("utm_content_raw", cols.get("utm_content"))
            name_col = cols.get("taxonomy_ad_name_social")
            if utm_col is None or name_col is None:
                logging.warning("Não achei colunas 'utm_content[_raw]' ou 'taxonomy_ad_name_social' em BI_PARAMETRIZAÇÃO.")
                self._li_ad_name_map = {}
            else:
                self._li_ad_name_map = (
                    df_param[[utm_col, name_col]]
                    .dropna(subset=[utm_col, name_col])
                    .assign(
                        _utm=lambda d: d[utm_col].astype(str).str.strip(),
                        _name=lambda d: d[name_col].astype(str).str.strip(),
                    )
                    .drop_duplicates(subset=["_utm"])
                    .set_index("_utm")["_name"]
                    .to_dict()
                )
        return self._li_ad_name_map
    
    def get_objective_map(self) -> Dict[str, str]:
            """
            Retorna um dicionário {utm_content: objective} a partir da aba BI_PARAMETRIZAÇÃO.
            """
            # garante df carregado
            self._ensure_df()
            # encontra colunas
            cols = {c.strip().lower(): c for c in self._df.columns}
            utm_col = cols.get("utm_content_raw") or cols.get("utm_content")
            obj_col = cols.get("objective")
            if utm_col is None or obj_col is None:
                log.warning("Não achei colunas 'utm_content[_raw]' ou 'objective' em BI_PARAMETRIZAÇÃO.")
                return {}
            # build map
            df = self._df[[utm_col, obj_col]].dropna(subset=[utm_col, obj_col])
            return {
                str(row[utm_col]).strip(): str(row[obj_col]).strip()
                for _, row in df.iterrows()
            }
    
    def df(self) -> pd.DataFrame:
        """DataFrame da BI_PARAMETRIZAÇÃO em cache (atualiza se TTL vencer)."""
        self._ensure_df()
        return BIParamLookup._df


def fill_objective_from_bi(
    df: pd.DataFrame,
    lookup: BIParamLookup,
    *,
    key_col: str = "utm_content",
    objective_col: str = "objective",
) -> pd.DataFrame:
        """
        Preenche objective vazio via mapping utm_content → objective usando BIParamLookup.
        """
        if key_col not in df.columns:
            return df
        obj_map = lookup.get_objective_map()
        # só preenche onde objective está vazio e utm_content não vazio
        mask = (
            df.get(objective_col, pd.Series("", index=df.index))
            .astype(str).str.strip().eq("")
            & df[key_col].astype(str).str.strip().ne("")
        )

        df.loc[mask, objective_col] = (
            df.loc[mask, key_col]
            .astype(str).str.strip()
            .map(obj_map)
            .fillna("")
        )
        return df


def enrich_with_bi_parametrizacao(
    df: pd.DataFrame,
    creds_path: str,
    spreadsheet_id: str,
    *,
    sheet_name: str | None = None,
) -> pd.DataFrame:
    lookup = BIParamLookup(creds_path, spreadsheet_id)

    # 1) utm_content ← ad_name (caso necessário)
    df = lookup.fill_utm_content_from_ad_name(
        df, coluna_ad_name="ad_name", coluna_destino="utm_content", write_back=False
    )

    # 2) dicionários de BI (já normalizados em lowercase)
    camp_map, id_map = lookup.get_campaign_maps()
    camp_map = {k.lower(): v for k, v in camp_map.items()}
    id_map   = {k.lower(): v for k, v in id_map.items()}

    # 3) chave: utm_content  ➜  se vazio usa campaign_name
    key_series = (
        df.get("utm_content", pd.Series("", index=df.index))
        .astype(str).str.strip()
        .where(lambda s: s != "",
               df.get("campaign_name", pd.Series("", index=df.index))
                 .astype(str).str.strip())
        .str.lower()
    )

    # 4) aplica mapeamento apenas em células realmente vazias
    if "Campanha" not in df.columns:
        df["Campanha"] = ""
    if "ID_Campanha" not in df.columns:
        df["ID_Campanha"] = ""

    mask_camp = df["Campanha"].astype(str).str.strip() == ""
    mask_id   = df["ID_Campanha"].astype(str).str.strip() == ""

    df.loc[mask_camp, "Campanha"]    = key_series.map(camp_map)
    df.loc[mask_id,   "ID_Campanha"] = key_series.map(id_map)

    # 5) start/end via utm_content
    df = lookup.fill_missing_start_end_from_utm(
        df, write_back=False, sheet_name=sheet_name
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
    return BIParamLookup(creds_path, spreadsheet_id).get_campaign_maps()

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

