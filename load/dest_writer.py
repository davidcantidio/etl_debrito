# File: load/dest_writer.py
"""Write‑back otimizado para as abas **modelo\***

Principais características
-------------------------
* **01 leitura**: cabeçalho (linha 1) e coluna **ID** das 5 abas‑modelo são
  obtidos de uma só vez via *batchGet* – não estoura a quota de leitura.
* **Zero leituras extras** durante o loop de ETL; somente gravação.
* **Deduplicação**: grava apenas registros cujo ``ID`` ainda não existe.
* Compatível com *dry‑run* para testes e com execução em paralelo.
"""
from __future__ import annotations

import datetime as _dt
import logging
import re
from typing import Dict, List, Optional, Set

import numpy as np
import pandas as pd
from googleapiclient.discovery import build
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from treat.utils.write_back import write_back_df

log = logging.getLogger(__name__)

# ───────────────────────────── mapeamento origem → destino ──────────────────
DESTINATION_SHEETS: Dict[str, str] = {
    "geral":   "modeloGeral",
    "genero":  "modeloGenero",
    "idade":   "modeloIdade",
    "alcance": "modeloAlcance",
    "regiao":  "modeloRegiao",
}

# ───────────────────────────── caches globais em memória ────────────────────
_HEADERS: Dict[str, List[str]] = {}        # aba‑destino → header completo
_EXISTING_IDS: Dict[str, Set[str]] = {}    # aba‑destino → set(ID)

# ───────────────────────────── helpers de API & Excel col ───────────────────
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


def _idx_to_col(idx: int) -> str:
    """Converte índice 0‑based em letra(s) de coluna estilo A1 (A, B, … AA…)."""
    out = ""
    while True:
        idx, rem = divmod(idx, 26)
        out = chr(65 + rem) + out
        if idx == 0:
            return out
        idx -= 1  # Excel é *quase* base‑26 (A=1)


@retry(
    retry=retry_if_exception_type(Exception),
    stop=stop_after_attempt(6),
    wait=wait_exponential(multiplier=1, min=1, max=32),
    reraise=True,
)
def _build_service(creds_path: str):
    """Instancia Google Sheets API (somente leitura) com retry/back‑off."""
    import google.oauth2.service_account as sa

    creds = sa.Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


# ───────────────────────── pre‑fetch: header + coluna ID ─────────────────────

def prefetch_meta(creds_path: str, spreadsheet_id: str) -> None:
    """Carrega cabeçalhos e IDs das abas‑modelo (executar **uma** vez por run)."""
    global _HEADERS, _EXISTING_IDS
    if _HEADERS:  # já executado nesta sessão
        return

    service = _build_service(creds_path)

    # 1) header da linha 1
    hdr_ranges = [f"{tab}!1:1" for tab in DESTINATION_SHEETS.values()]
    resp_hdr = (
        service.spreadsheets()
        .values()
        .batchGet(spreadsheetId=spreadsheet_id, ranges=hdr_ranges)
        .execute()
    )

    id_ranges: List[str | None] = []
    for vr in resp_hdr["valueRanges"]:
        tab = vr["range"].split("!", 1)[0]
        header = [c.strip() for c in vr.get("values", [[]])[0] if c.strip()]
        _HEADERS[tab] = header

        try:
            idx_id = [h.lower() for h in header].index("id")
            col_a1 = _idx_to_col(idx_id)
            id_ranges.append(f"{tab}!{col_a1}2:{col_a1}")
        except ValueError:  # não há coluna ID
            _EXISTING_IDS[tab] = set()
            id_ranges.append(None)

    # 2) coluna ID (apenas onde existe)
    valid = [r for r in id_ranges if r]
    if valid:
        resp_id = (
            service.spreadsheets()
            .values()
            .batchGet(spreadsheetId=spreadsheet_id, ranges=valid)
            .execute()
        )
        it = iter(resp_id["valueRanges"])
        for tab, rng in zip(DESTINATION_SHEETS.values(), id_ranges):
            if rng is None:
                continue
            vr = next(it)
            ids = [v for sub in vr.get("values", []) for v in sub if v.strip()]
            _EXISTING_IDS[tab] = set(ids)

    log.info("📥 Prefetch destino concluído – headers=%d, IDs=%d",
             len(_HEADERS), sum(len(s) for s in _EXISTING_IDS.values()))


# ─────────────────────────────── util: scalarização ─────────────────────────

def _scalar(v):
    if isinstance(v, _dt.date):
        return v.isoformat()
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return ""
    return v


# ───────────────────────────── API de gravação pública ──────────────────────

def _infer_data_type(sheet_name: str) -> str:
    """Dado o nome da aba-origem, devolve o “tipo” de dados destino.

    *Ignora* abas de Google Analytics (começam por ``ga``).
    """
    lower = sheet_name.lower()
    if lower.startswith("ga"):            # ←  GA nunca vai para modelo*
        raise ValueError(
            f"Aba '{sheet_name}' é do Google Analytics – não grava em abas-modelo."
        )

    m = re.search(r"(geral|genero|idade|alcance|regiao)$", lower)
    if not m:
        raise ValueError(
            f"Não reconheço o tipo de dados da aba '{sheet_name}' "
            "(esperava terminar em geral/genero/idade/alcance/regiao)."
        )
    return m.group(1)


def write_back_destination(
    df_model: pd.DataFrame,
    data_type: str,
    creds_path: str,
    spreadsheet_id: str,
    *,
    write_back: bool = True,
    dry_run: bool = False,
    a1_range: str = "A1",
    value_input_option: str = "USER_ENTERED",
) -> Optional[pd.DataFrame]:
    """Grava `df_model` na aba‑modelo correspondente, deduplicando por ``ID``."""
    if df_model.empty:
        log.info("Destino '%s': DataFrame vazio – nada a gravar", data_type)
        return None

    sheet_name = DESTINATION_SHEETS[data_type]
    if sheet_name not in _HEADERS:
        raise RuntimeError("Caches destino não carregados – chame prefetch_meta() antes.")

    header = _HEADERS[sheet_name]
    df_out = (
        df_model
        .reindex(columns=header, fill_value="")
        .applymap(_scalar)
    )

    # deduplicação por ID, se presente
    if "ID" in df_out.columns:
        existing = _EXISTING_IDS.get(sheet_name, set())
        df_out = df_out.loc[~df_out["ID"].isin(existing)]

    if df_out.empty:
        log.info("Destino '%s': nenhuma linha nova para gravar", sheet_name)
        return None

    if dry_run or not write_back:
        log.info("[Dry‑run] %d linha(s) seriam gravadas em '%s'", len(df_out), sheet_name)
        return df_out

    # grava efetivamente
    write_back_df(
        df=df_out,
        creds_path=creds_path,
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        a1_range=a1_range,
        value_input_option=value_input_option,
    )
    log.info("✅ Gravadas %d linha(s) em '%s'", len(df_out), sheet_name)

    if "ID" in df_out.columns:
        _EXISTING_IDS.setdefault(sheet_name, set()).update(df_out["ID"].tolist())

    return df_out


def write_back_for_sheet(
    df_model: pd.DataFrame,
    sheet_name: str,
    creds_path: str,
    spreadsheet_id: str,
    *,
    write_back: bool = True,
    dry_run: bool = False,
) -> Optional[pd.DataFrame]:
    """Wrapper: infere `data_type` a partir do nome da aba de origem."""
    return write_back_destination(
        df_model       = df_model,
        data_type      = _infer_data_type(sheet_name),
        creds_path     = creds_path,
        spreadsheet_id = spreadsheet_id,
        write_back     = write_back,
        dry_run        = dry_run,
    )
