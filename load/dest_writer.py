# load/dest_writer.py

"""
Write-back otimizado para as abas **modelo***

Principais características
-------------------------
* 01 leitura para cabeçalhos e IDs: usamos SheetsFetcher para recuperar todas
  as abas-modelo de uma só vez (batchGet), poupando quota.
* Zero leituras extras durante o loop de ETL; somente gravação (via write_back_df ou append).
* Deduplicação: grava apenas registros cujo "ID" ainda não existe.
* Compatível com dry-run e com execução em paralelo.
"""

import datetime as _dt
import json
import logging
import re
from typing import Dict, Iterable, List, Optional, Set

import numpy as np
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from extract.sheets_fetcher import SheetsFetcher
from treat.utils.sheets_cache import get_worksheet as _get_worksheet
from treat.utils.validations import validate_columns
from load.utils.column_mapper import apply_smart_column_mapping

log = logging.getLogger(__name__)

get_worksheet = _get_worksheet  # enable monkeypatch in tests

# ───────────────────────────── mapeamento origem → destino ──────────────────
DESTINATION_SHEETS: Dict[str, str] = {
    "geral": "modeloGeral",
    "genero": "modeloGenero",
    "idade": "modeloIdade",
    "alcance": "modeloAlcance",
    "regiao": "modeloRegiao",
}

# ───────────────────────────── caches globais em memória ────────────────────
_HEADERS: Dict[str, List[str]] = {}  # aba-destino → header completo
_EXISTING_IDS: Dict[str, Set[str]] = {}  # aba-destino → set(ID)


def _idx_to_col(idx: int) -> str:
    """Converte índice 0-based em letras de coluna estilo A1 (A, B, …, AA…)."""
    out = ""
    while True:
        idx, rem = divmod(idx, 26)
        out = chr(65 + rem) + out
        if idx == 0:
            return out
        idx -= 1


def prefetch_meta(fetcher: SheetsFetcher, spreadsheet_id: str) -> None:
    """
    Carrega cabeçalhos (linha 1) e IDs (coluna 'ID' a partir da linha 2)
    de todas as abas-modelo (modeloGeral, modeloGenero, etc.) em UMA ÚNICA
    operação, poupando leitura repetida da API.

    Depois disso, os caches globais _HEADERS e _EXISTING_IDS estão prontos
    para uso em write_back_destination, sem novas leituras.
    """
    if _HEADERS:
        return

    abas = list(DESTINATION_SHEETS.values())

    # 1) Buscar todas as linhas de cada aba no fetcher (cada aba → Lista[List[str]])
    try:
        raw_data = fetcher.get_cached(abas, as_frame=False)
    except Exception:
        raw_data = fetcher.get(abas, as_frame=False)

    # 2) Extrair cabeçalhos (linha 0 de cada aba) e armazenar em _HEADERS
    for tab, values in raw_data.items():
        primeira_linha = values[0] if values else []
        header = [c.strip() for c in primeira_linha if c.strip()]
        _HEADERS[tab] = header

    # 3) A partir dos dados lidos, preencher _EXISTING_IDS somente com a coluna ID
    for tab, values in raw_data.items():
        header = _HEADERS.get(tab, [])
        try:
            idx_id = [h.lower() for h in header].index("id")
        except ValueError:
            _EXISTING_IDS[tab] = set()
            continue

        ids: List[str] = []
        for row in values[1:]:
            if len(row) > idx_id:
                val = str(row[idx_id]).strip()
                if val:
                    ids.append(val)
        _EXISTING_IDS[tab] = set(ids)

    log.info(
        "📥 Prefetch destino concluído: %d abas com cabeçalho, total de IDs carregados=%d",
        len(_HEADERS),
        sum(len(s) for s in _EXISTING_IDS.values()),
    )


def _scalar(v):
    if isinstance(v, _dt.date):
        return v.isoformat()
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return ""
    return v


def _infer_data_type(sheet_name: str) -> str:
    """
    Dado nome de aba de origem (exemplo: 'metaGenero', 'tiktokIdade'), devolve
    o sufixo que mapeia para a aba-modelo correspondente. Ex.: 'genero', 'idade'.
    """
    lower = sheet_name.strip().lower()
    if lower.startswith("ga"):
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


def _build_service(creds_path: str):
    from treat.utils.google_service_pool import get_sheets_service
    return get_sheets_service(creds_path, readonly=False)


def _ensure_rows(
    creds_path: str, spreadsheet_id: str, sheet_name: str, end_row: int
) -> None:
    """Expande a planilha se `end_row` exceder o tamanho atual."""
    import sys

    pipeline = sys.modules.get("treat.treat_pipeline")
    ws_getter = getattr(pipeline, "get_worksheet", get_worksheet)
    try:
        ws = ws_getter(creds_path, spreadsheet_id, sheet_name)
    except Exception:
        return

    frozen = (
        getattr(ws, "_properties", {})
        .get("gridProperties", {})
        .get("frozenRowCount", 0)
    )
    min_rows = max(frozen + 1, 2)
    desired = max(end_row, min_rows)

    try:
        current = int(getattr(ws, "row_count", 0))
    except Exception:
        current = desired

    if current < desired:
        try:
            ws.resize(rows=desired)
        except Exception:
            pass


def _to_payload(df: pd.DataFrame, sheet_name: str) -> dict:
    existing = _EXISTING_IDS.get(sheet_name, set())
    start_row = len(existing) + 2  # 1 = header
    values = df.values.tolist()
    return {
        "range": f"{sheet_name}!A{start_row}",
        "majorDimension": "ROWS",
        "values": values,
    }


def _prepare_df(df_model: pd.DataFrame, sheet_name: str) -> Optional[pd.DataFrame]:
    header = _HEADERS[sheet_name]
    
    # Apply smart column mapping for destination
    df_mapped = apply_smart_column_mapping(
        df_model, df_model, sheet_name, for_destination=True
    )
    
    # Reindex with header but only include available columns to reduce warnings
    available_header_cols = [col for col in header if col in df_mapped.columns]
    missing_header_cols = set(header) - set(available_header_cols)
    
    if missing_header_cols and len(missing_header_cols) <= 3:
        log.debug(f"Destino {sheet_name}: colunas header ausentes: {sorted(missing_header_cols)}")
    
    df_out = df_mapped.reindex(columns=available_header_cols, fill_value="").map(_scalar)
    validate_columns(df_out, available_header_cols, stage=f"Destino {sheet_name}")
    
    if "ID" in df_out.columns:
        existing = _EXISTING_IDS.get(sheet_name, set())
        df_out = df_out.loc[~df_out["ID"].isin(existing)]
    if df_out.empty:
        return None
    return df_out


def collect_dest_payload(df_model: pd.DataFrame, sheet_name: str) -> Optional[dict]:
    """Prepara payload para batchUpdate sem enviá-lo."""
    if sheet_name not in _HEADERS:
        raise RuntimeError(
            "Caches destino não carregados – chame prefetch_meta() antes."
        )
    df_out = _prepare_df(df_model, sheet_name)
    if df_out is None:
        log.info("Destino '%s': nenhuma linha nova para gravar", sheet_name)
        return None
    payload = _to_payload(df_out, sheet_name)
    if "ID" in df_out.columns:
        _EXISTING_IDS.setdefault(sheet_name, set()).update(
            df_out["ID"].astype(str).tolist()
        )
    return payload


def flush_payloads(
    creds_path: str,
    spreadsheet_id: str,
    payloads: Iterable[dict],
    *,
    write_back: bool = True,
    dry_run: bool = False,
    value_input_option: str = "USER_ENTERED",
    service: Optional = None,
) -> None:
    payload_list = [p for p in payloads if p]
    if not payload_list:
        return

    total_cells = sum(len(p["values"]) * len(p["values"][0]) for p in payload_list)

    if dry_run or not write_back:
        log.info(
            "[Dry-run] batchUpdate enviaria %d ranges, total %s células",
            len(payload_list),
            f"{total_cells:,}",
        )
        return

    if service is None:
        for p in payload_list:
            sheet, start = p["range"].split("!")
            m = re.search(r"\d+", start)
            start_row = int(m.group()) if m else 1
            end_row = start_row + len(p["values"]) - 1
            _ensure_rows(creds_path, spreadsheet_id, sheet, end_row)
        service = _build_service(creds_path)
    else:
        service = service

    MAX_CELLS = 500_000
    MAX_BYTES = int(9.9 * 1024 * 1024)

    batches: List[List[dict]] = []
    current: List[dict] = []
    cells = 0
    size = 0

    for payload in payload_list:
        payload_size = len(json.dumps(payload).encode("utf-8"))
        payload_cells = len(payload["values"]) * len(payload["values"][0])
        if current and (
            cells + payload_cells > MAX_CELLS or size + payload_size > MAX_BYTES
        ):
            batches.append(current)
            current = []
            cells = 0
            size = 0

        current.append(payload)
        cells += payload_cells
        size += payload_size

    if current:
        batches.append(current)

    for batch in batches:
        body = {"valueInputOption": value_input_option, "data": batch}
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id, body=body
        ).execute()

    log.info(
        "batchUpdate enviou %d ranges, total %s células",
        len(payload_list),
        f"{total_cells:,}",
    )


def write_back_batch(
    frames: Dict[str, pd.DataFrame],
    creds_path: str,
    spreadsheet_id: str,
    *,
    write_back: bool = True,
    dry_run: bool = False,
    value_input_option: str = "USER_ENTERED",
    service: Optional = None,
) -> Dict[str, Optional[pd.DataFrame]]:
    """Grava várias abas-modelo em lote via batchUpdate."""

    results: Dict[str, Optional[pd.DataFrame]] = {}

    payloads: List[dict] = []

    for data_type, df_model in frames.items():
        if df_model.empty:
            log.info("Destino '%s': DataFrame vazio – nada a gravar", data_type)
            results[data_type] = None
            continue

        sheet_name = DESTINATION_SHEETS[data_type]
        if sheet_name not in _HEADERS:
            raise RuntimeError(
                "Caches destino não carregados – chame prefetch_meta() antes."
            )

        df_out = _prepare_df(df_model, sheet_name)
        if df_out is None:
            log.info("Destino '%s': nenhuma linha nova para gravar", data_type)
            results[data_type] = None
            continue

        payload = collect_dest_payload(df_model, sheet_name)
        if payload is not None:
            payloads.append(payload)
            results[data_type] = df_out
        else:
            results[data_type] = None

    if payloads:
        flush_payloads(
            creds_path=creds_path,
            spreadsheet_id=spreadsheet_id,
            payloads=payloads,
            write_back=write_back,
            dry_run=dry_run,
            value_input_option=value_input_option,
            service=service,
        )

    return results


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
    """Wrapper para gravar apenas uma aba-modelo."""

    result = write_back_batch(
        {data_type: df_model},
        creds_path=creds_path,
        spreadsheet_id=spreadsheet_id,
        write_back=write_back,
        dry_run=dry_run,
        value_input_option=value_input_option,
    )
    return result.get(data_type)


def prepare_dest_payload(
    df_model: pd.DataFrame, 
    sheet_name: str, 
    creds_path: str = None, 
    spreadsheet_id: str = None, 
    dry_run: bool = False
) -> Optional[dict]:
    """Prepara payload para batchUpdate sem enviá-lo."""
    data_type = _infer_data_type(sheet_name)
    sheet_name_dest = DESTINATION_SHEETS[data_type]
    return collect_dest_payload(df_model, sheet_name_dest)


def write_back_for_sheet(
    df_model: pd.DataFrame,
    sheet_name: str,
    creds_path: str,
    spreadsheet_id: str,
    *,
    write_back: bool = True,
    dry_run: bool = False,
) -> Optional[pd.DataFrame]:
    """
    Wrapper: infere `data_type` a partir do nome da aba de origem
    e chama write_back_destination.
    """
    data_type = _infer_data_type(sheet_name)
    return write_back_destination(
        df_model=df_model,
        data_type=data_type,
        creds_path=creds_path,
        spreadsheet_id=spreadsheet_id,
        write_back=write_back,
        dry_run=dry_run,
    )
