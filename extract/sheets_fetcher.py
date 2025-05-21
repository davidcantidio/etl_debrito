# File: extract/sheets_fetcher.py
import os
import time
import logging
from typing import Any, Dict, Iterable, List, Tuple

import pandas as pd
from google.api_core.exceptions import TooManyRequests
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

log = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


def _build_sheets_service(creds_path: str):
    """Constroi Google Sheets API service via google‑api‑python‑client."""
    import google.oauth2.service_account as sa

    creds = sa.Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


class SheetsFetcher:
    """Lê múltiplas abas de um Google Sheets num único *batchGet*, com cache TTL.

    * **Leitura em lote** → economiza quota.
    * **Retry/back‑off** → aguenta erros 429.
    * **Cache em memória** → evita leituras repetidas em até `cache_ttl` segundos.
    """

    def __init__(
        self,
        spreadsheet_id: str,
        creds_path: str,
        header_row: int = 0,
        col_range: str = "A:ZZ",
        cache_ttl: int = 300,
    ):
        self.spreadsheet_id = spreadsheet_id
        self.creds_path = creds_path
        self.header_row = header_row
        self.col_range = col_range
        self._service = _build_sheets_service(creds_path)
        self._cache: Dict[Tuple[str, ...], Tuple[float, Dict[str, List[List[str]]]]] = {}
        self._cache_ttl = cache_ttl

    # ───────────────────────────────── retry / backoff ─────────────────────────
    @retry(
        retry=retry_if_exception_type(TooManyRequests),
        stop=stop_after_attempt(6),                  # 1 + 2 + 4 + 8 + 16 + 32 ≈ 63 s
        wait=wait_exponential(multiplier=1, min=1, max=32),
        reraise=True,
    )
    def _batch_get(self, ranges: List[str]) -> Dict[str, Any]:
        """Faz uma chamada batchGet com back‑off para 429."""
        # log da tentativa de batchGet
        log.info("🔄 batchGet tentativa para ranges: %s", ranges)
        try:
            return (
                self._service.spreadsheets()
                .values()
                .batchGet(spreadsheetId=self.spreadsheet_id, ranges=ranges)
                .execute()
            )
        except HttpError as e:
            log.error("Sheets API HttpError: %s", e)
            raise

    # ───────────────────────────────── cache helper ────────────────────────────
    def _fetch_and_cache(self, sheet_names: List[str]) -> Dict[str, List[List[str]]]:
        # Prepara ranges a partir dos nomes solicitados (usa col_range)
        ranges = [f"{name}!{self.col_range}" for name in sheet_names]
        resp = self._batch_get(ranges)
        if not resp.get("valueRanges"):
            raise RuntimeError("Sheets API retornou valueRanges vazio – quota ou planilha vazia?")

        payload: Dict[str, List[List[str]]] = {}
        for name, vr in zip(sheet_names, resp["valueRanges"]):
            # log do range completo retornado para debug de título
            log.info("🔍 range completo retornado pela API: %s", vr.get("range"))
            original_key = name.strip()
            returned_key = vr["range"].split("!", 1)[0].strip()
            if returned_key != original_key:
                log.warning("Nome retornado pela API difere do solicitado: '%s' → '%s'", original_key, returned_key)
            payload[original_key] = vr.get("values", [])
        return payload

    # ───────────────────────────────── API pública ─────────────────────────────
    def get(self, sheet_names: Iterable[str], *, as_frame: bool = True) -> Dict[str, Any]:
        names = [n.strip() for n in dict.fromkeys(sheet_names)]
        key = tuple(sorted(names))
        now = time.time()

        # — cache —
        if key in self._cache and now - self._cache[key][0] < self._cache_ttl:
            raw = self._cache[key][1]
            log.info("📥 Cache hit para %s", key)
        else:
            raw = self._fetch_and_cache(names)
            self._cache[key] = (now, raw)
            log.info("📡 batchGet %d ranges", len(names))

        # Retorna raw lists ou DataFrames com as chaves originais (sem lowercasing)
        if not as_frame:
            return {n: raw.get(n, []) for n in names}

        return {n: self._as_dataframe(raw.get(n, [])) for n in names}

    def refresh(self, sheet_names: Iterable[str]):
        key = tuple(sorted(sheet_names))
        self._cache.pop(key, None)
        _ = self.get(sheet_names, as_frame=False)  # força reload

    # ───────────────────────────────── utilidades ──────────────────────────────
    @staticmethod
    def _as_dataframe(raw: List[List[str]]) -> pd.DataFrame:
        if not raw:
            return pd.DataFrame()
        header, *body = raw
        max_cols = len(header)
        normalized = [row + [""] * (max_cols - len(row)) if len(row) < max_cols else row[:max_cols] for row in body]
        return pd.DataFrame(normalized, columns=[c.strip() for c in header])
