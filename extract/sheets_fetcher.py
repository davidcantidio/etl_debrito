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

from treat.utils.get_google_client import get_google_client  # retorna um cliente gspread

log = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


def _build_sheets_service(creds_path: str):
    """Constrói o serviço da Google Sheets API via google-api-python-client."""
    import google.oauth2.service_account as sa

    creds = sa.Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


class SheetsFetcher:
    """
    Lê múltiplas abas de um Google Sheets num único batchGet, com cache TTL,
    e expõe método open_worksheet() para operações de write-back via gspread.
    """

    def __init__(
        self,
        spreadsheet_id: str,
        creds_path: str,
        header_row: int = 0,
        col_range: str = "A:ZZ",
        cache_ttl: int = 300,
         
    ):
        """
        Inicializa o fetcher.

        Parâmetros:
        - spreadsheet_id: ID da planilha Google.
        - creds_path: caminho para o JSON de credenciais de serviço.
        - header_row: índice da linha de cabeçalho ao ler como DataFrame (padrão: 0).
        - col_range: faixa de colunas a ser usada em cada leitura (padrão: "A:ZZ").
        - cache_ttl: tempo (em segundos) de validade do cache em memória (padrão: 300).
        """
        self.spreadsheet_id = spreadsheet_id
        self.creds_path = creds_path
        self.header_row = header_row
        self.col_range = col_range
        self._service = _build_sheets_service(creds_path)
        self._cache: Dict[Tuple[str, ...], Tuple[float, Dict[str, List[List[str]]]]] = {}
        self._cache_ttl = cache_ttl
        self._spreadsheet_meta: dict | None = None

        # Cache de headers por aba (preenchido em _fetch_and_cache)
        self._headers: Dict[str, List[str]] = {}

        # Cliente gspread para operações de open_worksheet (write-back, etc.)
        self._gclient = get_google_client(creds_path)

    @retry(
        retry=retry_if_exception_type(TooManyRequests),
        stop=stop_after_attempt(6),                  # 1 + 2 + 4 + 8 + 16 + 32 ≈ 63 s
        wait=wait_exponential(multiplier=1, min=1, max=32),
        reraise=True,
    )


    def _get_meta(self):
        if self._spreadsheet_meta is None:
            self._spreadsheet_meta = (
                self._service.spreadsheets().get(
                    spreadsheetId=self.spreadsheet_id,
                    includeGridData=False,
                ).execute()
            )
        return self._spreadsheet_meta
    

    def _batch_get(self, ranges: List[str]) -> Dict[str, Any]:
        """Executa batchGet com retry/back-off para código 429."""
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

    def _fetch_and_cache(self, sheet_names: List[str]) -> Dict[str, List[List[str]]]:
        """
        Constrói ranges a partir dos nomes de abas e realiza batchGet,
        retornando o payload em raw lists e atualizando o cache interno.
        """
        cleaned = [n.strip().strip("'") for n in sheet_names]
        ranges = [f"{n}!{self.col_range}" for n in cleaned]
        log.info("🧹 ranges normalizados: %s", ranges)

        resp = self._batch_get(ranges)
        if not resp.get("valueRanges"):
            raise RuntimeError("Sheets API retornou valueRanges vazio – quota ou planilha vazia?")

        payload: Dict[str, List[List[str]]] = {}
        for name, vr in zip(cleaned, resp["valueRanges"]):
            log.info("🔍 range completo retornado pela API: %s", vr.get("range"))
            original_key = name.strip()
            returned_key = vr["range"].split("!", 1)[0].strip().strip("'")
            if returned_key != original_key:
                log.warning(
                    "Nome retornado pela API difere do solicitado: '%s' → '%s'",
                    original_key,
                    vr["range"].split("!", 1)[0].strip(),
                )
            values = vr.get("values", [])
            payload[original_key] = values
            if values:
                self._headers[original_key] = [c.strip() for c in values[0]]
        return payload

    def get(self, sheet_names: Iterable[str], *, as_frame: bool = True) -> Dict[str, Any]:
        """
        Lê em lote as abas listadas em sheet_names.
        Retorna dicionário {nome_aba: DataFrame|raw lists} conforme as_frame.
        Utiliza cache TTL para evitar leituras desnecessárias.
        """
        names = [n.strip().strip("'") for n in dict.fromkeys(sheet_names)]
        key = tuple(sorted(names))
        now = time.time()

        if key in self._cache and now - self._cache[key][0] < self._cache_ttl:
            raw = self._cache[key][1]
            log.info("📥 Cache hit para %s", key)
        else:
            raw = self._fetch_and_cache(names)
            self._cache[key] = (now, raw)
            log.info("📡 batchGet %d ranges", len(names))

        for tab, values in raw.items():
            if tab not in self._headers and values:
                self._headers[tab] = [c.strip() for c in values[0]]

        if not as_frame:
            return {n: raw.get(n, []) for n in names}

        return {n: self._as_dataframe(raw.get(n, [])) for n in names}

    def get_cached(self, sheet_names: Iterable[str], *, as_frame: bool = True) -> Dict[str, Any]:
        """Retorna dados do cache sem acionar a API."""
        names = [n.strip().strip("'") for n in dict.fromkeys(sheet_names)]
        key = tuple(sorted(names))
        if key not in self._cache:
            raise KeyError(f"Cache miss para {names}")
        raw = self._cache[key][1]
        if not as_frame:
            return {n: raw.get(n, []) for n in names}
        return {n: self._as_dataframe(raw.get(n, [])) for n in names}

    def refresh(self, sheet_names: Iterable[str]):
        """
        Invalida o cache para as abas listadas e força recarregamento.
        """
        sanitized = [n.strip().strip("'") for n in sheet_names]
        key = tuple(sorted(sanitized))
        self._cache.pop(key, None)
        _ = self.get(sheet_names, as_frame=False)  # força reload

    @property
    def header_cache(self) -> Dict[str, List[str]]:
        """Retorna cabeçalhos já carregados em memória."""
        return dict(self._headers)

    @staticmethod
    def _as_dataframe(raw: List[List[str]]) -> pd.DataFrame:
        """
        Converte raw lists (header + body) em pandas.DataFrame,
        preenchendo linhas/colunas faltantes com strings vazias.
        """
        if not raw:
            return pd.DataFrame()
        header, *body = raw
        max_cols = len(header)
        normalized = [
            row + [""] * (max_cols - len(row)) if len(row) < max_cols else row[:max_cols]
            for row in body
        ]
        return pd.DataFrame(normalized, columns=[c.strip() for c in header])

    def open_worksheet(self, sheet_name: str):
        """
        Retorna objeto gspread.Worksheet para a aba informada,
        usando o cliente gspread inicializado em __init__.
        """
        cleaned = sheet_name.strip().strip("'")
        return self._gclient.open_by_key(self.spreadsheet_id).worksheet(cleaned)
