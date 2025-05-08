from __future__ import annotations
import os, itertools, logging
import pandas as pd
from typing import Iterable, Dict, List, Any

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

def build_sheets_service(creds_path: str):
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)
from googleapiclient.errors import HttpError

log = logging.getLogger(__name__)

class SheetsFetcher:
    """
    Extrai múltiplas abas via batchGet e mantém cache de RAW LISTS.
    get(..., as_frame) converte ao retornar, sem mudar o cache.
    """

    def __init__(
        self,
        spreadsheet_id: str | None = None,
        creds_path: str | None = None,
        header_row: int = 0,
        col_range: str = "A:ZZ",
        service=None,
    ):
        self.spreadsheet_id = spreadsheet_id or os.getenv("SPREADSHEET_ID")
        self.creds_path     = creds_path or os.getenv("GOOGLE_CREDS_PATH", "creds.json")
        self.header_row     = header_row
        self.col_range      = col_range
        # Se já foi passado um serviço, reutiliza; se não, constrói via helper
        self._service       = service or build_sheets_service(self.creds_path)
        # cache armazena RAW: List[List[str]]
        self._cache: Dict[str, List[List[str]]] = {}

    def get(self, sheet_names: Iterable[str], *, as_frame: bool = True) -> Dict[str, Any]:
        names = list(dict.fromkeys(sheet_names))
        missing = [n for n in names if n not in self._cache]
        if missing:
            self._fetch_batch(missing)

        out_raw = {n: self._cache[n] for n in names}
        if not as_frame:
            return out_raw

        return {n: self._as_dataframe(raw) for n, raw in out_raw.items()}

    def refresh(self, sheet_names: Iterable[str]):
        for n in sheet_names:
            self._cache.pop(n, None)
        self._fetch_batch(list(sheet_names))

    def _fetch_batch(self, names: List[str]):
        for batch in self._chunk(names, 100):
            log.info("📡 batchGet %d ranges", len(batch))
            ranges = [f"{n}!{self.col_range}" for n in batch]
            try:
                resp = (
                    self._service.spreadsheets()
                                 .values()
                                 .batchGet(
                                     spreadsheetId=self.spreadsheet_id,
                                     ranges=ranges,
                                     majorDimension="ROWS",
                                 )
                                 .execute()
                )
            except HttpError as e:
                log.error("Sheets API error: %s", e)
                for n in batch:
                    self._cache[n] = []
                continue

            seen = set()
            for vr in resp.get("valueRanges", []):
                name   = vr["range"].split("!")[0]
                values = vr.get("values", []) or []
                seen.add(name)

                if not values:
                    self._cache[name] = []
                else:
                    hdr_idx = self._detect_header_row(values)
                    header  = values[hdr_idx]
                    body    = list(self._normalize_rows(header, values[hdr_idx + 1:]))
                    self._cache[name] = [header] + body

            for n in batch:
                if n not in seen:
                    self._cache[n] = []

    @staticmethod
    def _chunk(items: List[str], size: int):
        for i in range(0, len(items), size):
            yield items[i : i + size]

    @staticmethod
    def _normalize_rows(header: List[str], rows: List[List[str]]):
        max_cols = len(header)
        for r in rows:
            if len(r) < max_cols:
                yield r + [""] * (max_cols - len(r))
            else:
                yield r[:max_cols]

    def _detect_header_row(self, values: List[List[str]]) -> int:
        P = 0.5
        for idx, row in enumerate(values):
            non_empty = sum(1 for cell in row if cell not in (None, "", "0"))
            if non_empty >= len(row) * P:
                return idx
        return self.header_row

    def get_column(
        self,
        sheet_name: str,
        column: str = "A",
        *,
        header_present: bool = True,
        as_series: bool = True,
    ):
        col_range = f"{sheet_name}!{column}:{column}"
        try:
            resp = (
                self._service.spreadsheets()
                             .values()
                             .get(
                                 spreadsheetId=self.spreadsheet_id,
                                 range=col_range,
                                 majorDimension="COLUMNS",
                             )
                             .execute()
            )
        except HttpError as e:
            log.error("Sheets API error (get_column %s): %s", sheet_name, e)
            return pd.Series(dtype="object") if as_series else []

        values = resp.get("values", [[]])
        col = values[0] if values else []

        if header_present and col:
            header = col[0]
            col = col[1:]
        else:
            header = column

        while col and col[-1] == "":
            col.pop()

        if as_series:
            return pd.Series(col, name=header)
        return col

    @staticmethod
    def _as_dataframe(raw: List[List[str]]) -> pd.DataFrame:
        if not raw:
            return pd.DataFrame()
        header, *body = raw
        max_cols = len(header)
        body = [r + [""] * (max_cols - len(r)) for r in body]
        return pd.DataFrame(body, columns=header)
