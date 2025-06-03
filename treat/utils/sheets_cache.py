#sheets_cache
"""Cache global para Google Sheets & Worksheets.

Evita estourar a quota de leitura criando objetos gspread apenas uma vez
por (credenciais, spreadsheet, aba).  Importe `get_worksheet()` de onde
precisar – não há dependência circular.
"""
from __future__ import annotations

from typing import Dict, Tuple
import gspread

from treat.utils.get_google_client import get_google_client

# chaves: (creds_path, spreadsheet_id)
_SHEET_CACHE: Dict[Tuple[str, str], gspread.Spreadsheet] = {}
# chaves: (creds_path, spreadsheet_id, sheet_name)
_WS_CACHE: Dict[Tuple[str, str, str], gspread.Worksheet] = {}


def get_worksheet(
    creds_path: str,
    spreadsheet_id: str,
    sheet_name: str,
) -> gspread.Worksheet:
    """
    Retorna `gspread.Worksheet` cacheado.

    A primeira chamada cria o objeto; as seguintes retornam o mesmo.
    """
    key_ws = (creds_path, spreadsheet_id, sheet_name)
    if key_ws not in _WS_CACHE:
        key_sheet = (creds_path, spreadsheet_id)
        if key_sheet not in _SHEET_CACHE:
            _SHEET_CACHE[key_sheet] = (
                get_google_client(creds_path).open_by_key(spreadsheet_id)
            )
        _WS_CACHE[key_ws] = _SHEET_CACHE[key_sheet].worksheet(sheet_name)
    return _WS_CACHE[key_ws]
