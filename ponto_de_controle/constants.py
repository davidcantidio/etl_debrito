from __future__ import annotations

import os
from collections import OrderedDict
from datetime import date
from pathlib import Path

# ── Sheets / abas ────────────────────────────────────────────────────────────
ORIGIN_SHEET_ID: str = os.getenv("ORIGIN_SHEET_ID", "")
ORIGIN_TAB: str = os.getenv("ORIGIN_TAB", "modeloGeral")

DEST_SHEET_ID: str = os.getenv("DEST_SHEET_ID", "")
DEST_TAB: str = os.getenv("DEST_TAB", "IMPULSIONAMENTOS 2025")
HEAD_ROW_DEST: int = int(os.getenv("HEAD_ROW_DEST", "4"))  # zero-based

# ── Caminho para o JSON de credenciais Google ───────────────────────────────
GOOGLE_CREDS_PATH: Path = Path(os.getenv("GOOGLE_CREDS_PATH", "creds.json"))

# ── Filtro temporal mínimo ──────────────────────────────────────────────────
MIN_DATE: date = date(2025, 6, 1)

# ── Colunas padrão de destino ───────────────────────────────────────────────
from treat.utils.campos_calculados import DEFAULT_DEST_COLUMNS  # noqa: E402  (import tardio)

DEST_COLUMNS: list[str] = list(OrderedDict.fromkeys(DEFAULT_DEST_COLUMNS))
assert len(DEST_COLUMNS) == 11, "DEST_COLUMNS deve conter 11 rótulos únicos"
