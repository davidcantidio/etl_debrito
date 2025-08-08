"""
Constantes específicas do módulo ponto_de_controle.

Este arquivo importa as configurações centrais de config.py
e adiciona constantes específicas deste módulo.
"""

from collections import OrderedDict

# Importa configurações centrais
from config import (
    DEST_SHEET_ID,
    DEST_TAB,
    GOOGLE_CREDS_PATH,
    HEAD_ROW_DEST,
    MIN_DATE,
    ORIGIN_SHEET_ID,
    ORIGIN_TAB,
)

# Importa colunas padrão
from treat.utils.campos_calculados import DEFAULT_DEST_COLUMNS

# Garante unicidade e ordem das colunas
DEST_COLUMNS: list[str] = list(OrderedDict.fromkeys(DEFAULT_DEST_COLUMNS))
assert len(DEST_COLUMNS) == 11, "DEST_COLUMNS deve conter 11 rótulos únicos"