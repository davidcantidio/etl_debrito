import pytest
import logging
from utils.get_google_client import get_google_client
from utils.google_sheets import CREDS_PATH, SPREADSHEET_ID

# Abas de origem que queremos validar
ORIGIN_SHEETS = [
    "metaGeral",
    "linkedinGeral",
    "tiktokGeral",
    "pinterestGeral",
]

# Colunas obrigatórias
MANDATORY_COLUMNS = ["Campaign name", "Start", "End"]

@pytest.mark.integration
@pytest.mark.sheet_structure
def test_origin_sheets_have_mandatory_columns():
    """
    Garante que todas as abas de origem contêm as colunas obrigatórias.
    """
    log = logging.getLogger(__name__)
    client = get_google_client(CREDS_PATH)
    sh = client.open_by_key(SPREADSHEET_ID)

    missing = []

    for sheet_name in ORIGIN_SHEETS:
        log.info(f"Verificando estrutura da aba: {sheet_name}")
        ws = sh.worksheet(sheet_name)
        header = [h.strip() for h in ws.row_values(1)]

        for col in MANDATORY_COLUMNS:
            if col not in header:
                missing.append((sheet_name, col))

    if missing:
        error_msg = "\n".join([f"Aba '{sheet}' faltando coluna '{col}'" for sheet, col in missing])
        pytest.fail(f"Colunas obrigatórias ausentes:\n{error_msg}")

    log.info("Todas as abas possuem as colunas obrigatórias!")
