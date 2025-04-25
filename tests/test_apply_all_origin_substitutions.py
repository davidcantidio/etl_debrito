"""
Testes de integração (NÃO unitários) para verificar se
`apply_all_origin_substitutions` faz as trocas esperadas **na origem**.

⚠️  Estes testes acessam a planilha Google real — rode-os apenas quando
    tiver certeza de que as credenciais e o ID da planilha estão corretos.

Execute com:
    pytest -m "integration and origin_subst"
"""

import os
import logging
import pytest
import pandas as pd

from utils.google_sheets import (
    carregar_aba_google_sheets,
    CREDS_PATH,
    SPREADSHEET_URL,
)
from utils.substitute_origin_values import apply_all_origin_substitutions
from utils.substitutions_lists import (
    ID_CONTENT_REPLACEMENTS,
    CAMPAIGN_NAME_REPLACEMENTS,
    AD_GROUP_NAME_REPLACEMENTS,
)

# ---------------------------------------------------------------------
# Configuração / helpers
# ---------------------------------------------------------------------
ORIGIN_SHEETS = {
    "meta":      "metaGeral",
    "linkedin":  "linkedinGeral",
    "tiktok":    "tiktokGeral",
    "pinterest": "pinterestGeral",
}

MAPPINGS = {
    "Content (utm)" : ID_CONTENT_REPLACEMENTS,
    "Campaign name" : CAMPAIGN_NAME_REPLACEMENTS,
    "Ad group name" : AD_GROUP_NAME_REPLACEMENTS,
}

logger = logging.getLogger(__name__)


def _load_sheet(sheet_name: str) -> pd.DataFrame:
    """Leitura sem transformar nada (tudo string)."""
    df = carregar_aba_google_sheets(CREDS_PATH, SPREADSHEET_URL, sheet_name)
    # Não queremos linhas totalmente vazias
    return df.dropna(how="all")


# ---------------------------------------------------------------------
# Markers
# ---------------------------------------------------------------------
def pytest_configure(config):  # registra marker custom
    config.addinivalue_line(
        "markers",
        "integration: testes que requerem acesso a recursos externos"
    )
    config.addinivalue_line(
        "markers",
        "origin_subst: cobre substitutions em abas de origem"
    )


skip_reason = (
    "Credenciais Google não encontradas/CONFIG incorreta — "
    "defina variáveis CREDS_PATH e SPREADSHEET_URL para rodar os testes de integração."
)

if not (os.path.isfile(CREDS_PATH) and SPREADSHEET_URL.startswith("https://")):
    pytest.skip(skip_reason, allow_module_level=True)

# ---------------------------------------------------------------------
# Testes parametrizados
# ---------------------------------------------------------------------
@pytest.mark.integration
@pytest.mark.origin_subst
@pytest.mark.parametrize("plataforma, aba_origem", ORIGIN_SHEETS.items())
def test_substitutions_are_applied_in_origin(plataforma, aba_origem):
    """
    1. Carrega a aba de origem.
    2. Aplica `apply_all_origin_substitutions`.
    3. Valida que **pelo menos uma** substituição de cada mapping acontece,
       se o valor original existe na aba.
    """
    df_raw = _load_sheet(aba_origem)
    df_processed = apply_all_origin_substitutions(
        df_raw,
        write_back=False,          # não grava no Sheets
        inplace=True               # mantém sem cópia extra (pode ser omitido)
    )

    for coluna, mapping in MAPPINGS.items():
        if coluna not in df_raw.columns:
            # A aba dessa plataforma não contém tal coluna; segue
            continue

        # normaliza antes de comparar
        before_norm = df_raw[coluna].astype(str).str.strip().str.lower()
        after_norm  = df_processed[coluna].astype(str).str.strip().str.lower()

        # registros cujo valor existia nas chaves de mapping
        mask_keys = before_norm.isin(mapping.keys())
        total_keys_present = int(mask_keys.sum())

        if total_keys_present == 0:
            # Aquele valor não aparece nessa plataforma – é ok, apenas loga
            logger.info(
                "[%s] nenhuma chave de %s encontrada em '%s'.",
                plataforma, coluna, aba_origem
            )
            continue

        # Esperamos 100 % de substituição quando o valor original existe
        replaced_mask = (before_norm != after_norm) & mask_keys
        num_replaced  = int(replaced_mask.sum())

        assert num_replaced == total_keys_present, (
            f"[{plataforma}] Substituições parciais em '{coluna}'. "
            f"Esperado={total_keys_present}, obtido={num_replaced}"
        )

@pytest.mark.integration
@pytest.mark.origin_subst
def test_mapping_dicts_not_empty():
    """Garante que os dicionários de substituição têm ao menos 1 item."""
    assert ID_CONTENT_REPLACEMENTS,  "ID_CONTENT_REPLACEMENTS vazio"
    assert CAMPAIGN_NAME_REPLACEMENTS, "CAMPAIGN_NAME_REPLACEMENTS vazio"
    assert AD_GROUP_NAME_REPLACEMENTS, "AD_GROUP_NAME_REPLACEMENTS vazio"
