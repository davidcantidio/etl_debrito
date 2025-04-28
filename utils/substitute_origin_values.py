import logging
import itertools
from typing import Mapping, Iterable, Optional, List, Dict

import pandas as pd
from gspread.utils import rowcol_to_a1
from gspread import Worksheet

from utils.substitutions_lists import (
    ID_CONTENT_REPLACEMENTS,
    CAMPAIGN_NAME_REPLACEMENTS,
    AD_GROUP_NAME_REPLACEMENTS,
    AD_NAME_REPLACEMENTS
)
from utils.get_google_client import get_google_client
from utils.google_sheets import CREDS_PATH, SPREADSHEET_ID

__all__ = [
    "apply_all_origin_substitutions",
]

# ---------------------------------------------------------------------------
# Configurações
# ---------------------------------------------------------------------------

_REPLACEMENT_SPECS: list[tuple[str, Mapping[str, str]]] = [
    ("Content (utm)", ID_CONTENT_REPLACEMENTS),
    ("Campaign name", CAMPAIGN_NAME_REPLACEMENTS),
    ("Ad group name", AD_GROUP_NAME_REPLACEMENTS),
    ("Ad name", AD_NAME_REPLACEMENTS)
]

# Segurança contra payloads enormes; 10k células ≅ 5 MiB.
_MAX_CELLS_PER_BATCH = 10_000

# ---------------------------------------------------------------------------
# Helpers internos (mantidos privados deliberadamente)
# ---------------------------------------------------------------------------


def _normalize_series(s: pd.Series) -> pd.Series:
    """Normaliza *strings* – strip + lower. Preserva NaN como string vazia."""
    return s.fillna("").astype(str).str.strip().str.lower()


def _chunk(it: Iterable, n: int):
    """Yield blocos de tamanho *n* sem levantar StopIteration externamente."""
    it = iter(it)
    while True:
        block = list(itertools.islice(it, n))
        if not block:
            break
        yield block


# ---------------------------------------------------------------------------
# Função pública
# ---------------------------------------------------------------------------


def apply_all_origin_substitutions(
    df: pd.DataFrame,
    sheet_name: Optional[str] = None,
    *,
    worksheet: Worksheet | None = None,
    write_back: bool = True,
    inplace: bool = True,
) -> pd.DataFrame:
    """Substitui valores *in‑place* nas colunas de origem e, opcionalmente,
    grava as mudanças na planilha Google.

    A lógica é idêntica à versão original, porém:
    • Header do Sheets é normalizado para *lower* → robusto a capitalização.
    • Menos conversões redundantes (normaliza série **uma** vez).
    • Evita re‑normalizar amostras de log.
    • Usa list‑comprehension mais enxuta para construir *updates*.
    • Mantém compatibilidade 100 % com a API e com os parâmetros da função.
    """

    log = logging.getLogger(__name__)

    if not inplace:
        df = df.copy()

    # --- prepara worksheet / cabeçalho ------------------------------------ #
    if write_back:
        if worksheet is None:
            if not sheet_name:
                raise ValueError("sheet_name é obrigatório se write_back=True e worksheet=None.")
            client = get_google_client(CREDS_PATH)
            worksheet = client.open_by_key(SPREADSHEET_ID).worksheet(sheet_name)
        header_sheet: list[str] = worksheet.row_values(1)
        header_sheet_lc: list[str] = [h.strip().lower() for h in header_sheet]

    # Mantemos um index consistente para mapear linhas → A1.
    df.reset_index(drop=True, inplace=True)

    # --- loop pelas colunas alvo ----------------------------------------- #
    updates: list[Dict[str, object]] = []

    for col_name, mapping in _REPLACEMENT_SPECS:
        if col_name not in df.columns:
            log.debug("[subst] coluna ausente: %s", col_name)
            continue
        if not mapping:
            log.debug("[subst] mapping vazio para %s", col_name)
            continue

        # Normaliza apenas UMA vez
        # normalizando as chaves do dicionário
        mapping = {
            k.strip().lower(): v
            for k, v in mapping.items()
        }

        s_norm = _normalize_series(df[col_name])
        mask = s_norm.isin(mapping)  # bool Series
        n_changes = int(mask.sum())
        if n_changes == 0:
            log.debug("[subst] nenhuma ocorrência em %s", col_name)
            continue

        # Logging de amostra (não re‑normaliza)
        sample_before = df.loc[mask, col_name].head(3).to_list()
        sample_after = [mapping[_normalize_series(pd.Series([v]))[0]] for v in sample_before]
        log.debug(
            "[subst] %d alterações em '%s'. Ex.: %s → %s",
            n_changes,
            col_name,
            sample_before,
            sample_after,
        )

        # Aplica substituição usando Series.map
        df.loc[mask, col_name] = s_norm[mask].map(mapping)

        # --- write‑back --------------------------------------------------- #
        if write_back:
            col_lookup = col_name.strip().lower()
            try:
                col_idx_1based = header_sheet_lc.index(col_lookup) + 1
            except ValueError:
                log.warning("[subst] '%s' não encontrado no header da planilha – skip write‑back", col_name)
                continue

            updates.extend(
                {
                    "range": rowcol_to_a1(row_i + 2, col_idx_1based),
                    "values": [[df.at[row_i, col_name]]],
                }
                for row_i in df.index[mask]
            )
            log.info("[subst] %d células preparadas para gravação em '%s'", n_changes, col_name)

    # --- envia batch updates --------------------------------------------- #
    if write_back and updates:
        for block in _chunk(updates, _MAX_CELLS_PER_BATCH):
            worksheet.batch_update(block, value_input_option="RAW")
        log.info("[subst] gravação concluída – total de %d células", len(updates))
    elif write_back:
        log.info("[subst] nenhuma célula a gravar – nada foi alterado")

    return df
