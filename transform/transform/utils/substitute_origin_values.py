# treat/utils/substitute_origin_values.py

"""
Aplica substituições (utm_content, campaign_name, …) em memória
e, opcionalmente, grava as diferenças usando a worksheet já aberta
pelo pipeline.  Nenhuma nova autenticação é feita aqui.
"""

from __future__ import annotations

import itertools
import logging
from typing import Dict, Iterable, List, Mapping

import pandas as pd
from gspread import Worksheet
from gspread.utils import rowcol_to_a1

from transform.transform.utils.substitutions_lists import (  # listas continuam no projeto
    AD_GROUP_NAME_REPLACEMENTS, AD_NAME_REPLACEMENTS,
    CAMPAIGN_NAME_REPLACEMENTS, ID_CONTENT_REPLACEMENTS)

__all__ = ["apply_all_origin_substitutions"]

# ---------------------------------------------------------------------------
_REPLACEMENT_SPECS: list[tuple[str, Mapping[str, str]]] = [
    ("utm_content", ID_CONTENT_REPLACEMENTS),
    ("campaign_name", CAMPAIGN_NAME_REPLACEMENTS),
    ("ad_group_name", AD_GROUP_NAME_REPLACEMENTS),
    ("ad_name", AD_NAME_REPLACEMENTS),
]

_MAX_CELLS_PER_BATCH = 10_000  # segurança – 10 k células ≅ 5 MiB
# ---------------------------------------------------------------------------


def _normalize_series(s: pd.Series) -> pd.Series:
    """strip + lower; preserva NaN como string vazia."""
    return s.fillna("").astype(str).str.strip().str.lower()


def _chunk(it: Iterable, n: int):
    """Yield blocos de tamanho n."""
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
    *,
    worksheet: Worksheet | None = None,
    write_back: bool = True,
    inplace: bool = True,
    header: List[str] | None = None,
) -> pd.DataFrame:
    """
    Aplica substituições nas colunas de origem.

    Parâmetros
    ----------
    df         : DataFrame de origem.
    worksheet  : Worksheet já aberta pelo pipeline (obrigatório se write_back=True).
    write_back : Se True, atualiza apenas as células modificadas.
    inplace    : Se False, opera sobre uma cópia de *df*.

    Retorna
    -------
    DataFrame resultante (cópia ou referência, conforme *inplace*).
    """
    log = logging.getLogger(__name__)

    if write_back and worksheet is None:
        raise ValueError("`worksheet` deve ser fornecido se write_back=True.")

    if not inplace:
        df = df.copy()

    header_lc: List[str] = []
    if write_back:
        if header is not None:
            header_lc = [h.strip().lower() for h in header]
        else:
            header_lc = [h.strip().lower() for h in worksheet.row_values(1)]

    # Mantém index estável → mapeia linha → célula
    df.reset_index(drop=True, inplace=True)

    updates: List[Dict[str, object]] = []

    for col_name, mapping in _REPLACEMENT_SPECS:
        if col_name not in df.columns or not mapping:
            continue

        # normaliza dicionário
        mapping_norm = {k.strip().lower(): v for k, v in mapping.items()}

        s_norm = _normalize_series(df[col_name])
        mask = s_norm.isin(mapping_norm)
        n_changes = int(mask.sum())
        if n_changes == 0:
            continue

        # log de amostra
        sample_before = df.loc[mask, col_name].head(3).to_list()
        sample_after = [
            mapping_norm[_normalize_series(pd.Series([v]))[0]] for v in sample_before
        ]
        log.debug(
            "[%s] %d alterações. Ex.: %s → %s",
            col_name,
            n_changes,
            sample_before,
            sample_after,
        )

        # aplica no DataFrame
        df.loc[mask, col_name] = s_norm[mask].map(mapping_norm)

        # prepara updates
        if write_back:
            try:
                col_idx = header_lc.index(col_name.lower()) + 1
            except ValueError:
                log.warning(
                    "[subst] coluna '%s' não existe no header – skip write-back",
                    col_name,
                )
                continue

            updates.extend(
                {
                    "range": rowcol_to_a1(row_i + 2, col_idx),
                    "values": [[df.at[row_i, col_name]]],
                }
                for row_i in df.index[mask]
            )

    # envia batch
    if write_back and updates:
        for block in _chunk(updates, _MAX_CELLS_PER_BATCH):
            worksheet.batch_update(block, value_input_option="RAW")
        log.info("[subst] %d células gravadas em '%s'", len(updates), worksheet.title)

    return df
