# load/dest_writer.py

"""
Write-back otimizado para as abas **modelo***

Principais características
-------------------------
* 01 leitura para cabeçalhos e IDs: usamos SheetsFetcher para recuperar todas
  as abas-modelo de uma só vez (batchGet), poupando quota.
* Zero leituras extras durante o loop de ETL; somente gravação (via write_back_df ou append).
* Deduplicação: grava apenas registros cujo "ID" ainda não existe.
* Compatível com dry-run e com execução em paralelo.
"""

import datetime as _dt
import logging
import re
from typing import Dict, List, Optional, Set

import numpy as np
import pandas as pd
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from extract.sheets_fetcher import SheetsFetcher
from treat.utils.write_back import write_back_df
from load.utils.append_records_to_sheet import append_records_to_sheet
from treat.utils.validations import validate_columns

log = logging.getLogger(__name__)

# ───────────────────────────── mapeamento origem → destino ──────────────────
DESTINATION_SHEETS: Dict[str, str] = {
    "geral":   "modeloGeral",
    "genero":  "modeloGenero",
    "idade":   "modeloIdade",
    "alcance": "modeloAlcance",
    "regiao":  "modeloRegiao",
}

# ───────────────────────────── caches globais em memória ────────────────────
_HEADERS: Dict[str, List[str]] = {}        # aba-destino → header completo
_EXISTING_IDS: Dict[str, Set[str]] = {}    # aba-destino → set(ID)


def _idx_to_col(idx: int) -> str:
    """Converte índice 0-based em letras de coluna estilo A1 (A, B, …, AA…)."""
    out = ""
    while True:
        idx, rem = divmod(idx, 26)
        out = chr(65 + rem) + out
        if idx == 0:
            return out
        idx -= 1


def prefetch_meta(
    fetcher: SheetsFetcher,
    spreadsheet_id: str
) -> None:
    """
    Carrega cabeçalhos (linha 1) e IDs (coluna 'ID' a partir da linha 2)
    de todas as abas-modelo (modeloGeral, modeloGenero, etc.) em UMA ÚNICA
    operação, poupando leitura repetida da API.

    Depois disso, os caches globais _HEADERS e _EXISTING_IDS estão prontos
    para uso em write_back_destination, sem novas leituras.
    """
    global _HEADERS, _EXISTING_IDS
    if _HEADERS:
        # já realizou prefetch nesta sessão
        return

    abas = list(DESTINATION_SHEETS.values())

    # 1) Buscar todas as linhas de cada aba no fetcher (cada aba → Lista[List[str]])
    raw_data = fetcher.get(abas, as_frame=False)

    # 2) Extrair cabeçalhos (linha 0 de cada aba) e armazenar em _HEADERS
    for tab, values in raw_data.items():
        primeira_linha = values[0] if values else []
        header = [c.strip() for c in primeira_linha if c.strip()]
        _HEADERS[tab] = header

    # 3) Identificar intervalos de ID para cada aba-modelo
    id_ranges: List[Optional[str]] = []
    for tab in abas:
        header = _HEADERS.get(tab, [])
        try:
            idx_id = [h.lower() for h in header].index("id")
            col_a1 = _idx_to_col(idx_id)
            # range a partir da linha 2 (sem cabeçalho)
            id_ranges.append(f"{tab}!{col_a1}2:{col_a1}")
        except ValueError:
            _EXISTING_IDS[tab] = set()
            id_ranges.append(None)

    # 4) Ler todas as colunas 'ID' de uma vez (quando existirem)
    valid_tabs = []
    for tab, rng in zip(abas, id_ranges):
        if rng:
            valid_tabs.append(tab)

    if valid_tabs:
        raw_ids = fetcher.get(valid_tabs, as_frame=False)
        for tab in valid_tabs:
            listas = raw_ids.get(tab, [])
            # cada linha em listas corresponde a uma célula ID
            ids = [item for row in listas for item in row if str(item).strip()]
            _EXISTING_IDS[tab] = set(ids)

    log.info(
        "📥 Prefetch destino concluído: %d abas com cabeçalho, total de IDs carregados=%d",
        len(_HEADERS),
        sum(len(s) for s in _EXISTING_IDS.values()),
    )


def _scalar(v):
    if isinstance(v, _dt.date):
        return v.isoformat()
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return ""
    return v


def _infer_data_type(sheet_name: str) -> str:
    """
    Dado nome de aba de origem (exemplo: 'metaGenero', 'tiktokIdade'), devolve
    o sufixo que mapeia para a aba-modelo correspondente. Ex.: 'genero', 'idade'.
    """
    lower = sheet_name.strip().lower()
    if lower.startswith("ga"):
        raise ValueError(
            f"Aba '{sheet_name}' é do Google Analytics – não grava em abas-modelo."
        )
    m = re.search(r"(geral|genero|idade|alcance|regiao)$", lower)
    if not m:
        raise ValueError(
            f"Não reconheço o tipo de dados da aba '{sheet_name}' "
            "(esperava terminar em geral/genero/idade/alcance/regiao)."
        )
    return m.group(1)


def write_back_destination(
    df_model: pd.DataFrame,
    data_type: str,
    creds_path: str,
    spreadsheet_id: str,
    *,
    write_back: bool = True,
    dry_run: bool = False,
    a1_range: str = "A1",
    value_input_option: str = "USER_ENTERED",
) -> Optional[pd.DataFrame]:
    """
    Grava `df_model` na aba-modelo correspondente, deduplicando por 'ID'.

    Pré-requisito: `prefetch_meta(fetcher, spreadsheet_id)` já deve ter sido
    invocado antes, para que _HEADERS e _EXISTING_IDS estejam preenchidos.
    """
    if df_model.empty:
        log.info("Destino '%s': DataFrame vazio – nada a gravar", data_type)
        return None

    sheet_name = DESTINATION_SHEETS[data_type]
    if sheet_name not in _HEADERS:
        raise RuntimeError("Caches destino não carregados – chame prefetch_meta() antes.")

    header = _HEADERS[sheet_name]

    # 1) Reindexa para o layout exato de colunas-modelo
    df_out = (
        df_model
        .reindex(columns=header, fill_value="")
        .applymap(_scalar)
    )

    # 2) Validação de esquema pós-reindexação
    validate_columns(df_out, header, stage=f"Destino {sheet_name}")

    # 3) Deduplicação por ID
    if "ID" in df_out.columns:
        existing = _EXISTING_IDS.get(sheet_name, set())
        df_out = df_out.loc[~df_out["ID"].isin(existing)]

    if df_out.empty:
        log.info("Destino '%s': nenhuma linha nova para gravar", data_type)
        return None

    # 4) Se dry_run ou write_back=False, apenas logar e retornar
    if dry_run or not write_back:
        log.info("[Dry-run] %d linhas seriam gravadas em '%s'", len(df_out), sheet_name)
        return df_out

    # 5) Gravação: APPEND em vez de overwrite
    #    Usamos append_records_to_sheet, que faz append a partir da primeira linha vazia.
    #    Caso append_records_to_sheet não retorne contagem, assumimos len(df_out).
    try:
        linhas_inseridas = append_records_to_sheet(
            creds_path      = creds_path,
            spreadsheet_id  = spreadsheet_id,
            sheet_name      = sheet_name,
            df              = df_out,
        )
        if linhas_inseridas is None:
            linhas_inseridas = len(df_out)
    except Exception as e:
        log.error("Erro ao gravar em '%s' via append: %s", sheet_name, e)
        raise

    log.info("✅ Gravadas %d linha(s) em '%s'", linhas_inseridas, sheet_name)

    # 6) Atualiza cache de IDs para futuras deduplicações
    if "ID" in df_out.columns:
        _EXISTING_IDS.setdefault(sheet_name, set()).update(df_out["ID"].astype(str).tolist())

    return df_out


def write_back_for_sheet(
    df_model: pd.DataFrame,
    sheet_name: str,
    creds_path: str,
    spreadsheet_id: str,
    *,
    write_back: bool = True,
    dry_run: bool = False,
) -> Optional[pd.DataFrame]:
    """
    Wrapper: infere `data_type` a partir do nome da aba de origem
    e chama write_back_destination.
    """
    data_type = _infer_data_type(sheet_name)
    return write_back_destination(
        df_model=df_model,
        data_type=data_type,
        creds_path=creds_path,
        spreadsheet_id=spreadsheet_id,
        write_back=write_back,
        dry_run=dry_run,
    )
