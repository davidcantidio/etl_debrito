from __future__ import annotations
import logging
import re
import datetime

from typing import Optional, Tuple, List, Dict

import pandas as pd

from treat.utils.write_back import write_back_df
from treat.utils.get_google_client import get_google_client

# Mapeia o "data_type" para a aba‑destino; a ordem de colunas virá do próprio header
DESTINATION_SHEETS: Dict[str, str] = {
    "geral":   "modeloGeral",
    "genero":  "modeloGenero",
    "idade":   "modeloIdade",
    "alcance": "modeloAlcance",
    "regiao":  "modeloRegiao",
}

log = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# helpers
# ──────────────────────────────────────────────────────────────────────────────

def infer_data_type(sheet_name: str) -> str:
    """Extrai o sufixo da aba‑origem (geral/genero/idade/alcance/regiao)."""
    m = re.search(r"(geral|genero|idade|alcance|regiao)$", sheet_name.lower())
    if not m:
        raise ValueError(f"Não reconheço o tipo de dados da aba '{sheet_name}'")
    return m.group(1)

def _to_scalar(v):
    if isinstance(v, pd.Series):
        return v.iat[0] if not v.empty else ""
    if isinstance(v, (list, tuple)):
        return v[0] if v else ""
    if isinstance(v, str) and "dtype:" in v and "\n" in v:
        first_line = v.splitlines()[0]
        tokens = first_line.split()
        return tokens[-1] if tokens else first_line.strip()
    return v

# ──────────────────────────────────────────────────────────────────────────────
# main writer
# ──────────────────────────────────────────────────────────────────────────────

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
    """Escreve apenas linhas novas no modelo, usando o **header da aba** como guia."""
    if not write_back:
        log.info("Write‑back destino desabilitado para '%s'", data_type)
        return None

    try:
        sheet_name = DESTINATION_SHEETS[data_type]
    except KeyError:
        raise ValueError(f"Tipo de dados desconhecido '{data_type}'")

    # Conecta ao Sheets
    client = get_google_client(creds_path)
    ws = client.open_by_key(spreadsheet_id).worksheet(sheet_name)

    # 1) Header atual da aba de destino (linha 1)
    header = ws.row_values(1)
    header = [h.strip() for h in header if h.strip()]
    header_lc = [h.lower() for h in header]

    # 2) Constrói df_dest com as colunas que existem no header
    cols_to_write = [col for col in header if col in df_model.columns]
    df_dest = df_model[cols_to_write].copy()

    # 2.1) Se faltar alguma coluna do header, adiciona vazia para manter alinhamento
    for col in header:
        if col not in df_dest.columns:
            df_dest[col] = ""
    df_dest = df_dest[header]  # garante ordem exata

    # 2.2) Scalariza valores
    df_dest = df_dest.transform(lambda col: col.map(_to_scalar))
    # ── Remove NaN/resultados vazios para evitar erro JSON
    df_dest = df_dest.fillna("")

    # ── Converte objetos date em string ISO para JSON
    def _date_to_iso_dest(v):
        return v.isoformat() if isinstance(v, datetime.date) else v
    df_dest = df_dest.applymap(_date_to_iso_dest)
    df_dest = df_dest.fillna("")

    # 3) Deduplicação por ID
    existing_ids: set[str] = set()
    if "ID" in header_lc:
        id_col_idx = header_lc.index("id") + 1
        id_values = ws.col_values(id_col_idx)[1:]  # skip header
        existing_ids = {v.strip() for v in id_values if v.strip()}

    if existing_ids:
        if "ID" not in df_dest.columns:
            log.warning("Header tem 'ID' mas df_model não – escrevendo todas as linhas.")
            df_new = df_dest
        else:
            df_new = df_dest.loc[~df_dest["ID"].isin(existing_ids)].copy()
            log.info("Linhas novas a escrever: %d", len(df_new))
    else:
        log.info("Aba '%s' não tem IDs existentes; todas as linhas serão escritas", sheet_name)
        df_new = df_dest

    if df_new.empty:
        log.info("Nenhuma linha nova para escrever em '%s'", sheet_name)
        return None

    log.info("Preparando write‑back destino: %d linhas × %d colunas em '%s'", len(df_new), len(header), sheet_name)

    if dry_run:
        log.info("[Dry run] não escrevendo na aba '%s'", sheet_name)
        return df_new

    write_back_df(
        df=df_new,
        creds_path=creds_path,
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        a1_range=a1_range,
        value_input_option=value_input_option,
    )
    log.info("✅ Write‑back concluído (%d linhas) em '%s'", len(df_new), sheet_name)
    return df_new


def write_back_for_sheet(
    df_model: pd.DataFrame,
    sheet_name: str,
    creds_path: str,
    spreadsheet_id: str,
    *,
    write_back: bool = True,
    dry_run: bool = False,
) -> Optional[pd.DataFrame]:
    """Inferir data_type a partir da aba‑origem e chamar write_back_destination."""
    data_type = infer_data_type(sheet_name)
    return write_back_destination(
        df_model=df_model,
        data_type=data_type,
        creds_path=creds_path,
        spreadsheet_id=spreadsheet_id,
        write_back=write_back,
        dry_run=dry_run,
    )
