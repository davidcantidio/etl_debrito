from extract import read_df
import pandas as pd
import logging
from typing import Iterable

from ponto_de_controle.constants import DEST_SHEET_ID, DEST_TAB, HEAD_ROW_DEST
from ponto_de_controle.constants import DEST_COLUMNS
from transform.utils.campos_calculados import make_id_ponto_de_controle

logger = logging.getLogger(__name__)

def read_destination_df() -> pd.DataFrame:
    """
    Lê a planilha de destino, resolve colunas duplicadas,
    reindexa em DEST_COLUMNS e devolve DataFrame único por __ID__.
    
    Raises:
        ValueError: Se configurações obrigatórias estão ausentes
        RuntimeError: Se falha ao acessar planilha de destino
    """
    if not DEST_SHEET_ID:
        raise ValueError("DEST_SHEET_ID não configurado")
    
    if not DEST_TAB:
        raise ValueError("DEST_TAB não configurado")
    
    logger.info("Lendo destino %s › %s …", DEST_SHEET_ID, DEST_TAB)
    try:
        df = read_df(sheet_id=DEST_SHEET_ID, tab=DEST_TAB, header_row=HEAD_ROW_DEST)
    except Exception as e:
        logger.error("Erro ao ler planilha de destino: %s", e)
        raise RuntimeError(f"Falha ao acessar planilha de destino: {e}") from e

    # renomeia colunas duplicadas deterministicamente
    if df.columns.duplicated().any():
        seen = {}
        new_cols = []
        for col in df.columns:
            count = seen.get(col, 0)
            new_cols.append(col if count == 0 else f"{col}.{count}")
            seen[col] = count + 1
        df.columns = new_cols
        logger.warning("Cabeçalhos duplicados renomeados: %s", df.columns.tolist())

    # reindexa e preenche faltantes
    df = df.reindex(columns=DEST_COLUMNS, fill_value="")

    # gera __ID__ e deduplica
    try:
        df["__ID__"] = df.apply(make_id_ponto_de_controle, axis=1, columns=DEST_COLUMNS)
    except Exception as e:
        logger.error("Erro ao gerar __ID__ para destino: %s", e)
        raise RuntimeError(f"Falha ao gerar identificadores únicos: {e}") from e
    
    # Verifica se há IDs válidos
    invalid_ids = df["__ID__"].isna().sum()
    if invalid_ids > 0:
        logger.warning("Encontrados %d __ID__ inválidos no destino", invalid_ids)
    
    before = len(df)
    df = df.drop_duplicates("__ID__", keep="first").reset_index(drop=True)
    duplicates_removed = before - len(df)
    
    if duplicates_removed > 0:
        logger.info("Destino: %d duplicatas removidas (%d → %d linhas)", 
                   duplicates_removed, before, len(df))
    else:
        logger.info("Destino: %d linhas únicas (sem duplicatas)", len(df))

    return df
