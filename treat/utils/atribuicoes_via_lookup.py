from __future__ import annotations

import logging
import os
from typing import Tuple

import pandas as pd

from utils.google_sheets import (
    carregar_aba_google_sheets,
    CREDS_PATH,
    SPREADSHEET_URL,
)
from utils.normalize import (
    extract_meta_platform_from_placement,
)
from utils.campanha_mapper import buscar_mapping

log = logging.getLogger(__name__)

PLATFORM_TO_VEICULO = {
    "tiktok":     "TikTok",
    "linkedin":   "LinkedIn",
    "pinterest":  "Pinterest",
    "twitter":    "Twitter",
    "youtube":    "YouTube",
    'Facebook':   "Facebook",
    "Instagram":  "Instagram"
    # adicione outros conforme necessário
}

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────


def _load_source_mapping() -> dict[str, str]:
    """Carrega a aba SOURCE e devolve {descrição_mídia_lower: id_veiculo}."""
    df_source = carregar_aba_google_sheets(
        CREDS_PATH,
        SPREADSHEET_URL,
        "SOURCE",
    )
    df_source["Descrição da Mídia"] = (
        df_source["Descrição da Mídia"].str.strip().str.lower()
    )
    return dict(zip(df_source["Descrição da Mídia"], df_source["ID_Veiculo"]))


# cache simples de SOURCE mapping (recarrega só se erro)
try:
    _SOURCE_MAP = _load_source_mapping()
except Exception as exc:  # pragma: no cover
    log.warning("Falha ao carregar SOURCE: %s", exc)
    _SOURCE_MAP = {}


# ─────────────────────────────────────────────────────────────────────────────
# 1) ID_VEICULO GENÉRICO
# ─────────────────────────────────────────────────────────────────────────────

def atribuir_id_veiculo_generico(df: pd.DataFrame) -> pd.DataFrame:
    """
    Usa 'Veiculo' para preencher 'ID_Veiculo' consultando SOURCE.
    """
    log.debug(">>> atribuir_id_veiculo_generico")
    if "Veiculo" not in df.columns:
        df["ID_Veiculo"] = ""
        return df

    df["ID_Veiculo"] = (
        df["Veiculo"].astype(str)
        .str.strip()
        .str.lower()
        .map(_SOURCE_MAP)
        .fillna("")
    )
    return df


# ─────────────────────────────────────────────────────────────────────────────
# 2) META – inferir Veiculo e ID_Veiculo a partir de placement
# ─────────────────────────────────────────────────────────────────────────────

def atribuir_veiculo_e_id_meta(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cria/atualiza 'Veiculo' e 'ID_Veiculo' usando a coluna 'placement' do Meta.
    Se não houver 'placement', retorna o DataFrame sem sobrescrever 'Veiculo'.
    """
    log.debug(">>> atribuir_veiculo_e_id_meta")

    # garante que as colunas existem, mas sem atribuir valor
    for col in ("Veiculo", "ID_Veiculo"):
        if col not in df.columns:
            df[col] = ""

    # só inferimos quando for Meta (tem 'placement')
    if "placement" in df.columns:
        df["Veiculo"] = df["placement"].apply(
            lambda p: extract_meta_platform_from_placement(p)
            if isinstance(p, str)
            else ""
        )
        # depois, preenche ID_Veiculo via SOURCE
        df = atribuir_id_veiculo_generico(df)

    return df



# ─────────────────────────────────────────────────────────────────────────────
# 3) LINKEDIN / TWITTER – veiculo via Criativo
# ─────────────────────────────────────────────────────────────────────────────

def atribuir_veiculo_por_criativo(df: pd.DataFrame) -> pd.DataFrame:
    """
    Lookup de 'Ad name' (CRIATIVO) → 'Veiculo' via BI_PARAMETRIZAÇÃO.
    """
    log.debug(">>> atribuir_veiculo_por_criativo")
    df_param = carregar_aba_google_sheets(
        CREDS_PATH, SPREADSHEET_URL, "BI_PARAMETRIZAÇÃO", header_row_index=1
    )
    df_param.columns = [c.strip().upper() for c in df_param.columns]

    if {"CRIATIVO", "VEÍCULOS"} <= set(df_param.columns):
        mapping = dict(
            zip(
                df_param["CRIATIVO"].astype(str).str.strip(),
                df_param["VEÍCULOS"].astype(str).str.strip(),
            )
        )
        df["Veiculo"] = (
            df["Ad name"].astype(str).str.strip()
            .map(mapping)
            .fillna("")
            if "Ad name" in df.columns
            else ""
        )
    else:
        log.warning(
            "Colunas 'CRIATIVO' ou 'VEÍCULOS' não encontradas em BI_PARAMETRIZAÇÃO."
        )
        df["Veiculo"] = ""

    return df


# ─────────────────────────────────────────────────────────────────────────────
# 4) PINTEREST – replica campaign_name em colunas destino
# ─────────────────────────────────────────────────────────────────────────────

def preencher_campos_com_campanha(df: pd.DataFrame) -> pd.DataFrame:
    """
    Copia 'Campaign name' para campos de campanha no Pinterest.
    """
    log.debug(">>> preencher_campos_com_campanha (Pinterest)")
    if "Campaign name" not in df.columns:
        log.warning(
            "Coluna 'Campaign name' ausente; não será possível preencher campos."
        )
        df["Campanha"] = ""
        df["Nome_do_Anuncio"] = ""
        df["Nome_do_Conjunto_de_Anuncio"] = ""
        return df

    df["Campanha"] = df["Campaign name"]
    df["Nome_do_Anuncio"] = df["Campaign name"]
    df["Nome_do_Conjunto_de_Anuncio"] = df["Campaign name"]
    return df


# ─────────────────────────────────────────────────────────────────────────────
# 5) Campanha – parametrização genérica
# ─────────────────────────────────────────────────────────────────────────────

def aplicar_parametrizacao_campanha(
    df: pd.DataFrame,
    mapping_campanha: dict,
    mapping_sigla: dict,
) -> pd.DataFrame:
    """
    Preenche 'Campanha' e 'ID_Campanha' a partir de 'Campaign_name'.
    """
    log.debug(">>> aplicar_parametrizacao_campanha")
    if "Campaign_name" not in df.columns:
        df["Campanha"] = ""
        df["ID_Campanha"] = ""
        return df

    df["Campanha"] = df["Campaign_name"].apply(
        lambda x: buscar_mapping(mapping_campanha, x) or x
    )
    df["ID_Campanha"] = df["Campaign_name"].apply(
        lambda x: buscar_mapping(mapping_sigla, x)
    )
    return df



def atribuir_veiculo_por_prefixo(df: pd.DataFrame, prefixo: str) -> pd.DataFrame:
    """
    Define Veiculo = nome capitalizado do prefixo e ID_Veiculo via SOURCE lookup.
    """
    veic = PLATFORM_TO_VEICULO.get(prefixo.lower(), prefixo.capitalize())
    # cria coluna se não existir
    if "Veiculo" not in df.columns:
        df["Veiculo"] = ""
    # só preenche onde estiver vazio
    df["Veiculo"] = df["Veiculo"].where(df["Veiculo"].str.strip() != "", veic)
    # chama o genérico para preencher ID_Veiculo
    return atribuir_id_veiculo_generico(df)


# ─────────────────────────────────────────────────────────────────────────────
# ALIASES PARA COMPATIBILIDADE (nomes antigos)
# ─────────────────────────────────────────────────────────────────────────────




# alias para quem importava 'atribuir_veiculo_meta'
atribuir_veiculo_meta = atribuir_veiculo_e_id_meta
