# transform/transform_runner.py
"""
Wrappers convenientes em torno do TreatPipeline.

Cada função prepara a instância correta de TreatPipeline e chama .run(),
centralizando todo o fluxo de transformação (pré-processo, BI, write-back,
renomeação, substituição de objective, geração de ID).

Para adicionar nova aba/plataforma:
 1) crie o dicionário de renomeação em treat/utils/renomeacoes.py
 2) adicione um wrapper neste arquivo (4-5 linhas)
"""

from __future__ import annotations

import logging
from typing import Dict

import pandas as pd

from transform.transform.preprocess_utils import preprocess_origin
from transform.transform.transform_pipeline import TreatPipeline
from transform.transform.utils.renomeacoes import (  # renomeacao_tiktok_genero,; renomeacao_pinterest_idade,
    renomeacao_geral, renomeacao_metaGenero, renomeacao_metaIdade)

log = logging.getLogger(__name__)


def _run_pipeline(
    df_raw: pd.DataFrame,
    creds_path: str,
    spreadsheet_id: str,
    sheet_name: str,
    mapping: Dict[str, str],
    *,
    write_back: bool = True,
) -> pd.DataFrame:
    """
    Instancia e executa o pipeline genérico.
    """
    pipeline = TreatPipeline(
        creds_path=creds_path,
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        mapping_renomeacao=mapping,
        write_back=write_back,
    )
    return pipeline.run(df_raw)


# ───────────────────────── wrappers públicos ─────────────────────────── #


def treat_general_data(
    df_raw: pd.DataFrame,
    creds_path: str,
    spreadsheet_id: str,
    *,
    write_back: bool = True,
) -> pd.DataFrame:
    """Processa aba 'metaGeral' (Meta Ads – dados gerais)"""
    return _run_pipeline(
        df_raw,
        creds_path,
        spreadsheet_id,
        sheet_name="metaGeral",
        mapping=renomeacao_geral,
        write_back=write_back,
    )


def treat_gender_data(
    df_raw: pd.DataFrame,
    creds_path: str,
    spreadsheet_id: str,
    *,
    write_back: bool = True,
) -> pd.DataFrame:
    """Processa aba 'metaGenero' (Meta Ads – distribuição por gênero)"""
    return _run_pipeline(
        df_raw,
        creds_path,
        spreadsheet_id,
        sheet_name="metaGenero",
        mapping=renomeacao_metaGenero,
        write_back=write_back,
    )


def treat_age_data(
    df_raw: pd.DataFrame,
    creds_path: str,
    spreadsheet_id: str,
    *,
    write_back: bool = True,
) -> pd.DataFrame:
    """Processa aba 'metaIdade' (Meta Ads – distribuição por faixa etária)"""
    return _run_pipeline(
        df_raw,
        creds_path,
        spreadsheet_id,
        sheet_name="metaIdade",
        mapping=renomeacao_metaIdade,
        write_back=write_back,
    )


def treat_ga_geral(
    df_raw: pd.DataFrame,
    creds_path: str,
    spreadsheet_id: str,
    *,
    write_back: bool = True,
) -> pd.DataFrame:
    """Processa aba 'GAGeral' (Google Analytics)"""
    if "Province name" in df_raw.columns and "region" not in df_raw.columns:
        df_raw = df_raw.rename(columns={"Province name": "region"})
    # Pré-processa para normalizar região e aplicar substituições
    df_raw = preprocess_origin(df_raw)
    return _run_pipeline(
        df_raw,
        creds_path,
        spreadsheet_id,
        sheet_name="GAGeral",
        mapping=renomeacao_geral,
        write_back=write_back,
    )


# Exemplo de wrappers adicionais para outras plataformas:
# def treat_tiktok_gender(...):
#     return _run_pipeline(..., sheet_name="tiktokGenero", mapping=renomeacao_tiktok_genero)
#
# def treat_pinterest_age(...):
#     return _run_pipeline(..., sheet_name="pinterestIdade", mapping=renomeacao_pinterest_idade)


# ───── aliases para compatibilidade com código legado ─────

treat_meta_geral = treat_general_data
treat_meta_genero = treat_gender_data
treat_meta_idade = treat_age_data
treat_ga = treat_ga_geral
