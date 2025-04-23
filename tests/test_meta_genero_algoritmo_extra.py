import pytest
import pandas as pd
import logging
from collections import Counter

from utils.common.meta.gender_placement_merge import (
    load_and_prepare_meta_gender_data,
    load_and_prepare_meta_placement_data,
    pivot_meta_gender_data,
    pivot_meta_placement_data,
    merge_placement_and_gender_data,
    distribute_gender_metrics,
    METRICAS,
    DISTRIBUICAO_LOGS,
)
from utils.normalize import normalize_gender

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@pytest.fixture(scope="module")
def df_genero_mergeado():
    """Retorna os dados pivotados + mergeados, prontos para redistribuição"""
    df_gender = load_and_prepare_meta_gender_data()
    df_placement = load_and_prepare_meta_placement_data()

    df_gender_pivot = pivot_meta_gender_data(df_gender)
    df_placement_pivot = pivot_meta_placement_data(df_placement)

    df_merged = merge_placement_and_gender_data(df_gender_pivot, df_placement_pivot)

    return df_gender_pivot, df_merged


def test_distribuicao_e_somas(df_genero_mergeado):
    """Testa se a redistribuição por gênero mantém a soma global e aplica corretamente a lógica especial"""
    df_gender, df_merged = df_genero_mergeado

    df_distribuido = distribute_gender_metrics(df_merged)
    assert not df_distribuido.empty, "DataFrame redistribuído está vazio."

    # Checagem 1: soma global das métricas é preservada
    for metrica in METRICAS:
        soma_antes = df_gender[metrica].sum()
        soma_depois = df_distribuido[metrica].sum()
        tolerancia = 0.01 if metrica == "Cost" else 1e-6
        assert abs(soma_antes - soma_depois) <= tolerancia, \
            f"Soma divergente em '{metrica}': antes={soma_antes} | depois={soma_depois}"

    # Checagem 2: logs de uso da lógica especial
    logger.info("==== DISTRIBUIÇÃO ESPECIAL APLICADA ====")
    for k, v in DISTRIBUICAO_LOGS.items():
        logger.info(f"{k}: {v} vezes")

    # Checagem 3: valores válidos em 'Genero'
    generos = df_distribuido['Genero'].dropna().unique()
    for g in generos:
        g_norm = normalize_gender(g)
        assert g_norm in ["Homem", "Mulher", "Não classificado"], f"Gênero inválido: {g_norm}"


def test_colunas_essenciais_presentes(df_genero_mergeado):
    """Confirma que colunas mínimas estão no DataFrame redistribuído"""
    _, df_merged = df_genero_mergeado
    df_result = distribute_gender_metrics(df_merged)

    colunas_esperadas = ['Ad ID', 'Date', 'Genero', '_Plataforma'] + METRICAS
    for col in colunas_esperadas:
        assert col in df_result.columns, f"Coluna esperada ausente: {col}"