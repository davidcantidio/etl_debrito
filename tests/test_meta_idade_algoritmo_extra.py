# tests/test_meta_idade_algoritmo_extra.py

import logging
from collections import Counter
from datetime import date
from math import floor
from decimal import Decimal, ROUND_DOWN, getcontext

import pandas as pd
import numpy as np
import pytest

from utils.common_meta import (
    METRICAS,
    get_placements,
    compute_pesos_impressao,
    _sanitize_metric_value,
    _special_distribution,
    _floor_cents,
    _distribute_proportional,
    DISTRIBUICAO_LOGS,
)
from datetime import date

from utils.common_meta import _fix_inconsistencies_and_types
from utils.common_meta import distribute_age_metrics

# Configura logger para verificação manual, se desejado
logging.basicConfig(level=logging.DEBUG, format="%(levelname)s: %(message)s")


# ----------------------------------------------------------------------
# 1) Constante METRICAS e get_placements
# ----------------------------------------------------------------------
def test_metrics_constant():
    esperado = [
        "Impressions",
        "Link clicks",
        "Cost",
        "Video watches at 100%",
    ]
    assert METRICAS == esperado, f"METRICAS incorreta: {METRICAS}"


def test_get_placements_basic():
    df = pd.DataFrame(
        columns=[
            "Ad ID",
            "Date",
            "instagram_story_Impressions",
            "facebook_feed_Impressions",
            "instagram_story_Cost",  # não deve interferir
            "facebook_feed_Cost",
        ]
    )

    resultado = get_placements(df)
    esperado = ["facebook_feed", "instagram_story"]
    assert resultado == esperado, f"Esperado {esperado}, obtido {resultado}"


def test_get_placements_handles_duplicates_and_noise():
    df = pd.DataFrame(
        columns=[
            "X_Impressions",               # placement X
            "X_Impressions_duplicate",     # ruído – não termina exatamente com _Impressions
            "Y_Impressions",               # placement Y
            # duplicado intencional: pandas já ignora repetição no index
            "Z_Something",                 # ignorado
        ]
    )
    esperado = ["X", "Y"]
    assert get_placements(df) == esperado


# ----------------------------------------------------------------------
# 2) compute_pesos_impressao
# ----------------------------------------------------------------------
def _make_dummy_row(values: dict) -> pd.Series:
    """Facilitador que devolve uma Series com name=0."""
    return pd.Series(values, name=0)


def test_pesos_basico():
    row = _make_dummy_row({"A_Impressions": 100, "B_Impressions": 50})
    pesos, total = compute_pesos_impressao(row, ["A", "B"])
    assert pesos == {"A": 100, "B": 50}
    assert total == 150


def test_pesos_negativos_zerados():
    row = _make_dummy_row({"A_Impressions": -10, "B_Impressions": 20})
    pesos, total = compute_pesos_impressao(row, ["A", "B"])
    assert pesos == {"A": 0, "B": 20}
    assert total == 20


def test_pesos_todos_zero_forca_ficticio():
    # “B” tem maior Link clicks
    row = _make_dummy_row({
        "A_Impressions": 0,
        "B_Impressions": 0,
        "C_Impressions": 0,
        "A_Link clicks": 2,
        "B_Link clicks": 10,
        "C_Link clicks": 5,
    })
    pesos, total = compute_pesos_impressao(row, ["A", "B", "C"])
    assert total == 1
    assert pesos["B"] == 1
    assert all(v in (0, 1) for v in pesos.values())


# ----------------------------------------------------------------------
# 3) Sanitização e caso especial
# ----------------------------------------------------------------------
def test_sanitize_negative_and_none():
    assert _sanitize_metric_value("Impressions", -5) == 0
    assert _sanitize_metric_value("Cost", -2.34) == 0.0
    assert _sanitize_metric_value("Link clicks", None) == 0


def test_sanitize_rounding_and_cost():
    assert _sanitize_metric_value("Impressions", 10.7) == 11
    assert _sanitize_metric_value("Link clicks", 10.2) == 10
    v_cost = _sanitize_metric_value("Cost", 12.3456)
    assert isinstance(v_cost, float) and np.isclose(v_cost, 12.3456)


def test_special_distribution_all_to_heaviest():
    P = ["A", "B", "C"]
    peso = {"A": 70, "B": 20, "C": 10}
    dist, usado = _special_distribution("Impressions", 2, P, peso)
    assert usado
    assert dist == {"A": 2, "B": 0, "C": 0}


def test_special_distribution_one_each():
    P = ["A", "B", "C"]
    peso = {"A": 10, "B": 10, "C": 10}
    dist, usado = _special_distribution("Link clicks", 3, P, peso)
    assert usado
    assert dist == {"A": 1, "B": 1, "C": 1}


def test_special_distribution_not_applicable():
    P = ["A", "B"]
    peso = {"A": 50, "B": 50}
    dist1, usado1 = _special_distribution("Cost", 1, P, peso)
    dist2, usado2 = _special_distribution("Impressions", 10, P, peso)
    assert dist1 is None and not usado1
    assert dist2 is None and not usado2


def test_special_counter_increment():
    DISTRIBUICAO_LOGS.clear()
    P = ["A", "B"]
    peso = {"A": 60, "B": 40}
    _special_distribution("Video watches at 100%", 1, P, peso)
    assert DISTRIBUICAO_LOGS["Video watches at 100%_especial"] == 1


# ----------------------------------------------------------------------
# 4) Distribuição proporcional
# ----------------------------------------------------------------------
def test_floor_cents():
    assert _floor_cents(1.239) == 1.23
    assert _floor_cents(0) == 0.0
    assert _floor_cents(5.2) == 5.20


def test_proportional_counts_integrity():
    P = ["A", "B", "C"]
    peso = {"A": 70, "B": 20, "C": 10}
    total = 123
    dist = _distribute_proportional("Impressions", total, P, peso)
    assert sum(dist.values()) == total
    assert all(isinstance(v, int) for v in dist.values())


def test_proportional_counts_largest_remainder():
    P = ["A", "B"]
    peso = {"A": 80, "B": 20}
    total = 157
    dist = _distribute_proportional("Link clicks", total, P, peso)
    ideal_A = round(total * 0.80)
    ideal_B = total - ideal_A
    assert abs(dist["A"] - ideal_A) <= 1
    assert abs(dist["B"] - ideal_B) <= 1


def test_proportional_cost_two_decimals():
    P = ["A", "B"]
    peso = {"A": 60, "B": 40}
    valor = 10.03
    dist = _distribute_proportional("Cost", valor, P, peso)
    # soma preservada e truncada em 2 casas
    assert round(sum(dist.values()), 2) == valor
    # cada parcela deve ser truncada para centavos exatos
    assert all(_floor_cents(v) == v for v in dist.values())


def test_zero_value_or_zero_weights():
    P = ["A", "B"]
    zero_weights = {"A": 0, "B": 0}
    dist1 = _distribute_proportional("Impressions", 0, P, zero_weights)
    dist2 = _distribute_proportional("Cost", 5.00, P, zero_weights)
    assert dist1 == {"A": 0, "B": 0}
    assert dist2 == {"A": 0, "B": 0}

def test_fix_inconsistency_logs_and_keeps_value(caplog):
    caplog.set_level(logging.CRITICAL, logger="common_meta.fix")
    metric = "Impressions"
    dist   = {"A": 5, "B": 0}
    peso   = {"A": 0, "B": 0}              # pesos incoerentes

    out = _fix_inconsistencies_and_types(metric, dist, peso)

    # valor permaneceu
    assert out == {"A": 5, "B": 0}
    # houve log crítico
    assert any(
        rec.levelno == logging.CRITICAL and "peso_imp é 0" in rec.message
        for rec in caplog.records
    )

def test_fix_types_for_cost_and_counts():

    # Cost → duas casas
    dist_cost = {"A": 3.456, "B": 1.111}
    peso_ok   = {"A": 10, "B": 5}
    out_cost  = _fix_inconsistencies_and_types("Cost", dist_cost, peso_ok)
    assert all(isinstance(v, float) for v in out_cost.values())
    assert all(v == _floor_cents(v) for v in out_cost.values())

    # Counts → int
    dist_cnt  = {"A": 2.7, "B": 0.2}
    out_cnt   = _fix_inconsistencies_and_types("Link clicks", dist_cnt, peso_ok)
    assert all(isinstance(v, int) for v in out_cnt.values())
    assert out_cnt == {"A": 3, "B": 0}


def test_build_output_rows_and_integrity():
    """
    Fluxo ‘end‑to‑end’ para uma linha:

    • 2 placements
    • métricas misturadas (Cost com centavos)
    • garante:
        – quantidade de linhas == len(P)
        – soma das métricas preservada
        – colunas‑chave presentes
    """
    # -------- cria df_merged dummy (pivô já agregado) -----------------
    df = pd.DataFrame({
        "Ad ID": ["AD123"],
        "Date": [date(2025, 1, 1)],
        "Age": ["18-24"],

        # métricas totais (colunas 'nuas')
        "Impressions": [1000],
        "Link clicks": [123],
        "Cost": [10.03],
        "Video watches at 100%": [17],

        # pesos por placement (Impressions)
        "A_Impressions": [600],
        "B_Impressions": [400],

        # colunas auxiliares (mesmas métricas por placement)
        "A_Impressions": [600], "B_Impressions": [400],
        "A_Link clicks": [0],   "B_Link clicks": [0],
        "A_Cost": [0],          "B_Cost": [0],
        "A_Video watches at 100%": [0], "B_Video watches at 100%": [0],
    })

    df_out = distribute_age_metrics(df)

    # -------- validações ---------------------------------------------
    assert len(df_out) == 2, "Devem existir duas linhas (uma por placement)."
    assert set(df_out["_Plataforma"]) == {"A", "B"}

    # soma preservada p/ cada métrica
    for m in METRICAS:
        total_saida = df_out[m].sum()
        total_entrada = df[m].iloc[0]
        if m == "Cost":
            assert np.isclose(total_saida, total_entrada, atol=0.01)
        else:
            assert total_saida == total_entrada

    # colunas‑chave
    obrig = {"Ad ID", "Date", "Age", "_Plataforma"} | set(METRICAS)
    assert obrig.issubset(df_out.columns)

def test_multiple_input_rows_output_count_and_sums():
    # Duas linhas de input, mesmos placements ["A","B"]
    rows = [
        {
            "Ad ID": "AD1", "Date": date(2025,1,1), "Age": "18-24",
            "A_Impressions": 100, "B_Impressions": 50,
            "A_Link clicks": 20,  "B_Link clicks": 10,
            "A_Cost":  3.00,      "B_Cost":  2.00,
            "A_Video watches at 100%": 5, "B_Video watches at 100%": 5,
        },
        {
            "Ad ID": "AD2", "Date": date(2025,1,2), "Age": "25-34",
            "A_Impressions": 200, "B_Impressions": 100,
            "A_Link clicks": 40,  "B_Link clicks": 20,
            "A_Cost":  6.00,      "B_Cost":  4.00,
            "A_Video watches at 100%": 10, "B_Video watches at 100%": 10,
        },
    ]
    df_merged = pd.DataFrame(rows)
    df_out = distribute_age_metrics(df_merged)

    P = get_placements(df_merged)
    # 2 linhas * 2 placements = 4
    assert len(df_out) == len(rows) * len(P)

    # verifique soma por bloco de 2 linhas
    for i, orig in enumerate(rows):
        block = df_out.iloc[i*len(P):(i+1)*len(P)]
        for m in METRICAS:
            original_total = sum(orig[f"{pl}_{m}"] for pl in P)
            assert block[m].sum() == original_total, (
                f"Soma de {m} no bloco {i} não preservou: "
                f"{block[m].sum()} vs {original_total}"
            )
def test_deterministic_platform_order():
    """
    Mesmo quando todas as cotas são iguais, a ordem das linhas
    para cada bloco deve seguir a ordem alfabética de placements.
    """
    row = {
        "Ad ID": "ADX", "Date": date(2025,1,3), "Age": "35-44",
        "A_Impressions": 10, "B_Impressions": 10, "C_Impressions": 10,
        **{f"{pl}_{m}": 1 for pl in ["A","B","C"] for m in METRICAS},
    }
    df_merged = pd.DataFrame([row])
    df_out = distribute_age_metrics(df_merged)

    P = get_placements(df_merged)
    assert list(df_out["_Plataforma"]) == P, "Ordem de _Plataforma não segue get_placements"

def test_columns_and_index():
    """
    Verifica que o DataFrame de saída tem exatamente as colunas
    esperadas e índice resetado 0..N-1.
    """
    row = {
        "Ad ID": "ADY", "Date": date(2025,1,4), "Age": "45-54",
        "A_Impressions": 5, "B_Impressions": 5,
        **{f"{pl}_{m}": 0 for pl in ["A","B"] for m in METRICAS},
    }
    df_merged = pd.DataFrame([row])
    df_out = distribute_age_metrics(df_merged)

    expected_cols = ["Ad ID","Date","Age","_Plataforma"] + METRICAS
    assert df_out.columns.tolist() == expected_cols
    # índice sequencial
    assert list(df_out.index) == list(range(len(df_out)))


@pytest.mark.parametrize("age_value", [None, "", "unknown", "others", "none", "Não classificado"])
def test_age_missing_variants_preserved(age_value):
    row = {
        "Ad ID": "ADZ", "Date": date(2025,1,5), "Age": age_value,
        "A_Impressions": 10, "B_Impressions": 0,
        **{f"{pl}_{m}": 0 for pl in ["A","B"] for m in METRICAS},
    }
    df_merged = pd.DataFrame([row])
    df_out = distribute_age_metrics(df_merged)

    if age_value is None:
        # pandas converte None em NaN
        assert df_out["Age"].isnull().all()
    else:
        assert all(df_out["Age"] == age_value), f"Age foi alterado: esperado {age_value}"

def test_extraneous_columns_filtered():
    """
    Se df_merged tiver colunas extras, elas não aparecem no output.
    """
    row = {
        "Ad ID": "ADX", "Date": date(2025,1,6), "Age": "18-24",
        "A_Impressions": 1, "B_Impressions": 1,
        **{f"{pl}_{m}": 0 for pl in ["A","B"] for m in METRICAS},
        "Campaign": "X", "ExtraInfo": 123,
    }
    df_merged = pd.DataFrame([row])
    df_out = distribute_age_metrics(df_merged)

    # não devem existir essas colunas
    for extra in ("Campaign","ExtraInfo"):
        assert extra not in df_out.columns



def test_global_sum_preservation():
    """A soma total de cada métrica deve ser idêntica antes e depois."""
    rows = []
    for i in range(5):
        rows.append({
            "Ad ID": f"AD{i}",
            "Date": date(2025, 1, i+1),
            "Age": "18-24",
            "A_Impressions": 100+i,
            "B_Impressions": 50+i,
            "A_Link clicks": 10+i,
            "B_Link clicks": 5+i,
            "A_Cost":  1.00 + i,
            "B_Cost":  0.50 + i,
            "A_Video watches at 100%": 2+i,
            "B_Video watches at 100%": 1+i,
        })
    df_in  = pd.DataFrame(rows)
    df_out = distribute_age_metrics(df_in)

    for m in METRICAS:
        tol = 0.01 if m == "Cost" else 0
        assert abs(df_in[m].sum() - df_out[m].sum()) <= tol, f"Soma de {m} alterada"


def test_deterministic_output_hash():
    """Executar duas vezes com mesmo input deve entregar DataFrames idênticos."""
    row = {
        "Ad ID": "DDET", "Date": date(2025, 2, 1), "Age": "25-34",
        "A_Impressions": 80, "B_Impressions": 20,
        **{f"{pl}_{m}": 0 for pl in ["A","B"] for m in METRICAS},
    }
    df_in = pd.DataFrame([row])
    out1 = distribute_age_metrics(df_in)
    out2 = distribute_age_metrics(df_in)
    pd.testing.assert_frame_equal(out1, out2, check_like=True)