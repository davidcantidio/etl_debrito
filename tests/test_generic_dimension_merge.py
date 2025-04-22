import pytest
import pandas as pd
from utils.common.meta.age_placements_merge import (
    METRICAS,
    load_and_prepare_meta_age_data,
    load_and_prepare_meta_placement_data,
    pivot_meta_age_data,
    pivot_meta_placement_data,
    merge_placement_and_age_data,
    get_placements,
    compute_pesos_impressao,
    distribute_age_metrics,
    _floor_cents,
    _distribuir_cost,
    _distribuir_contagem
)

# ----------------------------
# TESTES BÁSICOS DE CONSTANTES
# ----------------------------

def test_metrics_constant():
    assert METRICAS == ["Impressions", "Link clicks", "Cost", "Video watches at 100%"]

# ----------------------------
# TESTES DE LEITURA E PREPARAÇÃO DE DADOS
# ----------------------------

def test_load_and_prepare_meta_age_data():
    df = load_and_prepare_meta_age_data()
    
    # Verificando se as colunas esperadas estão no DataFrame
    for col in METRICAS:
        assert col in df.columns
    
    # Verifique se os valores numéricos estão corretos após conversão
    assert df["Impressions"].dtype == float
    assert df["Cost"].dtype == float

def test_load_and_prepare_meta_placement_data():
    df = load_and_prepare_meta_placement_data()
    
    # Verificando se a coluna 'Ad ID' e 'Date' estão presentes
    assert "Ad ID" in df.columns
    assert "Date" in df.columns

# ----------------------------
# TESTES DE PIVOTAGEM DE DADOS
# ----------------------------

def test_pivot_meta_age_data():
    df = pd.DataFrame({
        "Ad ID": [1, 1, 2, 2],
        "Date": ["2025-01-01", "2025-01-01", "2025-01-02", "2025-01-02"],
        "Age": ["18-24", "25-34", "18-24", "25-34"],
        "Impressions": [1000, 2000, 1500, 2500],
        "Link clicks": [50, 100, 75, 125],
        "Cost": [10, 20, 15, 25],
    })
    
    result = pivot_meta_age_data(df)
    
    assert result.shape == (4, 5)  # Expecting 4 rows and 5 columns (Ad ID, Date, Age, Impressions, Link clicks)
    assert result["Age"].iloc[0] == "18-24"

def test_pivot_meta_placement_data():
    df = pd.DataFrame({
        "Ad ID": [1, 2],
        "Date": ["2025-01-01", "2025-01-02"],
        "Placement": ["Facebook", "Instagram"],
        "Impressions": [1000, 1500],
        "Link clicks": [100, 150],
        "Cost": [10, 15],
    })
    
    result = pivot_meta_placement_data(df)
    
    assert "Facebook_Impressions" in result.columns
    assert "Instagram_Impressions" in result.columns

# ----------------------------
# TESTES DE CÁLCULO DE PESOS
# ----------------------------

def test_compute_pesos_impressao():
    row = pd.Series({"Facebook_Impressions": 1000, "Instagram_Impressions": 500})
    placements = ["Facebook", "Instagram"]
    
    pesos, total = compute_pesos_impressao(row, placements)
    
    assert pesos == {"Facebook": 1000, "Instagram": 500}
    assert total == 1500

def test_compute_pesos_impressao_fallback():
    row = pd.Series({"Facebook_Impressions": 0, "Instagram_Impressions": 0})
    placements = ["Facebook", "Instagram"]
    
    pesos, total = compute_pesos_impressao(row, placements)
    
    assert pesos == {"Facebook": 1, "Instagram": 0}
    assert total == 1

# ----------------------------
# TESTES DE DISTRIBUIÇÃO DE MÉTRICAS
# ----------------------------

def test_distribute_age_metrics():
    df = pd.DataFrame({
        "Ad ID": [1, 1],
        "Date": ["2025-01-01", "2025-01-01"],
        "Age": ["18-24", "25-34"],
        "Impressions": [1000, 2000],
        "Cost": [10, 20],
        "Link clicks": [50, 100],
    })
    
    result = distribute_age_metrics(df)
    
    assert result.shape == (4, 6)  # Espera-se 4 linhas (2 para cada placement)
    assert result["Age"].iloc[0] == "18-24"

# ----------------------------
# TESTES AUXILIARES
# ----------------------------

def test_floor_cents():
    assert _floor_cents(1.239) == 1.23
    assert _floor_cents(5.0) == 5.00
    assert _floor_cents(3.14159) == 3.14

def test_distribuir_cost():
    pesos = {"Facebook": 60, "Instagram": 40}
    result = _distribuir_cost(100, pesos)
    assert abs(sum(result.values()) - 100) < 0.01

def test_distribuir_contagem():
    pesos = {"Facebook": 60, "Instagram": 40}
    result = _distribuir_contagem(100, pesos)
    assert sum(result.values()) == 100
