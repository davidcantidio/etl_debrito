# tests/test_meta_idade_etl.py

import pandas as pd
import pytest
from datetime import date

from scripts.etl_idade import MetaIdadeETL
from utils.fields_lists import AGE_MODEL_COLUMN_ORDER
from utils.common_meta import METRICAS

@pytest.fixture
def sample_meta_idade_df():
    # ... mesmo fixture que você já definiu, com colunas 'Age', 'Impressions', etc.
    return pd.DataFrame([row])

def test_meta_idade_etl_pipeline(sample_meta_idade_df):
    etl = MetaIdadeETL(
        df=sample_meta_idade_df(),
        id_veiculo=None,
        veiculo=None,
        mapping_campanha={},
        mapping_sigla={}
    )
    output = etl.processar(df_destino=pd.DataFrame())

    # validações de colunas
    expected_cols = ["Numero", "Data", "Nome_da_Conta", "ID_Veiculo", "Veiculo",
                     "ID_Campanha", "Campanha", "Nome_do_Conjunto_de_Anuncio",
                     "Nome_do_Anuncio", "Objetivo", "Idade"] + METRICAS + ["ID"]
    assert output.columns.tolist() == expected_cols

    # validação de somas por métrica
    for m in METRICAS:
        assert output[m].sum() == sample_meta_idade_df()[m].iloc[0]
