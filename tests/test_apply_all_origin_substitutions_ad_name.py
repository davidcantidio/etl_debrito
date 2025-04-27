# tests/test_apply_all_origin_substitutions_ad_name.py

import pytest
import pandas as pd
import logging

from utils.substitute_origin_values import apply_all_origin_substitutions
from utils.substitutions_lists import AD_NAME_REPLACEMENTS

@pytest.mark.integration
@pytest.mark.origin_subst
def test_apply_substitution_on_ad_name(caplog):
    """
    Testa se as substituições definidas para o campo 'Ad name' são corretamente aplicadas
    e que o DataFrame resultante reflete essas mudanças.
    """
    caplog.set_level(logging.DEBUG)

    if not AD_NAME_REPLACEMENTS:
        pytest.skip("Nenhuma substituição definida em AD_NAME_REPLACEMENTS")

    # pega uma chave de exemplo do mapeamento
    original_ad_name, new_ad_name = next(iter(AD_NAME_REPLACEMENTS.items()))

    # normaliza o valor para simular o que o ETL faz
    original_ad_name_normalized = original_ad_name.strip().lower()

    # cria um DataFrame de entrada simulando a aba de origem
    df_in = pd.DataFrame({
        "Ad name": [original_ad_name_normalized],
        "Other": ["valor_irrelevante"],
    })

    # aplica as substituições SEM gravação no Sheets
    df_out = apply_all_origin_substitutions(
        df_in,
        sheet_name="metaGeral",  # necessário mas ignorado no write_back=False
        write_back=False,
        inplace=False,
    )

    print(f"Original: {original_ad_name_normalized} | Esperado: {new_ad_name} | Obtido: {df_out.loc[0, 'Ad name']}")

    # verifica se substituiu corretamente
    assert df_out.loc[0, "Ad name"] == new_ad_name
