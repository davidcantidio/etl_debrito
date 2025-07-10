"""
Testes unitários para ponto_de_controle.transform.transform_df
Verifica layout, NaNs e unicidade de __ID__.
"""
import pandas as pd

from ponto_de_controle import transform, constants

DF_IN = pd.DataFrame(
    {
        "start": ["2025-06-05"],
        "end": ["2025-06-10"],
        "Veiculo": ["Instagram"],
        "URL_do_Anuncio": ["https://xpto"],
        "Campanha": ["ABC"],
        "objective": ["Tráfego"],
    }
)


def test_transform_df_layout_and_id_uniqueness():
    df_t = transform.transform_df(DF_IN)

    expected_cols = constants.DEST_COLUMNS + ["__ID__"]
    assert list(df_t.columns) == expected_cols
    assert df_t.isna().sum().sum() == 0
    assert df_t["__ID__"].is_unique
