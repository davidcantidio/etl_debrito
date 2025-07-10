"""
Testes unitários para ponto_de_controle.origin.read_origin_df
– O extract.read_df é “mockado” para evitar acesso a Sheets.
"""
from datetime import date

import pandas as pd
import pytest

from ponto_de_controle import origin


@pytest.fixture()
def fake_origin_df() -> pd.DataFrame:
    """DataFrame mínimo que satisfaz o pipeline."""
    return pd.DataFrame(
        {
            "date": ["2025-06-05", "2025-06-07"],
            "start": ["2025-06-05", "2025-06-07"],
            "end": ["2025-06-06", "2025-06-08"],
            "Campanha": ["ABC", "ABC"],
            "utm_content": ["c1", "c2"],
            "Veiculo": ["Instagram", "Instagram"],
        }
    )


def test_read_origin_df_filters_and_keeps_unique(monkeypatch, fake_origin_df):
    # 1) mocks --------------------------------------------------------------
    monkeypatch.setattr(
        origin, "read_df", lambda sheet_id, tab, header_row: fake_origin_df
    )
    monkeypatch.setattr(
        origin, "add_key_creative",
        lambda df: df.assign(key_creative=df["utm_content"]),
    )
    monkeypatch.setattr(origin, "dedupe_by_key_creative", lambda df: df)

    # 2) execução -----------------------------------------------------------
    df = origin.read_origin_df()

    # 3) asserções ----------------------------------------------------------
    assert not df.empty
    assert df["date"].min() >= str(origin.MIN_DATE)
    assert df["key_creative"].ne("").all()
