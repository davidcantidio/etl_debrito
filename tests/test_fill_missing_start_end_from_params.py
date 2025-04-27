# tests/test_fill_missing_start_end_from_params.py
import pandas as pd
import pytest
from utils.datas import fill_missing_start_end_from_params
from utils.get_google_client import get_google_client
from utils.google_sheets import SPREADSHEET_ID

from utils.substitute_origin_values import fill_missing_start_end_from_params

# -- stubs for Google Sheets client --
class FakeWSParam:
    def get_all_values(self):
        # first row can be anything, second row is our header, rest is data
        return [
            ["X", "Y", "Z"],  # dummy header row 1
            ["NOME CAMPANHA", "START", "END"],  # real header row 2
            ["Camp A", "2020-01-01", "2020-12-31"],
            ["Camp B", "",           "2021-06-30"],
        ]

class FakeSh:
    def worksheet(self, name):
        assert name == "BI_PARAMETRIZAÇÃO"
        return FakeWSParam()

class FakeClient:
    def open_by_key(self, key):
        return FakeSh()

@pytest.fixture(autouse=True)
def patch_google_client(monkeypatch):
    # patch the get_google_client in the substitute_origin_values module
    import utils.substitute_origin_values as mod
    monkeypatch.setattr(mod, "get_google_client", lambda *_: FakeClient())

def test_fill_both_start_and_end_when_missing():
    df_in = pd.DataFrame({
        "Campaign name": ["Camp A", "Camp B", "Camp C"],
        "Start":         ["",       "",       ""      ],
        "End":           ["",       "",       ""      ],
    })

    df_out = fill_missing_start_end_from_params(df_in, write_back=False, inplace=False)

    # Camp A: both start/end from mapping
    assert df_out.loc[0, "Start"] == "2020-01-01"
    assert df_out.loc[0, "End"]   == "2020-12-31"
    # Camp B: only End is mapped, Start remains blank
    assert df_out.loc[1, "Start"] == ""
    assert df_out.loc[1, "End"]   == "2021-06-30"
    # Camp C: not in mapping, both stay blank
    assert df_out.loc[2, "Start"] == ""
    assert df_out.loc[2, "End"]   == ""

def test_does_not_overwrite_existing_values():
    df_in = pd.DataFrame({
        "Campaign name": ["Camp A", "Camp B"],
        "Start":         ["2019-05-01", ""],
        "End":           [          "", ""],
    })

    df_out = fill_missing_start_end_from_params(df_in, write_back=False, inplace=False)

    # existing Start must be preserved
    assert df_out.loc[0, "Start"] == "2019-05-01"
    # End for Camp A comes from mapping
    assert df_out.loc[0, "End"] == "2020-12-31"
    # Camp B: only End filled
    assert df_out.loc[1, "Start"] == ""
    assert df_out.loc[1, "End"]   == "2021-06-30"
