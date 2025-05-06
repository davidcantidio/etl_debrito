import pytest
import pandas as pd

from treat.utils.preprocess_utils import (
    normalize_region_column,
    apply_origin_substitutions,
    preprocess_origin,
)
from treat.utils.geo_normalize import normalize_region
from treat.utils.substitute_origin_values import apply_all_origin_substitutions

# Exemplos reais de região para inspeção manual
VALORES_PARA_TESTE = [
    "Acre (state)",
    "Rio de Janeiro (state)",
    "Unknown",
    "São Paulo (state)",
    "Federal District",
    "Sao Paulo",
    "Amapa",
    "Para",
    "-1",
    "BR-OTHER",
    "Greater Cuiaba",
    "Brazil: Maranhao",
    "Greater São Paulo Area",
    "Brazil: Sao Paulo",
    "Greater Richmond Region",
    "London Area, United Kingdom",
    "Los Angeles Metropolitan Area",
    "Greater Ribeirão Preto",
    "Greater São Luís Area",
    "Charlotte Metro",
    "Bogotá D.C. Metropolitan Area",
    "Greater Belem",
    "Vitoria, Brazil Metropolitan Area",
    "State of Piaui"
]

def test_show_region_normalization(capsys):
    """
    Imprime cada par raw → normalizado.
    Rode com `pytest -s` para verificar manualmente.
    """
    for raw in VALORES_PARA_TESTE:
        norm = normalize_region(raw)
        print(f"{raw!r} -> {norm!r}")
    # sem asserts, apenas inspeção

def test_normalize_region_column_uses_normalize_region(monkeypatch):
    df = pd.DataFrame({"region": ["x", "y"], "foo": [1, 2]})
    # forçar normalize_region
    monkeypatch.setattr(
        "treat.utils.preprocess_utils.normalize_region",
        lambda s: f"ZZ[{s}]"
    )
    out = normalize_region_column(df, col_name="region")
    assert out["region"].tolist() == ["ZZ[x]", "ZZ[y]"]
    assert out["foo"].tolist() == [1, 2]

    # se não existir a coluna, retorna o mesmo objeto
    df2 = pd.DataFrame({"a": [1]})
    assert normalize_region_column(df2, "region") is df2

def test_apply_origin_substitutions_idempotent_and_shape():
    df = pd.DataFrame({"a": ["aa", "bb"], "b": [1, 2]})
    out1 = apply_origin_substitutions(df)
    assert isinstance(out1, pd.DataFrame) and out1.shape == df.shape
    # idempotência
    out2 = apply_origin_substitutions(out1)
    pd.testing.assert_frame_equal(out2, out1)

def test_preprocess_origin_combines_steps(monkeypatch):
    df = pd.DataFrame({"region": ["foo"], "x": ["bar"]})
    monkeypatch.setattr(
        "treat.utils.preprocess_utils.apply_origin_substitutions",
        lambda df: df.assign(x="XX")
    )
    monkeypatch.setattr(
        "treat.utils.preprocess_utils.normalize_region_column",
        lambda df, col_name: df.assign(region="YY")
    )
    out = preprocess_origin(df)
    assert out.at[0, "x"] == "XX"
    assert out.at[0, "region"] == "YY"

def test_apply_all_origin_substitutions_write_back_single_column(monkeypatch):
    # força mapping em campaign_name
    monkeypatch.setattr(
        "treat.utils.substitute_origin_values.CAMPAIGN_NAME_REPLACEMENTS",
        {"foo": "BAR"}
    )
    df = pd.DataFrame({
        "campaign_name": ["foo", "baz"],
        "other": [1, 2]
    })
    recorded = []
    class DummyWS:
        def row_values(self, _): 
            return ["other", "campaign_name"]
        def batch_update(self, updates, **kw):
            recorded.extend(updates)
    class DummySh:
        def worksheet(self, _): return DummyWS()
    class DummyCl:
        def open_by_key(self, _): return DummySh()
    monkeypatch.setattr(
        "treat.utils.substitute_origin_values.get_google_client",
        lambda creds: DummyCl()
    )

    out = apply_all_origin_substitutions(
        df,
        sheet_name="ignored",
        write_back=True,
        inplace=False
    )
    # memória
    assert out.loc[0, "campaign_name"] == "BAR"
    assert out.loc[1, "campaign_name"] == "baz"
    # write-back: só B2
    assert recorded == [{"range": "B2", "values": [["BAR"]]}]

def test_apply_all_origin_substitutions_write_back_multiple_columns(monkeypatch):
    # força mapping em utm_content e ad_group_name
    monkeypatch.setattr(
        "treat.utils.substitute_origin_values.ID_CONTENT_REPLACEMENTS",
        {"u1": "U1NEW"}
    )
    monkeypatch.setattr(
        "treat.utils.substitute_origin_values.AD_GROUP_NAME_REPLACEMENTS",
        {"g1": "G1NEW"}
    )
    df = pd.DataFrame({
        "utm_content": ["u1", "x"],
        "ad_group_name": ["g1", "y"],
        "other": [0, 0]
    })
    recorded = []
    class DummyWS:
        def row_values(self, _):
            return ["utm_content", "ad_group_name", "other"]
        def batch_update(self, updates, **kw):
            recorded.extend(updates)
    class DummySh:
        def worksheet(self, _): return DummyWS()
    class DummyCl:
        def open_by_key(self, _): return DummySh()
    monkeypatch.setattr(
        "treat.utils.substitute_origin_values.get_google_client",
        lambda creds: DummyCl()
    )

    out = apply_all_origin_substitutions(
        df,
        sheet_name="ignored",
        write_back=True,
        inplace=False
    )
    # memória
    assert out.loc[0, "utm_content"]   == "U1NEW"
    assert out.loc[0, "ad_group_name"] == "G1NEW"
    assert out.loc[1, "utm_content"]   == "x"
    assert out.loc[1, "ad_group_name"] == "y"
    # write-back: A2 e B2
    assert {"range": "A2", "values": [["U1NEW"]]} in recorded
    assert {"range": "B2", "values": [["G1NEW"]]} in recorded
    assert len(recorded) == 2
