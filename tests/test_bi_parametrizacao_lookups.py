import pytest
import pandas as pd
from utils.lookups_bi_parametrizacao import BIParamLookup

# Prepare a dummy DataFrame for testing
@pytest.fixture(autouse=True)
def dummy_biparam(monkeypatch):
    # Sample rows: header keys in lowercase
    data = [
        {
            "ad name": "Creative A",
            "campanha": "Camp X",
            "sigla": "CX",
            "utm_content": "utm_a",
            "start": "2025-01-01",
            "end": "2025-01-31",
            "criativo": "Creative A"
        },
        {
            "ad name": "Creative B",
            "campanha": "Camp Y",
            "sigla": "CY",
            "utm_content": "utm_b",
            "start": "2025-02-01",
            "end": "2025-02-28",
            "criativo": "Creative B"
        }
    ]
    df = pd.DataFrame(data)
    # Monkeypatch load to return our dummy df
    monkeypatch.setattr(BIParamLookup, '_load_df', lambda self: df)
    return df

@pytest.fixture()
def lookup():
    # creds and id don't matter since load is patched
    return BIParamLookup('creds.json', 'sheet-id')

def test_find_col_caches_and_finds(lookup):
    # First call should populate cache
    col = lookup._find_col('campanha')
    assert col == 'campanha'
    # Cached
    lookup._df = pd.DataFrame()  # clear df to ensure no reload needed for cache
    cached = lookup._find_col('campanha')
    assert cached == 'campanha'

def test_map_columns_key_and_vals(lookup):
    mapping = lookup._map_columns('ad name', ['campanha', 'sigla'], upper_keys=True)
    assert mapping['CREATIVE A'] == ('Camp X', 'CX')
    assert mapping['CREATIVE B'] == ('Camp Y', 'CY')

def test_get_taxonomy_camp_name_and_id(lookup):
    m_name, m_id = lookup.get_taxonomy_camp_name_and_id_from_ad_name()
    assert m_name['CREATIVE A'] == 'Camp X'
    assert m_id['CREATIVE B'] == 'CY'

def test_utm_start_end(lookup):
    out = lookup.utm_start_end()
    assert out['utm_a']['Start'] == '2025-01-01'
    assert out['utm_b']['End'] == '2025-02-28'

def test_get_criativo_mapping(lookup):
    mapping = lookup.get_criativo_mapping()
    assert mapping['utm_a'] == 'Creative A'
    assert mapping['utm_b'] == 'Creative B'

def test_lookup_utm_for_ad_name(lookup):
    assert lookup.lookup_utm_for_ad_name('Creative A') == 'utm_a'
    assert lookup.lookup_utm_for_ad_name('Nonexistent') == ''
