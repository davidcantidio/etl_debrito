# tests/test_extract_sheets_fetcher.py

import os
import pytest
import pandas as pd
from extract.sheets_fetcher import SheetsFetcher
import yaml
CONFIG_PATH = os.path.join(os.path.dirname(__file__), os.pardir, "sheets_config.yaml")

@pytest.fixture(scope="module")
def raw_config():
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)

def test_yaml_structure(raw_config):
    # valida seções mínimas
    expected_sections = {"meta", "tiktok", "linkedin"}
    assert expected_sections.issubset(raw_config.keys()), \
        f"Seções faltando em YAML: esperava {expected_sections}, achei {set(raw_config.keys())}"
    # cada seção deve listar strings não-vazias
    for section, sheets in raw_config.items():
        assert isinstance(sheets, list), f"Seção '{section}' deve ser lista"
        assert all(isinstance(s, str) and s for s in sheets), \
            f"Cada entrada em '{section}' deve ser string não-vazia"

def test_fetcher_respects_yaml_sections(raw_config, fetcher):
    # usando o mesmo fixture fetcher (SheetsFetcher) definido acima
    for section, sheets in raw_config.items():
        dfs = fetcher.get(sheets)
        assert set(dfs.keys()) == set(sheets), \
            f"Seção '{section}': esperei {sheets}, recebi {list(dfs.keys())}"
    # cache interno só deve conter abas do YAML
    all_requested = {s for sub in raw_config.values() for s in sub}
    assert set(fetcher._cache.keys()) == all_requested

@pytest.fixture(scope="module")
def creds_path() -> str:
    return os.getenv("GOOGLE_CREDS_PATH", "creds.json")

@pytest.fixture(scope="module")
def spreadsheet_id() -> str:
    sid = os.getenv("SPREADSHEET_ID")
    if not sid:
        pytest.skip("Defina SPREADSHEET_ID para rodar testes de integração.")
    return sid

@pytest.fixture(scope="module")
def fetcher(spreadsheet_id, creds_path):
    """Instância única de SheetsFetcher para todos os testes."""
    return SheetsFetcher(
        spreadsheet_id=spreadsheet_id,
        creds_path=creds_path,
        header_row=1,
        col_range="A:ZZ"
    )

def test_service_has_batch_get(fetcher):
    svc = fetcher._service
    # garante que o método batchGet existe
    assert hasattr(svc.spreadsheets().values(), "batchGet"), "batchGet não encontrado no service"

def test_get_returns_dataframes(fetcher):
    sheets = ["metaGeral", "tiktokGeral", "linkedinGeral"]
    dfs = fetcher.get(sheets, as_frame=True)
    # chaves corretas e tipos
    assert set(dfs.keys()) == set(sheets)
    for name, df in dfs.items():
        assert isinstance(df, pd.DataFrame)
        assert df.columns.size > 0, f"Aba '{name}' retornou DataFrame sem colunas"

def test_cache_prevents_second_call(monkeypatch, fetcher):
    call_count = {"n": 0}
    # wrap _fetch_batch para contar invocações
    orig = fetcher._fetch_batch
    def spy_fetch(names):
        call_count["n"] += 1
        return orig(names)
    monkeypatch.setattr(fetcher, "_fetch_batch", spy_fetch, raising=True)

    sheets = ["metaGeral", "tiktokGeral"]
    fetcher._cache.clear()
    _ = fetcher.get(sheets)
    _ = fetcher.get(sheets)

    assert call_count["n"] == 1, f"Esperava 1 invocação de _fetch_batch, mas foram {call_count['n']}"

def test_as_frame_false(fetcher):
    sheets = ["metaGeral"]
    raw = fetcher.get(sheets, as_frame=False)
    assert isinstance(raw, dict) and "metaGeral" in raw
    assert isinstance(raw["metaGeral"], list)
    # primeiro elemento deve ser lista de headers
    headers = raw["metaGeral"][0]
    assert isinstance(headers, list) and all(isinstance(h, str) for h in headers)

def test_refresh(fetcher):
    sheet = "tiktokGeral"
    # garante que já está em cache
    _ = fetcher.get([sheet])
    # monkeypatcha o cache para valor diferente
    fetcher._cache[sheet] = pd.DataFrame({"x":[1,2,3]})
    # refresh faz refetch real e substitui cache
    fetcher.refresh([sheet])
    df = fetcher.get([sheet])[sheet]
    assert not df.equals(pd.DataFrame({"x":[1,2,3]})), "refresh não atualizou o cache"

def test_empty_sheet_returns_empty(fetcher):
    # assegure que exista uma aba "googleIdade" na planilha
    empty = fetcher.get(["googleIdade"])["googleIdade"]
    print(f"\nℹ️ Aba 'googleIdade': {empty.shape}")
    assert isinstance(empty, pd.DataFrame)
    assert empty.empty

def test_unique_columns(fetcher):
    sheets = ["metaGeral", "tiktokGeral", "linkedinGeral"]
    dfs = fetcher.get(sheets)
    for name, df in dfs.items():
        cols = list(df.columns)
        dup = {c for c in cols if cols.count(c) > 1}
        print(f"\nℹ️ Colunas duplicadas em '{name}': {dup}")
        assert not dup, f"Aba '{name}' tem colunas duplicadas: {dup}"
