import pytest
import re

import pandas as pd
import time
from treat.bi_param_utils import (
    BIParamLookup,
    get_campaign_parameterization,
    load_utm_mapping,
    determine_meta_ad_preview_link,
    generate_linkedin_ad_preview_link_from_lookup,
    build_pinterest_preview_link,
    generate_pinterest_ad_preview_link
)
from utils.google_sheets import CREDS_PATH, SPREADSHEET_ID



@pytest.fixture(scope="module")
def lookup():
    # instancia uma vez para todos os testes
    return BIParamLookup(CREDS_PATH, SPREADSHEET_ID)

@pytest.fixture(autouse=True)
def clear_cache(lookup):
    lookup._df = None
    lookup._last_load = 0




def test_campaign_and_utm_mapping_consistency(lookup):
    """
    Verifica que:
      1) as chaves (ad_name_raw upper) são as mesmas para campanha e UTM;
      2) nenhum valor está vazio;
      3) todos os UTMs aparecem no resultado de utm_start_end().
    """
    camp_map, utm_map = lookup.get_taxonomy_camp_name_and_id_from_ad_name()

    # 1) Mesmo conjunto de chaves
    assert set(camp_map.keys()) == set(utm_map.keys()), "Chaves de campanha e UTM divergem"

    # 2) Valores não vazios
    assert all(v.strip() for v in camp_map.values()), "Há campanhas vazias"
    assert all(v.strip() for v in utm_map.values()), "Há UTMs vazias"



def test_utm_start_end_format(lookup):
    """
    Verifica que utm_start_end() devolve dicionário
    com chaves 'start' e 'end' cujos valores estão no formato YYYY-MM-DD.
    """
    utm_dates = lookup.utm_start_end()
    iso_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}$")

    for utm_key, dates in utm_dates.items():
        assert "start" in dates and "end" in dates
        assert iso_pattern.match(dates["start"]), f"start inválido para {utm_key}: {dates['start']}"
        assert iso_pattern.match(dates["end"]),   f"end inválido   para {utm_key}: {dates['end']}"





def test_criativo_mapping_and_inverse_lookup(lookup):
    """
    get_criativo_mapping devolve {utm_content_lower: taxonomy_ad_name}
    e lookup_utm_for_ad_name inverte corretamente.
    """
    mapping = lookup.get_criativo_mapping()
    # Deve haver pelo menos um par válido
    assert mapping, "mapa criativo está vazio"

    # Testa alguns pares aleatórios (até 5)
    for utm, ad in list(mapping.items())[:5]:
        assert isinstance(utm, str) and utm.strip()
        assert isinstance(ad,  str) and ad.strip()
        assert lookup.lookup_utm_for_ad_name(ad) == utm


def test_fill_utm_content_from_ad_name(lookup):
    """
    fill_utm_content_from_ad_name deve preencher corretamente
    a coluna utm_content a partir de taxonomy_ad_name.
    """
    # prepara um pequeno DataFrame de teste
    criativo_map = lookup.get_criativo_mapping()
    # pega até 3 entradas válidas + 1 desconhecida
    sample_ads = list(criativo_map.values())[:3] + ["NÃO_EXISTE"]
    df = pd.DataFrame({
        "taxonomy_ad_name": sample_ads,
        "utm_content": [""] * len(sample_ads),
    })

    out = lookup.fill_utm_content_from_ad_name(
        df,
        coluna_ad_name="taxonomy_ad_name",
        coluna_destino="utm_content",
        write_back=False
    )


    for idx, ad in enumerate(sample_ads):
        # deve corresponder ao lookup inverso
        assert out.at[idx, "utm_content"] == lookup.lookup_utm_for_ad_name(ad)


def test_get_campaign_parameterization_wrapper():
    # Assegura que o wrapper devolve o mesmo que o método da classe
    lookup = BIParamLookup(CREDS_PATH, SPREADSHEET_ID)
    camp1, utm1 = lookup.get_campaign_maps()
    camp2, utm2 = get_campaign_parameterization(CREDS_PATH, SPREADSHEET_ID)
    assert camp1 == camp2
    assert utm1 == utm2 

def test_load_utm_mapping_wrapper():
    # Thin-wrapper para utm_start_end()
    lookup = BIParamLookup(CREDS_PATH, SPREADSHEET_ID)
    direct = lookup.utm_start_end()
    wrapped = load_utm_mapping(CREDS_PATH, SPREADSHEET_ID)
    assert direct == wrapped

def test_missing_columns_raise_keyerror(monkeypatch):
    # Simula planilha sem coluna utm_content → utm_start_end() deve KeyError
    fake_rows = [
        ["col1", "col2"],      # agora duas colunas de header
        ["foo",   "bar"],      # dados na linha 1
        ["A",     "B"],
    ]

    class WS: 
        def get_all_values(self): return fake_rows
    class SH:
        def worksheet(self, _): return WS()
    class CL:
        def open_by_key(self, _): return SH()
    monkeypatch.setattr("treat.utils.bi_param_utils.get_google_client", lambda p: CL())
    lookup = BIParamLookup("x","y")
    with pytest.raises(KeyError):
        lookup.utm_start_end()

def test_cache_ttl(monkeypatch):
    # Garante que _load_df só é chamado uma vez antes de expirar
    calls = {"count": 0}
    def fake_load(self):
        calls["count"] += 1
        return pd.DataFrame(
            [["A","B","C","d","2025-01-01","2025-01-02","X"]],
            columns=["taxonomy_ad_name","taxonomy_campaign_name","utm_campaign","utm_content","start","end","criativo"]
        )

    monkeypatch.setattr(BIParamLookup, "_load_df", fake_load)
    lookup = BIParamLookup("p","s")
    lookup.HEADER_ROW = 1

    # chama duas vezes sem avançar o tempo
    _ = lookup.get_campaign_maps()
    _ = lookup.get_campaign_maps()
    assert calls["count"] == 1

    # aqui a única mudança:
    orig_time = time.time
    monkeypatch.setattr(time, "time", lambda: orig_time() + lookup._TTL + 1)

    _ = lookup.get_campaign_maps()
    assert calls["count"] == 2

def test_determine_meta_preview_link():
    df = pd.DataFrame({
        "Preview Link FB": ["fb1","","fb3"],
        "Preview Link IG": ["ig1","ig2",""],
        "URL_do_Anuncio":  ["","preex",""]
    })
    out = determine_meta_ad_preview_link(df)
    assert out.at[0, "URL_do_Anuncio"] == "ig1"
    assert out.at[1,"URL_do_Anuncio"] == "preex"      
    assert out.at[2,"URL_do_Anuncio"] == "fb3"        

def test_generate_linkedin_and_pinterest_previews():
    # LinkedIn
    param_df = pd.DataFrame({
        "utm_content": ["u1","u2","u1"], "preview": ["p1", None, "p2"]
    })
    lk = generate_linkedin_ad_preview_link_from_lookup(param_df)
    assert lk == {"u1":"p2"}


    # Pinterest URL builder
    assert build_pinterest_preview_link("123") == "https://www.pinterest.com/pin/123"
    assert build_pinterest_preview_link("") == ""

    # Pinterest full function
    dforig = pd.DataFrame({"Preview Link": ["999",""]})
    outp = generate_pinterest_ad_preview_link(dforig)
    assert outp.at[0,"URL_do_Anúncio"] == "https://www.pinterest.com/pin/999"
    assert outp.at[1,"URL_do_Anúncio"] == ""

def test_fill_missing_start_end_from_utm_with_write_back(monkeypatch, lookup):
    # 1) Fake BI_PARAMETRIZAÇÃO cache: utm_content → start/end
    fake_map = {
        "u1": {"start": "2025-01-01", "end": "2025-01-31"},
        "u2": {"start": "2025-02-01", "end": "2025-02-28"},
    }
    monkeypatch.setattr(lookup, "utm_start_end", lambda: fake_map)

    # 2) Prepara DataFrame de origem:
    #    - linha 0: ambos vazios → deve preencher
    #    - linha 1: start já preenchido, end vazio → só end
    #    - linha 2: ambos preenchidos → não toca
    df = pd.DataFrame({
        "utm_content": ["u1", "u2", "u1"],
        "start":       ["",    "2025-02-XX", "2025-01-01"],
        "end":         ["",    "",           "2025-01-31"],
    })

    # 3) Dummy worksheet para capturar atualizações
    recorded: list = []
    class DummyWS:
        def row_values(self, _): 
            return ["utm_content", "start", "end"]
        def batch_update(self, updates, value_input_option):
            recorded.extend(updates)

    class DummySh:
        def worksheet(self, _): return DummyWS()
    class DummyCl:
        def open_by_key(self, _): return DummySh()

    # Monkeypatch do client
    monkeypatch.setattr(
        "treat.utils.bi_param_utils.get_google_client",
        lambda creds: DummyCl()
    )

    # 4) Executa
    out = lookup.fill_missing_start_end_from_utm(
        df,
        coluna_utm="utm_content",
        coluna_start="start",
        coluna_end="end",
        sheet_name="ignored",
        write_back=True
    )

    # 5) Verifica preenchimento in-memory
    assert out.at[0, "start"] == "2025-01-01"
    assert out.at[0, "end"]   == "2025-01-31"
    assert out.at[1, "start"] == "2025-02-XX"  # manteve o que já tinha
    assert out.at[1, "end"]   == "2025-02-28"
    assert out.at[2, "start"] == "2025-01-01"
    assert out.at[2, "end"]   == "2025-01-31"

    # 6) Verifica batch_update: só 3 células devem ter sido escritas
    #    linha 0 start (A2), linha 0 end (C2), linha 1 end (C3)
    ranges = {u["range"] for u in recorded}
    assert ranges == {"B2", "C2", "C3"}
    # e os valores batidos
    values_map = {u["range"]: u["values"][0][0] for u in recorded}
    assert values_map == {
        "B2": "2025-01-01",
        "C2": "2025-01-31",
        "C3": "2025-02-28",
    }
