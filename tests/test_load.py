import pandas as pd
import pytest
import load as l

# Funções do módulo load
fetch_data = l.fetch_data
prepare_new_records = l.prepare_new_records
append_new_records = l.append_new_records
load_missing_records = l.load_missing_records
get_missing_records = l.get_missing_records
from load.utils.validate_impressions_consistency import validate_impressions_consistency

class FakeFetcher:
    """
    Mock de SheetsFetcher: retorna DataFrames pré-definidos.
    """
    def __init__(self, data_map):
        self.data_map = data_map

    def get(self, sheet_names, as_frame=True):
        return {name: self.data_map[name] for name in sheet_names}


def test_fetch_data_returns_correct_data():
    df1 = pd.DataFrame({'A': [1]})
    df2 = pd.DataFrame({'B': [2]})
    fetcher = FakeFetcher({'Origem': df1, 'Destino': df2})
    src, dst = fetch_data(fetcher, 'Origem', 'Destino')
    pd.testing.assert_frame_equal(src, df1)
    pd.testing.assert_frame_equal(dst, df2)


@pytest.mark.parametrize(
    "df_src,df_dest,expected_ids",
    [
        (pd.DataFrame({'ID': ['a']}), pd.DataFrame(), ['a']),                # destino vazio
        (pd.DataFrame({'ID': ['x', 'x']}), pd.DataFrame({'ID': ['x']}), ['x']), # duplicata
    ]
)
def test_prepare_new_records_various(df_src, df_dest, expected_ids):
    new = prepare_new_records(df_src, df_dest)
    assert sorted(new['ID'].tolist()) == expected_ids


def test_numero_sequencial_and_holes():
    df_dest = pd.DataFrame({'ID': ['a', 'b'], 'Numero': [1, 5]})
    df_src = pd.DataFrame({'ID': ['c']})
    new = prepare_new_records(df_src, df_dest)
    assert new['Numero'].iloc[0] == 6


def test_append_new_records_calls_appender():
    spy_calls = []

    def spy_appender(sheet, df):
        spy_calls.append((sheet, df.copy()))

    df_new = pd.DataFrame({'ID': ['1', '2']})
    append_new_records(spy_appender, 'SheetX', df_new)
    assert spy_calls[0][0] == 'SheetX'
    pd.testing.assert_frame_equal(spy_calls[0][1], df_new)

    spy_calls.clear()
    append_new_records(spy_appender, 'SheetX', pd.DataFrame())
    assert spy_calls == []


def test_load_missing_records_end_to_end(monkeypatch, caplog):
    caplog.set_level('INFO', logger='load')
    df_src = pd.DataFrame({'ID': ['a', 'b'], 'X': [10, 20]})
    df_dest = pd.DataFrame({'ID': ['a'], 'X': [10]})
    fetcher = FakeFetcher({'Origem': df_src, 'Destino': df_dest})

    spy = []

    def spy_appender(sheet, df):
        spy.append((sheet, df.copy()))

    # Usa lógica real
    monkeypatch.setattr(l, 'get_missing_records', get_missing_records)

    load_missing_records(
        spreadsheet_id='id',
        creds_path='creds',
        origem_sheet='Origem',
        destino_sheet='Destino',
        fetcher=fetcher,
        appender=spy_appender
    )

    assert len(spy) == 1
    sheet, df_passed = spy[0]
    assert sheet == 'Destino'
    assert df_passed['ID'].tolist() == ['b']
    assert df_passed['Numero'].tolist() == [1]
    assert 'Inserindo' in caplog.text


def test_validate_impressions_consistency_passes():
    """Valida que a função de consistência não levanta erro quando as somas batem."""
    
    # DataFrames mockados
    age = pd.DataFrame({'Veiculo': ['Facebook', 'Instagram'], 'Impressoes': [100, 200]})
    gender = pd.DataFrame({'Veiculo': ['Facebook', 'Instagram'], 'Impressoes': [150, 250]})
    region = pd.DataFrame({'Veiculo': ['Facebook', 'Instagram'], 'Impressoes': [120, 180]})
    meta = pd.DataFrame({'Veiculo': ['Facebook', 'Instagram'], 'Impressoes': [370, 630]})

    # FakeFetcher devolvendo as abas
    ff = FakeFetcher({
        'metaAge': age,
        'metaGender': gender,
        'metaRegion': region,
        'metaGeral': meta,
    })

    # Não deve levantar exceção
    validate_impressions_consistency(ff)


def test_validate_impressions_consistency_fails():
    # Divergência proposital (meta tem 100 a mais no Facebook)
    age = pd.DataFrame({'Veiculo': ['Facebook'], 'Impressoes': [50]})
    gender = pd.DataFrame({'Veiculo': ['Facebook'], 'Impressoes': [50]})
    region = pd.DataFrame({'Veiculo': ['Facebook'], 'Impressoes': [50]})
    meta = pd.DataFrame({'Veiculo': ['Facebook'], 'Impressoes': [200]})

    ff = FakeFetcher({
        'metaAge': age,
        'metaGender': gender,
        'metaRegion': region,
        'metaGeral': meta,
    })

    with pytest.raises(ValueError):
        validate_impressions_consistency(ff)


def test_nome_do_anuncio_bi_parametrizacao(monkeypatch):
    """Garante que todos os valores em Nome_do_Anuncio existem na coluna taxonomy_ad_name da aba BI_PARAMETRIZACAO."""
    df_src = pd.DataFrame({
        'ID': ['a', 'b'],
        'Nome_do_Anuncio': ['Anuncio X', 'Anuncio Y'],
        'X': [10, 20]
    })
    df_dest = pd.DataFrame({'ID': ['a'], 'Nome_do_Anuncio': ['Anuncio X'], 'X': [10]})
    df_param = pd.DataFrame({'taxonomy_ad_name': ['Anuncio X', 'Anuncio Y', 'Outro']})

    fetcher = FakeFetcher({
        'Origem': df_src,
        'Destino': df_dest,
        'BI_PARAMETRIZACAO': df_param
    })

    captured = {}
    def spy_appender(sheet, df):
        captured['df'] = df.copy()

    monkeypatch.setattr(l, 'get_missing_records', get_missing_records)

    l.load_missing_records(
        spreadsheet_id='dummy',
        creds_path='dummy',
        origem_sheet='Origem',
        destino_sheet='Destino',
        fetcher=fetcher,
        appender=spy_appender
    )

    final_df = captured['df']
    nomes = final_df['Nome_do_Anuncio'].dropna().unique()
    taxonomy = fetcher.get(['BI_PARAMETRIZACAO'])['BI_PARAMETRIZACAO']['taxonomy_ad_name'].dropna().unique()
    for nome in nomes:
        assert nome in taxonomy, f"'{nome}' não encontrado em taxonomy_ad_name da aba BI_PARAMETRIZACAO"

def test_id_content_present_in_final_df(monkeypatch):
    """Verifica se todas as linhas do DataFrame final possuem ID_Content não nulo/vazio."""
    # Dados de origem com ID_Content
    df_src = pd.DataFrame({
        'ID': ['a', 'b'],
        'ID_Content': ['content_a', 'content_b'],
        'X': [10, 20]
    })
    # Destino já contém um registro
    df_dest = pd.DataFrame({'ID': ['a'], 'ID_Content': ['content_a'], 'X': [10]})

    fetcher = FakeFetcher({'Origem': df_src, 'Destino': df_dest})

    captured = {}
    def spy_appender(sheet, df):
        captured['df'] = df.copy()

    # Usa lógica real para identificação de faltantes
    monkeypatch.setattr(l, 'get_missing_records', get_missing_records)

    load_missing_records(
        spreadsheet_id='id',
        creds_path='creds',
        origem_sheet='Origem',
        destino_sheet='Destino',
        fetcher=fetcher,
        appender=spy_appender
    )

    final_df = captured['df']
    # Deve existir coluna ID_Content e nenhum valor vazio
    assert 'ID_Content' in final_df.columns
    assert final_df['ID_Content'].replace('', pd.NA).notna().all()  # nenhum vazio ou NA
