import pytest
import pandas as pd

from utils.read_sheet_as_dataframe      import read_sheet_as_dataframe_range
from utils.get_google_client            import get_google_client
from utils.google_sheets                import SPREADSHEET_ID, CREDS_PATH
from utils.datas                        import fill_missing_start_end_from_params
from utils.normalize                    import normalize_campaign_series

# ---- Fixtures reutilizáveis ----

@pytest.fixture(scope="session")
def google_client():
    return get_google_client(CREDS_PATH)

@pytest.fixture(scope="session")
def bi_param_df(google_client):
    """DataFrame da aba BI_PARAMETRIZAÇÃO, já com colunas normalizadas."""
    df = read_sheet_as_dataframe_range(
        google_client,
        SPREADSHEET_ID,
        sheet_name="BI_PARAMETRIZAÇÃO",
        range_str="A1:ZZ",
        header_row_index=1
    )
    df.columns = df.columns.str.strip().str.replace('\n', ' ', regex=False).str.upper()
    return df

@pytest.fixture(scope="session")
def campaign_mappings(bi_param_df):
    """Dois dicionários: CAMPANHA → START e CAMPANHA → END."""
    df = bi_param_df.copy()
    if "CAMPANHA" not in df.columns:
        raise KeyError("Coluna 'CAMPANHA' não encontrada em BI_PARAMETRIZAÇÃO!")

    df["campanha_norm"] = normalize_campaign_series(df["CAMPANHA"])
    start_map = dict(zip(df["campanha_norm"], df.get("START", "")))
    end_map   = dict(zip(df["campanha_norm"], df.get("END",   "")))
    return start_map, end_map

# ---- Função auxiliar de debug ----

def debug_campanhas_problematicas(df_orig, campaign_name):
    print(f"\n🔍 Buscando campanhas parecidas com '{campaign_name}':")
    search = df_orig["Ad name"].dropna().astype(str).str.lower()
    matches = search[search.str.contains(campaign_name.lower().strip(), na=False)]
    if matches.empty:
        print("Nenhuma campanha parecida encontrada.")
    else:
        print("Campanhas parecidas encontradas:", matches.to_list())

# ---- Lista de abas a testar ----

SHEETS = [
    "metaGeral",
    "linkedinGeral",
    "pinterestGeral",
    "tiktokGeral",
]

# ---- Teste principal ----

@pytest.mark.parametrize("sheet_name", SHEETS, ids=SHEETS)
def test_fill_missing_start_end(
    google_client,
    campaign_mappings,
    sheet_name
):
    start_map, end_map = campaign_mappings

    df_orig = read_sheet_as_dataframe_range(
        google_client,
        SPREADSHEET_ID,
        sheet_name=sheet_name,
        range_str="A1:ZZ",
        header_row_index=0
    )

    if not {"Start", "End", "Ad name"}.issubset(df_orig.columns):
        pytest.skip(f"{sheet_name} não contém Start/End/Ad name")

    empty_start_before = df_orig["Start"].astype(str).str.strip().eq("").sum()
    empty_end_before   = df_orig["End"].  astype(str).str.strip().eq("").sum()

    df_filled = fill_missing_start_end_from_params(
        df_orig.copy(),
        sheet_name=sheet_name,
        write_back=False,
        inplace=False
    )

    empty_start_after = df_filled["Start"].astype(str).str.strip().eq("").sum()
    empty_end_after   = df_filled["End"].  astype(str).str.strip().eq("").sum()

    assert empty_start_after <= empty_start_before, (
        f"[{sheet_name}] Start vazios aumentou: {empty_start_before} → {empty_start_after}"
    )
    assert empty_end_after <= empty_end_before, (
        f"[{sheet_name}] End vazios aumentou: {empty_end_before} → {empty_end_after}"
    )

    df_norm = df_orig.copy()
    df_norm["campanha_norm"] = normalize_campaign_series(df_norm["Ad name"])

    # Mask para linhas onde deveria preencher Start
    mask_s = df_norm["Start"].astype(str).str.strip().eq("") & df_norm["campanha_norm"].isin(start_map)
    for idx in df_norm[mask_s].index:
        campanha = df_norm.at[idx, "campanha_norm"]

        # Verifica se a campanha realmente existe no DataFrame preenchido
        if campanha not in normalize_campaign_series(df_filled["Ad name"]).values:
            print(f"\n⚠️ Campanha '{df_norm.at[idx, 'Ad name']}' não encontrada no preenchido — ignorando.")
            continue

        esperado = start_map[campanha].strip()
        obtido   = df_filled.at[idx, "Start"].strip()

        if esperado and not obtido:
            debug_campanhas_problematicas(df_orig, df_norm.at[idx, "Ad name"])

        assert obtido == esperado, (
            f"[{sheet_name}] Start incorreto para '{df_norm.at[idx, 'Ad name']}': "
            f"esperado '{esperado}', obteve '{obtido}'"
        )

    # Mask para linhas onde deveria preencher End
    mask_e = df_norm["End"].astype(str).str.strip().eq("") & df_norm["campanha_norm"].isin(end_map)
    for idx in df_norm[mask_e].index:
        campanha = df_norm.at[idx, "campanha_norm"]

        # Verifica se a campanha realmente existe no DataFrame preenchido
        if campanha not in normalize_campaign_series(df_filled["Ad name"]).values:
            print(f"\n⚠️ Campanha '{df_norm.at[idx, 'Ad name']}' não encontrada no preenchido — ignorando.")
            continue

        esperado = end_map[campanha].strip()
        obtido   = df_filled.at[idx, "End"].strip()

        if esperado and not obtido:
            debug_campanhas_problematicas(df_orig, df_norm.at[idx, "Ad name"])

        assert obtido == esperado, (
            f"[{sheet_name}] End incorreto para '{df_norm.at[idx, 'Ad name']}': "
            f"esperado '{esperado}', obteve '{obtido}'"
        )


def debug_start_end_issue(df_orig: pd.DataFrame, ad_name_target: str):
    """
    Mostra informações detalhadas para um Ad name problemático.
    """
    print(f"\n🔍 Diagnóstico para Ad name: '{ad_name_target}'")

    if "Ad name" not in df_orig.columns:
        print("⚠️ Coluna 'Ad name' não encontrada.")
        return

    df_orig["ad_norm"] = normalize_campaign_series(df_orig["Ad name"])
    ad_name_norm = ad_name_target.strip().lower()

    match = df_orig[df_orig["ad_norm"] == ad_name_norm]

    if match.empty:
        print("⚠️ Nenhuma linha encontrada no DataFrame de origem para esse Ad name.")
    else:
        print(f"Encontradas {len(match)} linhas para o Ad name:")
        print(match[["Ad name", "Start", "End"]])

        for idx, row in match.iterrows():
            start_val = str(row["Start"]).strip()
            if start_val == "":
                print(f"➡️ Linha {idx+2}: Start estava vazio antes do preenchimento.")
            else:
                print(f"➡️ Linha {idx+2}: Start já tinha valor '{start_val}' antes do preenchimento.")
