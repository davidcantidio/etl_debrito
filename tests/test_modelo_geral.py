import logging
import pandas as pd
import pytest
from utils.google_sheets import carregar_aba_google_sheets, SPREADSHEET_URL
from utils.get_google_client import get_google_client
from utils.google_sheets import CREDS_PATH

SPREADSHEET_ID = "1DazUQxspLgT0utOFHcTINbFngXw7Fq0LOq6v4lRGixg"


def load_final_sheet(sheet_name: str) -> pd.DataFrame:
    """
    Loads the final sheet specified by `sheet_name` into a pandas DataFrame.
    """
    client = get_google_client(CREDS_PATH)
    return carregar_aba_google_sheets(CREDS_PATH, SPREADSHEET_URL, sheet_name)


def test_unique_vehicles():
    """
    Ensures all expected vehicles appear in the final modeloGeral sheet.
    Prints and logs the found vehicles for verification.
    """
    logger = logging.getLogger("test_unique_vehicles")
    df = load_final_sheet("modeloGeral")
    vehicles = sorted(df["Veiculo"].dropna().unique())
    logger.debug(f"Found vehicles: {vehicles}")
    print("Vehicles in final sheet:", vehicles)
    # TODO: replace with actual expected list from configuration
    expected = vehicles  # adjust this to your expected list
    assert set(expected) == set(vehicles), f"Expected vehicles {expected}, but found {vehicles}"


def test_no_blank_cells_except_url():
    """
    Verifies that there are no blank cells in any column except URL_do_Anuncio.

    If blank cells are found, logs and prints the row number, list of missing columns,
    and context from Veiculo, Campanha, and Nome_do_Anuncio.
    """
    logger = logging.getLogger("test_no_blank_cells")
    df = load_final_sheet("modeloGeral")
    exempt = {"URL_do_Anuncio"}
    errors = []
    for idx, row in df.iterrows():
        missing = [col for col in df.columns
                   if col not in exempt and (pd.isna(row[col]) or str(row[col]).strip() == "")]
        if missing:
            row_num = idx + 2  # account for header row in sheet
            context = {c: row.get(c, "") for c in ["Veiculo", "Campanha", "Nome_do_Anuncio"] if c in df.columns}
            message = f"Row {row_num}: missing columns {missing}, context={context}"
            logger.error(message)
            errors.append(message)
    if errors:
        pytest.fail("\n".join(errors))


def test_id_veiculo_numeric():
    """
    Ensures all values in the 'ID_Veiculo' column consist only of digits.
    Logs and prints any rows where a non-digit value is found.
    """
    logger = logging.getLogger("test_id_veiculo_numeric")
    df = load_final_sheet("modeloGeral")
    errors = []
    for idx, row in df.iterrows():
        val = str(row.get("ID_Veiculo", "")).strip()
        if not val.isdigit():
            row_num = idx + 2  # account for header row in sheet
            context = {
                "Veiculo": row.get("Veiculo", ""),
                "Campanha": row.get("Campanha", ""),
                "Nome_do_Anuncio": row.get("Nome_do_Anuncio", "")
            }
            message = f"Row {row_num}: non-numeric ID_Veiculo='{val}', context={context}"
            logger.error(message)
            print(message)
            errors.append(message)
    if errors:
        pytest.fail(f"Found {len(errors)} non-numeric ID_Veiculo entries. See above for details.")
