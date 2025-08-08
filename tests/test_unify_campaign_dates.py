import pandas as pd

from transform.utils.datas import unify_campaign_dates


def test_unify_campaign_dates():
    df = pd.DataFrame(
        {
            "campaign_name": ["A", "A", "B", "B", "B"],
            "start": ["2023-01-02", "2023-01-01", "2023-02-01", "", "2023-02-03"],
            "end": ["2023-01-10", "", "2023-02-10", "2023-02-09", "2023-02-08"],
        }
    )
    result = unify_campaign_dates(df.copy())
    expected_start = [pd.to_datetime("2023-01-01").date()] * 2 + [
        pd.to_datetime("2023-02-01").date()
    ] * 3
    expected_end = [pd.to_datetime("2023-01-10").date()] * 2 + [
        pd.to_datetime("2023-02-10").date()
    ] * 3
    assert list(result["start"]) == expected_start
    assert list(result["end"]) == expected_end
