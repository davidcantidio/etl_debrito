import pandas as pd

from treat.utils.datas import unify_campaign_dates


def test_unify_campaign_dates_missing():
    df = pd.DataFrame(
        {
            "campaign_name": ["A", "A", "B"],
            "start": ["", "", ""],
            "end": ["2023-01-10", "2023-01-11", ""],
        }
    )
    result = unify_campaign_dates(df.copy())
    assert result.loc[0, "start"] == ""
    assert result.loc[2, "start"] == ""
    assert result.loc[0, "end"] == pd.to_datetime("2023-01-11").date()
    assert result.loc[1, "end"] == pd.to_datetime("2023-01-11").date()
    assert result.loc[2, "end"] == ""
