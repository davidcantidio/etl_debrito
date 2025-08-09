# tests/test_min_date_filter.py
import pandas as pd
import datetime as dt
from transform.transform.settings import MIN_DATE
from transform.extract import read_df

def test_filter_respects_min_date(tmp_path):
    """
    Garante que `read_df` (ou função que aplica o filtro)
    elimina linhas anteriores a MIN_DATE.
    """
    # --- dado de entrada sintético -------------------------
    df_fake = pd.DataFrame(
        {
            "date": [
                (MIN_DATE - dt.timedelta(days=1)).isoformat(),  # deve ser filtrado
                MIN_DATE.isoformat(),                          # deve ficar
            ],
            "Campanha": ["Foo", "Bar"],
            "Veiculo": ["Facebook", "Instagram"],
        }
    )
    csv = tmp_path / "fake.csv"
    df_fake.to_csv(csv, index=False)

    # --- chamada ------------------------------------------------
    df_out = read_df(csv)          # ajuste se assinatura for diferente

    # --- asserções ---------------------------------------------
    assert (df_out["date_dt"] >= MIN_DATE).all(), "Linha < MIN_DATE passou pelo filtro"
    assert len(df_out) == 1, "Deveria restar exatamente 1 linha >= MIN_DATE"
