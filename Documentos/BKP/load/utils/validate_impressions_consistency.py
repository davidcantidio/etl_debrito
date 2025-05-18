# validate_impressions_consistency.py
import pandas as pd
from typing import Sequence
from extract.sheets_fetcher import SheetsFetcher

def validate_impressions_consistency(
    fetcher: SheetsFetcher,
    meta_sheet: str = "metaGeral",
    demo_sheets: Sequence[str] = ("metaAge", "metaGender", "metaRegion"),
):
    """Lança ValueError se a soma de impressões (age+gender+region)
    não bater com metaGeral para cada veículo."""
    df_meta = fetcher.get([meta_sheet], as_frame=True)[meta_sheet]
    demos = fetcher.get(list(demo_sheets), as_frame=True)

    demo_sum = (
        pd.concat([demos[s] for s in demo_sheets])
          .groupby("Veiculo", as_index=False)["Impressoes"]
          .sum()
    )
    meta_sum = df_meta.groupby("Veiculo", as_index=False)["Impressoes"].sum()

    merged = demo_sum.merge(
        meta_sum, on="Veiculo", how="outer",
        suffixes=("_demo", "_meta")
    ).fillna(0)

    mismatch = merged[merged["Impressoes_demo"] != merged["Impressoes_meta"]]
    if not mismatch.empty:
        raise ValueError(
            "Divergência de impressões:\\n" + mismatch.to_string(index=False)
        )
