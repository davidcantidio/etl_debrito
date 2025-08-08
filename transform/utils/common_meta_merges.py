from abc import ABC, abstractmethod
from typing import List

import pandas as pd
from utils.get_google_client import get_google_client
from utils.google_sheets import CREDS_PATH, SPREADSHEET_ID
from utils.read_sheet_as_dataframe import read_sheet_as_dataframe_range

# cliente compartilhado
_CLIENT = get_google_client(CREDS_PATH)


class MetaplacementMerge(ABC):
    """
    Classe base para leitura/pivô/merge de abas 'metaX' + 'metaGeral'.
    """

    def __init__(self, dimension_sheet: str, general_sheet: str):
        self.dimension_sheet = dimension_sheet
        self.general_sheet = general_sheet

    def load_dimension_data(self) -> pd.DataFrame:
        df = read_sheet_as_dataframe_range(
            _CLIENT,
            SPREADSHEET_ID,
            sheet_name=self.dimension_sheet,
            range_str="A1:ZZ",
            header_row_index=0,
        )
        # remove linhas sem as chaves de dimensão
        return df.dropna(subset=self.dimension_keys(), how="any")

    def load_general_data(self) -> pd.DataFrame:
        df = read_sheet_as_dataframe_range(
            _CLIENT,
            SPREADSHEET_ID,
            sheet_name=self.general_sheet,
            range_str="A1:ZZ",
            header_row_index=0,
        )
        return df.dropna(subset=self.general_keys(), how="any")

    @abstractmethod
    def dimension_keys(self) -> List[str]:
        """Ex: ['ad_id','Date','Age']"""

    @abstractmethod
    def metric_columns(self) -> List[str]:
        """Ex: METRICAS"""

    def general_keys(self) -> List[str]:
        """Chaves comuns para o merge; por padrão os dois primeiros da dimensão."""
        return self.dimension_keys()[:2]

    def pivot_dimension_data(self, df: pd.DataFrame) -> pd.DataFrame:
        keys = self.dimension_keys()
        metrics = self.metric_columns()
        return df.groupby(keys, as_index=False)[metrics].sum()

    def pivot_general_data(self, df: pd.DataFrame) -> pd.DataFrame:
        id_vars = self.general_keys()
        value_vars = [c for c in df.columns if c not in id_vars + ["placement"]]
        df_piv = df.pivot_table(
            index=id_vars,
            columns="placement",
            values=value_vars,
            aggfunc="sum",
            fill_value=0,
        )
        # flatten columns: ('Impressions','A') → 'A_Impressions'
        df_piv.columns = [f"{pl}_{m}" for m, pl in df_piv.columns]
        df_piv.reset_index(inplace=True)
        return df_piv

    def merge_data(self) -> pd.DataFrame:
        df_dim = self.load_dimension_data()
        df_dim_p = self.pivot_dimension_data(df_dim)

        df_gen = self.load_general_data()
        df_gen_p = self.pivot_general_data(df_gen)

        return pd.merge(
            df_dim_p,
            df_gen_p,
            on=self.general_keys(),
            how="inner",
        )
