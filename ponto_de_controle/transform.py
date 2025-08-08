from typing import Iterable
import pandas as pd

from transform.utils.datas import concat_period, normalize_date_to_str_DD_M_YYYY
from transform.utils.normalize import normalize_vehicle
from transform.utils.campos_calculados import make_id_ponto_de_controle

from ponto_de_controle.constants import HEAD_ROW_DEST, DEST_TAB  # se precisar em debug
from ponto_de_controle.constants import GOOGLE_CREDS_PATH  # idem
from ponto_de_controle.constants import MIN_DATE  # idem
from ponto_de_controle.constants import ORIGIN_SHEET_ID, ORIGIN_TAB  # idem

from ponto_de_controle.constants import DEST_COLUMNS
import logging

logger = logging.getLogger(__name__)

def transform_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converte `df` da origem para o layout de destino e calcula `__ID__`.
    """
    df2 = df.copy()

    df2["Data"] = df2["start"].apply(normalize_date_to_str_DD_M_YYYY)
    df2["Periodo"] = df2.apply(lambda r: concat_period(r["start"], r["end"]), axis=1)
    df2["Veiculo"] = df2["Veiculo"].apply(normalize_vehicle)

    df2["Link conteúdos impulsionados"] = df2.get("URL_do_Anuncio", "")
    df2["Agência"] = "De Brito"
    df2["Editoria"] = df2["Campanha"]
    df2["Objetivo"] = df2.get("objective", "")
    df2[["Meta", "Status", "Resultado"]] = ""

    df_t = df2.reindex(columns=DEST_COLUMNS, fill_value="")
    df_t["__ID__"] = df_t.apply(
        make_id_ponto_de_controle, axis=1, columns=DEST_COLUMNS
    )

    logger.info("df_transf: %d linhas × %d colunas", *df_t.shape)
    return df_t
