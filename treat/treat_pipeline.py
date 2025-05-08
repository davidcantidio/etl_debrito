from __future__ import annotations
import pandas as pd
from typing import Dict, Callable
import logging
from treat.utils.geo_normalize import ( 
    obter_estado_de_regiao,
    carregar_caches_padrao
)

from .bi_param_utils import (
    BIParamLookup,
    enrich_with_bi_parametrizacao,
)
from .preprocess_utils import preprocess_origin
from treat.utils.write_back import write_back_df

from treat.utils.renomeacoes       import (
    renomear_colunas_origem_para_modelo,
    aplicar_substituicoes_objetivo,
)
from treat.utils.campos_calculados import gerar_id
from treat.utils.normalize import normalize_age, normalize_gender

log = logging.getLogger(__name__)

CACHE_ESTADOS, CACHE_MUNICIPIOS = carregar_caches_padrao()

class TreatPipeline:
    """Pipeline genérico que aplica **todas** as etapas de tratamento a uma aba
    de origem (Meta, TikTok, Pinterest, etc.).

    Parâmetros
    ----------
    creds_path      : caminho do JSON de credenciais do serviço
    spreadsheet_id  : ID da planilha (Google Sheets)
    sheet_name      : nome da aba de origem a ser tratada
    mapping_renomeacao : dict col_origem → col_modelo (específico da aba)
    write_back      : se True, grava as correções de volta na aba
    subs_objetivo_fn: função que aplica substituições de "objective"
                      (permite customizar por plataforma se necessário)
    """

    def __init__(
        self,
        creds_path: str,
        spreadsheet_id: str,
        sheet_name: str,
        mapping_renomeacao: Dict[str, str],
        *,
        write_back: bool = True,
        subs_objetivo_fn: Callable[[pd.DataFrame], pd.DataFrame] = aplicar_substituicoes_objetivo,
    ) -> None:
        self.creds_path = creds_path
        self.spreadsheet_id = spreadsheet_id
        self.sheet_name = sheet_name
        self.write_back = write_back
        self.mapping = mapping_renomeacao
        self.subs_obj_fn = subs_objetivo_fn

        # lookup BI compartilhado durante toda a execução
        self._bi_lookup = BIParamLookup(creds_path, spreadsheet_id)

    # ───────────────────────────── helpers internos ────────────────────────── #
    def _preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica substituições de origem + normalização de região."""
        return preprocess_origin(df)

    def _fill_start_end(self, df: pd.DataFrame) -> pd.DataFrame:
        """Preenche start/end vazios via utm_content usando BI_PARAMETRIZAÇÃO."""
        return self._bi_lookup.fill_missing_start_end_from_utm(
            df, sheet_name=self.sheet_name, write_back=self.write_back
        )

    def _enrich_bi(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enriquece campanha, ID_Campanha e preenche utm_content vazio."""
        return enrich_with_bi_parametrizacao(
            df, self.creds_path, self.spreadsheet_id, sheet_name=self.sheet_name
        )

    # ──────────────────────────── método principal ─────────────────────────── #
    def run(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """Executa o pipeline completo e devolve DataFrame padronizado."""
        # 1) pré‑processamento
        df = self._preprocess(df_raw)
        log.debug("[TreatPipeline] Após preprocess: %d linhas, %d colunas", *df.shape)

        # 2) completar start/end
        df = self._fill_start_end(df)

        # 3) enriquecimento BI (Campanha, ID_Campanha, utm_content)
        df = self._enrich_bi(df)
        
        # 4) normalização de idade (se aplicável)
        if "age" in df.columns:
            df["age"] = df["age"].apply(normalize_age)

        # 5) normalização de região (se aplicável)
        if "region" in df.columns:
            df["region"] = df["region"].apply(
                lambda v: obter_estado_de_regiao(v, CACHE_MUNICIPIOS, CACHE_ESTADOS)
            )

        # normalização de gênero (se aplicável)
        if "gender" in df.columns:
            df["gender"] = df["gender"].apply(normalize_gender)

        # 4) grava correções de volta na aba de origem
        if self.write_back:
            write_back_df(df, self.creds_path, self.spreadsheet_id, self.sheet_name)
            log.info("[TreatPipeline] Correções gravadas na aba '%s'", self.sheet_name)

        # 5) renomeia colunas segundo mapping do modelo
        df = renomear_colunas_origem_para_modelo(df, self.mapping)

        # 6) substitui valores de objective
        df = self.subs_obj_fn(df)

        # 7) gera ID único
        df["ID"] = df.apply(gerar_id, axis=1)
        log.debug("[TreatPipeline] Pipeline concluído para aba '%s'", self.sheet_name)
        return df
