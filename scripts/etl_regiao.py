# scripts/etl_regiao.py

import logging
from typing import Optional

import pandas as pd
from scripts.etl_geral import BaseGeralETL
from utils.fields_lists import REGION_MODEL_COLUMN_ORDER
from utils.organizar_dataframe import reordenar_colunas_para_modelo
from utils.normalize import convert_numeric_columns
from utils.geo_normalize import carregar_caches_padrao, limpeza_basica, obter_estado_de_regiao
from utils.atribuicoes_via_lookup import atribuir_veiculo_e_id_meta, aplicar_parametrizacao_campanha
from utils.renomeacoes import renomear_colunas_origem_para_modelo, aplicar_substituicoes_objetivo
from utils.campos_calculados import gerar_id
from utils.numeracao import gerar_numeracao
from utils.datas import generate_pinterest_dates
from utils.common_pinterest import preencher_campos_com_campanha
from utils.google_sheets import CREDS_PATH, SPREADSHEET_ID
from utils.get_google_client import get_google_client
from utils.read_sheet_as_dataframe import read_sheet_as_dataframe_range

from utils.common.meta.region_placements_merge import (
    METRICAS,
    load_and_prepare_meta_region_data,
    load_and_prepare_meta_placement_data,
    pivot_meta_region_data,
    pivot_meta_placement_data,
    merge_placement_and_region_data,
    distribute_region_metrics,
)
from utils.filter_utils import remove_zero_impressoes

class BaseRegiaoETL(BaseGeralETL):
    """Pipeline genérico de Região para todas as plataformas, exceto Meta Ads."""

    def ajustar_tipos_e_calculos(self):
        # não faz nada extra aqui — a renomeação Province→Estado e limpeza geográfica
        # já ocorre no módulo region_placements_merge
        return super().ajustar_tipos_e_calculos()

    def processar(self, df_destino: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        # 1) Padronizar nomes de colunas
        self.renomear_colunas_origem_para_modelo()
        # 2) Ajustar valores (objetivos, campanhas, veículo)
        self.aplicar_substituicoes_objetivo()
        self.aplicar_parametrizacao_campanha_externa()
        self.criar_veiculo()
        self.remover_colunas_indesejadas()
        # 3) Ordenar, gerar ID e numeração
        self.df = reordenar_colunas_para_modelo(self.df, REGION_MODEL_COLUMN_ORDER)
        self.gerar_id()
        self.df = gerar_numeracao(self.df, df_destino, linha_insercao=2, coluna="Numero")
        return self.df


class MetaRegiaoETL(BaseRegiaoETL):
    """ETL de Região específico para Meta Ads (faz pivot, merge, distribuição e depois adiciona campos)."""

    @staticmethod
    def _ensure_numeric(df: pd.DataFrame) -> pd.DataFrame:
        subs = METRICAS + [f"_{m}" for m in METRICAS]
        cols = [c for c in df.columns if any(s in c for s in subs)]
        df[cols] = df[cols].apply(pd.to_numeric, errors="coerce").fillna(0)
        return df

    def processar(self, df_destino: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        log = logging.getLogger("MetaRegiaoETL")
        log.info(">>> Iniciando MetaRegiaoETL.processar()")

        # A) Checar consistência de investimento raw
        client_raw = get_google_client(CREDS_PATH)
        df_raw = read_sheet_as_dataframe_range(
            client_raw,
            SPREADSHEET_ID,
            sheet_name="metaRegiao",
            range_str="A1:ZZ",
            header_row_index=0,
        )
        soma_raw = convert_numeric_columns(df_raw, ["Cost"])["Cost"].sum()

        # 1) Leitura e preparação (inclui Province→Estado + geo-normalização)
        df_region = load_and_prepare_meta_region_data()
        df_placement = load_and_prepare_meta_placement_data()

        df_region = convert_numeric_columns(df_region, METRICAS)
        df_placement = convert_numeric_columns(df_placement, METRICAS)

        # 2) Pivot
        df_region_piv = self._ensure_numeric(pivot_meta_region_data(df_region))
        df_place_piv = self._ensure_numeric(pivot_meta_placement_data(df_placement))

        # 3) Merge + distribuição proporcional
        df_dist = distribute_region_metrics(
            merge_placement_and_region_data(df_region_piv, df_place_piv)
        )

        # 4) Campos descritivos via Ad ID
        info_map = {
            "Account name":            "Nome_da_Conta",
            "Campaign name":           "Campaign_name",
            "Campaign objective type": "Objetivo",
            "Ad group name":           "Nome_do_Conjunto_de_Anuncio",
            "Ad name":                 "Nome_do_Anuncio",
        }
        df_info = (
            df_placement[["Ad ID"] + list(info_map.keys())]
            .drop_duplicates("Ad ID")
            .rename(columns=info_map)
        )
        df_dist = df_dist.merge(df_info, on=["Ad ID"], how="left")

        # 5) Tradução de Objetivo e lookup de Campanha
        df_dist = aplicar_substituicoes_objetivo(df_dist)
        df_dist = aplicar_parametrizacao_campanha(
            df_dist, self.mapping_campanha, self.mapping_sigla
        )

        # 6) Veículo
        df_dist["Placement"] = df_dist["_Plataforma"]
        df_dist = atribuir_veiculo_e_id_meta(df_dist)
        df_dist = renomear_colunas_origem_para_modelo(df_dist)

        # 7) Ordenação final, geração de ID e numeração
        df_dist = reordenar_colunas_para_modelo(df_dist, REGION_MODEL_COLUMN_ORDER)
        df_dist = gerar_id(df_dist)
        df_dist = gerar_numeracao(df_dist, df_destino, linha_insercao=2, coluna="Numero")

        # 8) Validação de investimento
        soma_final = df_dist["Investimento"].sum()
        log.info(
            "🟢 Investimento original vs final: %s → %s",
            f"{soma_raw:.2f}".replace(".", ","),
            f"{soma_final:.2f}".replace(".", ","),
        )
        log.info("✅ MetaRegiaoETL concluído — %s linhas", df_dist.shape[0])
        return df_dist


class TikTokRegiaoETL(BaseRegiaoETL):
    pass


class LinkedinRegiaoETL(BaseRegiaoETL):
    pass


class PinterestRegiaoETL(BaseRegiaoETL):
    def ajustar_tipos_e_calculos(self):
        super().ajustar_tipos_e_calculos()
        self.df = generate_pinterest_dates(self.df)
        return preencher_campos_com_campanha(self.df)
