# scripts/etl_idade.py

import logging
from typing import Optional

import pandas as pd
from scripts.etl_geral import BaseGeralETL
from utils.fields_lists import GENDER_MODEL_COLUMN_ORDER
from utils.organizar_dataframe import reordenar_colunas_para_modelo
from utils.normalize import (
    normalizar_genero,
    converter_colunas_numericas,
)
from utils.atribuicoes_via_lookup import (
    atribuir_veiculo_e_id_meta,
    aplicar_parametrizacao_campanha,
)
from utils.renomeacoes import (
    renomear_colunas_origem_para_modelo,
    aplicar_substituicoes_objetivo,
)
from utils.campos_calculados import gerar_id
from utils.numeracao import gerar_numeracao
from utils.common_pinterest import preencher_campos_com_campanha
from utils.datas import generate_pinterest_dates
from utils.google_sheets import CREDS_PATH, SPREADSHEET_ID
from utils.get_google_client import get_google_client
from utils.read_sheet_as_dataframe import read_sheet_as_dataframe_range

# Ferramentas de merge/distribute para Meta Genero
from utils.common.meta.gender_placement_merge import (
    METRICAS,
    load_and_prepare_meta_gender_data,
    load_and_prepare_meta_placement_data,
    pivot_meta_gender_data,
    pivot_meta_placement_data,
    merge_placement_and_gender_data,
    distribute_gender_metrics,
)


class BaseGeneroETL(BaseGeralETL):
    """Pipeline genérico de Genero para todas as plataformas, exceto Meta."""

    def ajustar_tipos_e_calculos(self):
        super().ajustar_tipos_e_calculos()
        # Faixa_Etaria → Genero
        if "Faixa_Etaria" in self.df.columns:
            self.df.rename(columns={"Faixa_Etaria": "Genero"}, inplace=True)
        if "Genero" in self.df.columns:
            self.df["Genero"] = self.df["Genero"].apply(normalizar_genero)
        return self.df

    def criar_veiculo(self):
        super().criar_veiculo()
        return self.df

    def reordenar_colunas_para_modelo(self):
        self.df = reordenar_colunas_para_modelo(self.df, GENDER_MODEL_COLUMN_ORDER)
        return self.df

    def processar(self, df_destino: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        self.renomear_colunas_origem_para_modelo()
        self.ajustar_tipos_e_calculos()
        self.aplicar_substituicoes_objetivo()
        self.aplicar_parametrizacao_campanha_externa()
        self.criar_veiculo()
        self.remover_colunas_indesejadas()
        self.reordenar_colunas_para_modelo()
        self.gerar_id()
        self.df = gerar_numeracao(self.df, df_destino, linha_insercao=2, coluna="Numero")
        return self.df


class MetaGeneroETL(BaseGeneroETL):
    """ETL Genero dedicado ao Meta Ads."""

    @staticmethod
    def _ensure_numeric(df: pd.DataFrame) -> pd.DataFrame:
        metric_substrings = METRICAS + [f"_{m}" for m in METRICAS]
        cols = [c for c in df.columns if any(sub in c for sub in metric_substrings)]
        df[cols] = df[cols].apply(pd.to_numeric, errors="coerce").fillna(0)
        return df

    def processar(self, df_destino: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        log = logging.getLogger("MetaGeneroETL")
        log.info(">>> Iniciando MetaGeneroETL.processar()")

        # A) Leitura raw para consistência de Investimento
        client_raw = get_google_client(CREDS_PATH)
        df_raw = read_sheet_as_dataframe_range(
            client_raw, SPREADSHEET_ID,
            sheet_name="metaGenero",
            range_str="A1:ZZ",
            header_row_index=0
        )
        # soma raw em decimal
        soma_raw = converter_colunas_numericas(df_raw, ["Cost"])["Cost"].sum()

        # 1) Carrega e converte vírgula→ponto decimal ------------------
        df_gender       = load_and_prepare_meta_gender_data()
        df_placement = load_and_prepare_meta_placement_data()
        df_gender       = converter_colunas_numericas(df_gender, METRICAS)
        df_placement = converter_colunas_numericas(df_placement, METRICAS)

        # 2) Pivôs e garantia de numérico -----------------------------
        df_gender_piv   = self._ensure_numeric(pivot_meta_gender_data(df_gender))
        df_place_piv = self._ensure_numeric(pivot_meta_placement_data(df_placement))

        # 3) Merge + distribuição de métricas -------------------------
        df_dist = distribute_gender_metrics(merge_placement_and_gender_data(df_gender_piv, df_place_piv))

        # 4) Campos descritivos via Ad ID -----------------------------
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

        # 5) Inferência de Veículo / ID_Veiculo -----------------------
        df_dist["Placement"] = df_dist["_Plataforma"]
        df_dist = atribuir_veiculo_e_id_meta(df_dist)

        # 6) Renomeação padrão + Genero -------------------------------
        df_dist = renomear_colunas_origem_para_modelo(df_dist)
        if "Faixa_Etaria" in df_dist.columns:
            df_dist.rename(columns={"Faixa_Etaria": "Genero"}, inplace=True)
        df_dist["Genero"] = df_dist["Genero"].apply(normalizar_genero)

        # 7) Tradução de Objetivo e lookup Campanha ------------------
        df_dist = aplicar_substituicoes_objetivo(df_dist)
        df_dist = aplicar_parametrizacao_campanha(
            df_dist, self.mapping_campanha, self.mapping_sigla
        )

        # 8) Reordenação, geração de ID e numeração ------------------
        df_dist = reordenar_colunas_para_modelo(df_dist, GENDER_MODEL_COLUMN_ORDER)
        df_dist = gerar_id(df_dist)
        df_dist = gerar_numeracao(df_dist, df_destino, linha_insercao=2, coluna="Numero")

        # 9) Consistência de Investimento ----------------------------
        soma_final = df_dist["Investimento"].sum()
        # formata para BR
        orig_str = f"{soma_raw:.2f}".replace('.', ',')
        final_str = f"{soma_final:.2f}".replace('.', ',')
        log.info("🟢 Investimento original vs final: %s → %s", orig_str, final_str)
        log.info("✅ MetaGeneroETL concluído — %s linhas", df_dist.shape[0])
        return df_dist


class TikTokGeneroETL(BaseGeneroETL):
    pass


class LinkedinGeneroETL(BaseGeneroETL):
    pass


class PinterestGeneroETL(BaseGeneroETL):
    def ajustar_tipos_e_calculos(self):
        super().ajustar_tipos_e_calculos()
        self.df = generate_pinterest_dates(self.df)
        self.df = preencher_campos_com_campanha(self.df)
        return self.df
