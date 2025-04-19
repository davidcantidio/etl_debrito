# scripts/etl_idade.py

import logging
from typing import Optional

import pandas as pd
from scripts.etl_geral import BaseGeralETL
from utils.fields_lists import AGE_MODEL_COLUMN_ORDER
from utils.organizar_dataframe import reordenar_colunas_para_modelo
from utils.normalize import normalizar_faixa_etaria
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

# Ferramentas de merge/distribute para Meta Idade
from utils.common.meta.age_placements_merge import (
    load_and_prepare_meta_age_data,
    load_and_prepare_meta_placement_data,
    pivot_meta_age_data,
    pivot_meta_placement_data,
    merge_placement_and_age_data,
    distribute_age_metrics,
)


class BaseIdadeETL(BaseGeralETL):
    """
    Pipeline genérico de Idade para todas as plataformas, exceto Meta‑Idade.
    Renomeia Faixa_Etaria→Idade, normaliza, aplica mapeamentos e organiza as colunas.
    """

    def ajustar_tipos_e_calculos(self):
        super().ajustar_tipos_e_calculos()
        # Renomeia e normaliza Faixa_Etaria → Idade
        if "Faixa_Etaria" in self.df.columns:
            self.df.rename(columns={"Faixa_Etaria": "Idade"}, inplace=True)
        if "Idade" in self.df.columns:
            self.df["Idade"] = self.df["Idade"].apply(normalizar_faixa_etaria)
        return self.df

    def criar_veiculo(self):
        super().criar_veiculo()
        return self.df

    def reordenar_colunas_para_modelo(self):
        self.df = reordenar_colunas_para_modelo(self.df, AGE_MODEL_COLUMN_ORDER)
        return self.df

    def processar(self, df_destino: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        logging.debug(">>> In processar (BaseIdadeETL)")
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


class MetaIdadeETL(BaseIdadeETL):
    """
    ETL Idade para Meta Ads:
    1) carrega metaIdade e metaGeral
    2) pivô, merge e distribuição de métricas
    3) junta campos descritivos (Conta, Campanha, Objetivo, nomes)
    4) inferência de Veiculo/ID_Veiculo
    5) renomeação de colunas, normalização de Idade, tradução de Objetivo
    6) lookup externo de Campanha/ID_Campanha
    7) reordenação, geração de ID e numeração conforme modelo de Idade
    """

    def processar(self, df_destino: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        log = logging.getLogger("MetaIdadeETL")
        log.info(">>> Iniciando MetaIdadeETL.processar()")

        # 1) Carrega abas de origem
        df_age = load_and_prepare_meta_age_data()
        df_placement = load_and_prepare_meta_placement_data()

        # 2) Pivot & merge (métricas)
        df_age_piv = pivot_meta_age_data(df_age)
        df_place_piv = pivot_meta_placement_data(df_placement)
        df_merged = merge_placement_and_age_data(df_age_piv, df_place_piv)

        # 3) Redistribui métricas
        df_dist = distribute_age_metrics(df_merged)

        # 4) Junta campos descritivos via Ad ID
        info_map = {
            "Account name": "Nome_da_Conta",
            "Campaign name": "Campaign_name",
            "Campaign objective type": "Objetivo",
            "Ad group name": "Nome_do_Conjunto_de_Anuncio",
            "Ad name": "Nome_do_Anuncio",
        }
        df_info = (
            df_placement[["Ad ID"] + list(info_map.keys())]
            .drop_duplicates("Ad ID")
            .rename(columns=info_map)
        )
        df_dist = df_dist.merge(df_info, on="Ad ID", how="left")

        # 5) Inferência de Veículo / ID_Veiculo
        df_dist["Placement"] = df_dist["_Plataforma"]
        df_dist = atribuir_veiculo_e_id_meta(df_dist)

        # 6) Renomeação padrão de colunas
        df_dist = renomear_colunas_origem_para_modelo(df_dist)

        # 7) Normalização de Idade
        if "Faixa_Etaria" in df_dist.columns:
            df_dist.rename(columns={"Faixa_Etaria": "Idade"}, inplace=True)
        df_dist["Idade"] = df_dist["Idade"].apply(normalizar_faixa_etaria)

        # 8) Tradução de Objetivo
        df_dist = aplicar_substituicoes_objetivo(df_dist)

        # 9) Lookup externo de Campanha / ID_Campanha
        df_dist = aplicar_parametrizacao_campanha(
            df_dist, self.mapping_campanha, self.mapping_sigla
        )

        # 10) Reordenação, geração de ID e numeração
        df_dist = reordenar_colunas_para_modelo(df_dist, AGE_MODEL_COLUMN_ORDER)
        df_dist = gerar_id(df_dist)
        df_dist = gerar_numeracao(df_dist, df_destino, linha_insercao=2, coluna="Numero")

        # 11) Log de consistência de Investimento
        soma_orig = df_age["Cost"].sum()
        soma_final = df_dist["Investimento"].sum()
        log.info("🟢 Investimento original vs final: %.2f → %.2f", soma_orig, soma_final)

        log.debug(f"Colunas finais (MetaIdadeETL): {list(df_dist.columns)}")
        log.info("✅ MetaIdadeETL concluído — %s linhas", df_dist.shape[0])
        return df_dist


class TikTokIdadeETL(BaseIdadeETL):
    """ETL Idade para TikTok — usa pipeline genérico de BaseIdadeETL."""
    pass


class LinkedinIdadeETL(BaseIdadeETL):
    """ETL Idade para LinkedIn — usa pipeline genérico de BaseIdadeETL."""
    pass


class PinterestIdadeETL(BaseIdadeETL):
    """
    ETL Idade para Pinterest:
    Adiciona datas de campanha e campos de anúncio ao pipeline genérico.
    """

    def ajustar_tipos_e_calculos(self):
        super().ajustar_tipos_e_calculos()
        self.df = generate_pinterest_dates(self.df)
        self.df = preencher_campos_com_campanha(self.df)
        return self.df
