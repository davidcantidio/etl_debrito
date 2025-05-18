# etl_alcance.py

import logging
import pandas as pd
from scripts.etl_geral import BaseGeralETL
from utils.fields_lists import REACH_MODEL_COLUMN_ORDER
from utils.organizar_dataframe import reordenar_colunas_para_modelo
from utils.numeracao import gerar_numeracao
from utils.atribuicoes_via_lookup import atribuir_veiculo_e_id_meta
from utils.common_pinterest import preencher_campos_com_campanha
from utils.datas import generate_pinterest_dates
from utils.common_linkedin import preencher_nomes_anuncio_linkedin

def gerar_id(df: pd.DataFrame) -> pd.DataFrame:
    logging.debug(">>> In gerar_id (etl_alcance)")
    def id_logic(row):
        campos = ['Data', 'Campanha', 'Impressoes']
        return "-".join(str(row[c]) for c in campos if c in row)
    df['ID'] = df.apply(id_logic, axis=1)
    return df

class BaseAlcanceETL(BaseGeralETL):

    def ajustar_tipos_e_calculos(self):
        logging.debug(">>> In BaseAlcanceETL.ajustar_tipos_e_calculos")
        super().ajustar_tipos_e_calculos()
        return self.df

    def criar_veiculo(self):
        logging.debug(">>> In BaseAlcanceETL.criar_veiculo")
        super().criar_veiculo()
        return self.df

    def reordenar_colunas_para_modelo(self):
        logging.debug(">>> In BaseAlcanceETL.reordenar_colunas_para_modelo")
        self.df = reordenar_colunas_para_modelo(self.df, REACH_MODEL_COLUMN_ORDER)
        return self.df

    def processar(self, df_destino=None):
        logging.debug(">>> In BaseAlcanceETL.processar")
        self.renomear_colunas_origem_para_modelo()
        self.ajustar_tipos_e_calculos()
        self.aplicar_substituicoes_objetivo()
        self.aplicar_parametrizacao_campanha_externa()
        self.determine_ad_preview_link()
        self.criar_veiculo()

        if "reach" in self.df.columns:
            logging.debug("Copiando 'reach' para 'Alcance'")
            self.df["Alcance"] = self.df["reach"]

        self.remover_colunas_indesejadas()
        self.reordenar_colunas_para_modelo()
        self.df = gerar_id(self.df)
        self.df = gerar_numeracao(self.df, df_destino, linha_insercao=2, coluna="Numero")

        # mantém só as colunas do modelo + ID
        valid_cols = set(REACH_MODEL_COLUMN_ORDER) | {"ID"}
        self.df = self.df[[c for c in self.df.columns if c in valid_cols]]

        logging.debug(">>> Final BaseAlcanceETL.processar")
        return self.df

class MetaAlcanceETL(BaseAlcanceETL):
    def criar_veiculo(self):
        logging.debug(">>> In MetaAlcanceETL.criar_veiculo")
        return atribuir_veiculo_e_id_meta(self.df)

class TikTokAlcanceETL(BaseAlcanceETL):
    pass

class LinkedinAlcanceETL(BaseAlcanceETL):
    def __init__(self, *args, mapping_preview=None, mapping_criativo=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.mapping_preview = mapping_preview or {}
        self.mapping_criativo = mapping_criativo or {}

    def ajustar_tipos_e_calculos(self):
        logging.debug(">>> In LinkedinAlcanceETL.ajustar_tipos_e_calculos")
        super().ajustar_tipos_e_calculos()
        if 'utm_content' in self.df.columns:
            self.df = preencher_nomes_anuncio_linkedin(self.df, self.mapping_criativo)
        else:
            logging.warning("utm_content ausente; não preenche nomes de anúncio.")
        return self.df

class PinterestAlcanceETL(BaseAlcanceETL):
    def ajustar_tipos_e_calculos(self):
        logging.debug(">>> In PinterestAlcanceETL.ajustar_tipos_e_calculos")
        super().ajustar_tipos_e_calculos()
        self.df = generate_pinterest_dates(self.df)
        self.df = preencher_campos_com_campanha(self.df)
        return self.df
