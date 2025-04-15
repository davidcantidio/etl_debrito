# etl_alcance.py

import logging
import pandas as pd
from scripts.etl_geral import BaseGeralETL
from utils.fields_lists import REACH_MODEL_COLUMN_ORDER
from utils.organizar_dataframe import reordenar_colunas_para_modelo
from utils.numeracao import gerar_numeracao
from utils.atribuicoes_via_lookup import atribuir_veiculo_e_id_meta, atribuir_id_veiculo_generico
from utils.common_pinterest import preencher_campos_com_campanha
from utils.datas import generate_pinterest_dates
from utils.common_linkedin import preencher_nomes_anuncio_linkedin


def gerar_id(df: pd.DataFrame) -> pd.DataFrame:
    """
    Gera um ID único por linha com base nos campos 'Data', 'Campanha' e 'Impressoes'.
    """
    logging.debug(">>> In gerar_id (etl_alcance)")
    def id_logic(row):
        campos = ['Data', 'Campanha', 'Impressoes']
        partes = [str(row[campo]) for campo in campos if campo in row]
        return "-".join(partes)
    df['ID'] = df.apply(id_logic, axis=1)
    return df

class BaseAlcanceETL(BaseGeralETL):
    """
    Classe base para ETLs de Alcance.
    Aplica transformações genéricas sem normalizações específicas.
    """

    def ajustar_tipos_e_calculos(self):
        logging.debug(">>> In BaseAlcanceETL.ajustar_tipos_e_calculos (Antes do super)")
        super().ajustar_tipos_e_calculos()
        # Para Alcance não há transformações específicas além das gerais.
        return self.df

    def criar_veiculo(self):
        # Para plataformas genéricas, usamos o mecanismo genérico definido na BaseGeralETL.
        logging.debug(">>> In BaseAlcanceETL.criar_veiculo (Chamando super para atribuir Veiculo e ID)")
        super().criar_veiculo()
        return self.df

    def reordenar_colunas_para_modelo(self):
        logging.debug(">>> In BaseAlcanceETL.reordenar_colunas_para_modelo")
        self.df = reordenar_colunas_para_modelo(self.df, REACH_MODEL_COLUMN_ORDER)
        return self.df

    def processar(self, df_destino=None):
        logging.debug(">>> In processar (BaseAlcanceETL)")
        self.renomear_colunas_origem_para_modelo()
        self.ajustar_tipos_e_calculos()
        self.aplicar_substituicoes_objetivo()
        self.aplicar_parametrizacao_campanha_externa()
        self.determine_ad_preview_link()
        self.criar_veiculo()
        
        # Copiar os dados do campo "Reach" para a coluna "Alcance"
        if "Reach" in self.df.columns:
            logging.debug("Copiando dados da coluna 'Reach' para 'Alcance'")
            self.df["Alcance"] = self.df["Reach"]
        
        self.remover_colunas_indesejadas()
        self.reordenar_colunas_para_modelo()
        self.df = gerar_id(self.df)
        self.df = gerar_numeracao(self.df, df_destino, linha_insercao=2, coluna="Numero")
        
        # Filtra para manter somente as colunas definidas no modelo de Alcance, além da coluna "ID"
        valid_cols = set(REACH_MODEL_COLUMN_ORDER).union({"ID"})
        self.df = self.df[[col for col in self.df.columns if col in valid_cols]]
        
        logging.debug(">>> Final do processar (BaseAlcanceETL)")
        return self.df

# ---------- SUBCLASSES POR PLATAFORMA ----------

class MetaAlcanceETL(BaseAlcanceETL):
    def criar_veiculo(self):
        logging.debug(">>> In MetaAlcanceETL.criar_veiculo (Usando atribuir_veiculo_e_id_meta)")
        # Para Meta, utiliza a função específica que infere veículo a partir de Placement
        self.df = atribuir_veiculo_e_id_meta(self.df)
        return self.df

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

        # ✅ Aplica preenchimento com o mapeamento de criativo
        if 'utm_content' in self.df.columns:
            self.df = preencher_nomes_anuncio_linkedin(self.df, self.mapping_criativo)
        else:
            logging.warning("utm_content ausente, não será possível preencher nomes de anúncio.")

        return self.df



class PinterestAlcanceETL(BaseAlcanceETL):
    def ajustar_tipos_e_calculos(self):
        logging.debug(">>> In PinterestAlcanceETL.ajustar_tipos_e_calculos (Antes do super)")
        super().ajustar_tipos_e_calculos()
        self.df = generate_pinterest_dates(self.df)
        self.df = preencher_campos_com_campanha(self.df)
        return self.df
