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
    Aplica apenas transformações genéricas, sem normalizações específicas.
    """

    def ajustar_tipos_e_calculos(self):
        logging.debug(">>> In BaseAlcanceETL.ajustar_tipos_e_calculos (Antes do super)")
        super().ajustar_tipos_e_calculos()
        # Não há transformações específicas para Alcance além das gerais.
        return self.df

    def criar_veiculo(self):
        logging.debug(">>> In BaseAlcanceETL.criar_veiculo (Chamando atribuir_veiculo_e_id_meta)")
        self.df = atribuir_veiculo_e_id_meta(self.df)
        # Após atribuir o veículo, renomeia 'Placement' para 'Posicionamento'
        if "Placement" in self.df.columns:
            logging.debug("Renomeando 'Placement' para 'Posicionamento'")
            self.df.rename(columns={"Placement": "Posicionamento"}, inplace=True)
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
        
        # Copiar o dado de "Reach" para "Alcance" antes de remover colunas indesejadas
        if "Reach" in self.df.columns:
            logging.debug("Copiando dados da coluna 'Reach' para 'Alcance'")
            self.df["Alcance"] = self.df["Reach"]
        
        self.remover_colunas_indesejadas()
        self.reordenar_colunas_para_modelo()
        self.df = gerar_id(self.df)
        self.df = gerar_numeracao(self.df, df_destino, linha_insercao=2, coluna="Numero")
        
        # Filtra para manter somente as colunas definidas no modelo de alcance, além da coluna 'ID'
        valid_cols = set(REACH_MODEL_COLUMN_ORDER).union({"ID"})
        self.df = self.df[[col for col in self.df.columns if col in valid_cols]]
        
        logging.debug(">>> Final do processar (BaseAlcanceETL)")
        return self.df


# ---------- SUBCLASSES POR PLATAFORMA ----------

class MetaAlcanceETL(BaseAlcanceETL):
    pass

class TikTokAlcanceETL(BaseAlcanceETL):
    pass

class LinkedinAlcanceETL(BaseAlcanceETL):
    pass

class PinterestAlcanceETL(BaseAlcanceETL):
    def ajustar_tipos_e_calculos(self):
        logging.debug(">>> In PinterestAlcanceETL.ajustar_tipos_e_calculos (Antes do super)")
        super().ajustar_tipos_e_calculos()
        self.df = generate_pinterest_dates(self.df)
        self.df = preencher_campos_com_campanha(self.df)
        return self.df
