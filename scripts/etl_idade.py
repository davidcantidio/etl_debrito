# etl_idade.py
import logging
from scripts.etl_geral import BaseGeralETL
from utils.fields_lists import AGE_MODEL_COLUMN_ORDER
from utils.organizar_dataframe import reordenar_colunas_para_modelo
from utils.normalize import normalizar_faixa_etaria
from utils.atribuicoes_via_lookup import atribuir_veiculo_e_id_meta
from utils.common_pinterest import preencher_campos_com_campanha
from utils.datas import generate_pinterest_dates
from utils.numeracao import gerar_numeracao

class BaseIdadeETL(BaseGeralETL):
    """
    Classe base para ETLs de Idade.
    """

    def ajustar_tipos_e_calculos(self):
        logging.debug(">>> In BaseIdadeETL.ajustar_tipos_e_calculos (Antes do super)")
        super().ajustar_tipos_e_calculos()
        logging.debug(f"[DEBUG] Colunas após super().ajustar_tipos_e_calculos: {list(self.df.columns)}")

        # Renomeia 'Faixa_Etaria' para 'Idade', se existir
        if 'Faixa_Etaria' in self.df.columns:
            logging.debug("[DEBUG] Renomeando 'Faixa_Etaria' -> 'Idade'")
            self.df.rename(columns={'Faixa_Etaria': 'Idade'}, inplace=True)

        # Aplica normalização no campo 'Idade'
        if 'Idade' in self.df.columns:
            logging.debug("[DEBUG] Normalizando campo 'Idade'")
            self.df['Idade'] = self.df['Idade'].apply(normalizar_faixa_etaria)

        return self.df
    
    def criar_veiculo(self):
        logging.debug(">>> In BaseIdadeETL.criar_veiculo (Chamando super para atribuir Veiculo e ID)")
        super().criar_veiculo()
        return self.df
    
    def reordenar_colunas_para_modelo(self):
        logging.debug(">>> In BaseIdadeETL.reordenar_colunas_para_modelo")
        self.df = reordenar_colunas_para_modelo(self.df, AGE_MODEL_COLUMN_ORDER)
        return self.df

    def processar(self, df_destino=None):
        """
        Sobrescreve o método processar de BaseGeralETL para controlar a ordem das etapas
        (caso seja necessário). Aqui mantemos o fluxo original da superclasse, mas adicionamos logs.
        """
        logging.debug(">>> In processar (BaseIdadeETL)")

        # Passo 1) renomear colunas
        self.renomear_colunas_origem_para_modelo()
        logging.debug(f"[DEBUG] Colunas após renomear_colunas_origem_para_modelo: {list(self.df.columns)}")

        # Passo 2) ajustar tipos e cálculos
        self.ajustar_tipos_e_calculos()
        logging.debug(f"[DEBUG] Colunas após ajustar_tipos_e_calculos: {list(self.df.columns)}")

        # Passo 3) substituições objetivo
        self.aplicar_substituicoes_objetivo()
        logging.debug(f"[DEBUG] Colunas após aplicar_substituicoes_objetivo: {list(self.df.columns)}")

        # Passo 4) lookup de campanha e ID_Campanha
        self.aplicar_parametrizacao_campanha_externa()
        logging.debug(f"[DEBUG] Colunas após aplicar_parametrizacao_campanha_externa: {list(self.df.columns)}")

        # Passo 5) determine preview link (vazio ou sobrescrito por subclasses)
        self.determine_ad_preview_link()
        logging.debug(f"[DEBUG] Colunas após determine_ad_preview_link: {list(self.df.columns)}")

        # Passo 6) criar_veiculo
        self.criar_veiculo()
        logging.debug(f"[DEBUG] Colunas após criar_veiculo: {list(self.df.columns)}")

        # Passo 7) remover colunas indesejadas
        self.remover_colunas_indesejadas()
        logging.debug(f"[DEBUG] Colunas após remover_colunas_indesejadas: {list(self.df.columns)}")

        # Passo 8) reordenar colunas para o modelo de idade
        self.reordenar_colunas_para_modelo()
        logging.debug(f"[DEBUG] Colunas após reordenar_colunas_para_modelo: {list(self.df.columns)}")

        # Passo 9) gerar ID
        self.gerar_id()
        logging.debug(f"[DEBUG] Colunas após gerar_id: {list(self.df.columns)}")

        # Passo 10) gerar numeracao (append only)
        self.df = gerar_numeracao(self.df, df_destino, linha_insercao=2, coluna='Numero')

        logging.debug(">>> Final do processar (BaseIdadeETL)")
        return self.df


# Classes específicas para cada plataforma

class MetaIdadeETL(BaseIdadeETL):
    def criar_veiculo(self):
        logging.debug(">>> In MetaIdadeETL.criar_veiculo (Chamando atribuir_veiculo_e_id_meta)")
        self.df = atribuir_veiculo_e_id_meta(self.df)
        return self.df


class TikTokIdadeETL(BaseIdadeETL):
    pass


class LinkedinIdadeETL(BaseIdadeETL):
    pass


class PinterestIdadeETL(BaseIdadeETL):
    """
    Subclasse para tratar peculiaridades do Pinterest na dimensão Idade.
    A principal diferença é que precisamos gerar datas específicas do Pinterest
    e preencher Nome_do_Conjunto_de_Anuncio / Nome_do_Anuncio usando 'Campaign name'
    (mantendo 'Campanha' e 'ID_Campanha' vindos do lookup).
    """

    def ajustar_tipos_e_calculos(self):
        logging.debug(">>> In PinterestIdadeETL.ajustar_tipos_e_calculos (Antes de super)")
        super().ajustar_tipos_e_calculos()
        logging.debug("[DEBUG] Gerando datas do Pinterest")
        self.df = generate_pinterest_dates(self.df)
        logging.debug(f"[DEBUG] Colunas após generate_pinterest_dates: {list(self.df.columns)}")

        logging.debug("[DEBUG] Aplicando preencher_campos_com_campanha para Nome_do_Conjunto_de_Anuncio e Nome_do_Anuncio")
        self.df = preencher_campos_com_campanha(self.df)
        logging.debug(f"[DEBUG] Colunas após preencher_campos_com_campanha: {list(self.df.columns)}")

        return self.df
