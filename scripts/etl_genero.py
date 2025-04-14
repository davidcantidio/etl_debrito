# etl_genero.py
import logging
from scripts.etl_geral import BaseGeralETL
from utils.fields_lists import GENDER_MODEL_COLUMN_ORDER
from utils.organizar_dataframe import reordenar_colunas_para_modelo
from utils.normalize import normalizar_genero
from utils.atribuicoes_via_lookup import atribuir_veiculo_e_id_meta
from utils.common_pinterest import preencher_campos_com_campanha
from utils.datas import generate_pinterest_dates
from utils.numeracao import gerar_numeracao

class BaseGeneroETL(BaseGeralETL):
    """
    Classe base para ETLs de Gênero.
    """
    def ajustar_tipos_e_calculos(self):
        logging.debug(">>> In BaseGeneroETL.ajustar_tipos_e_calculos (Antes do super)")
        super().ajustar_tipos_e_calculos()
        logging.debug(f"[DEBUG] Colunas após super().ajustar_tipos_e_calculos: {list(self.df.columns)}")
        
        # Renomeia 'Gender' para 'Genero', se existir
        if 'Gender' in self.df.columns:
            logging.debug("[DEBUG] Renomeando 'Gender' -> 'Genero'")
            self.df.rename(columns={'Gender': 'Genero'}, inplace=True)
        
        # Aplica normalização no campo 'Genero'
        if 'Genero' in self.df.columns:
            logging.debug("[DEBUG] Normalizando campo 'Genero'")
            self.df['Genero'] = self.df['Genero'].apply(normalizar_genero)
        
        return self.df

    def criar_veiculo(self):
        logging.debug(">>> In BaseGeneroETL.criar_veiculo (Chamando super para atribuir Veiculo e ID)")
        super().criar_veiculo()
        return self.df

    def reordenar_colunas_para_modelo(self):
        logging.debug(">>> In BaseGeneroETL.reordenar_colunas_para_modelo")
        self.df = reordenar_colunas_para_modelo(self.df, GENDER_MODEL_COLUMN_ORDER)
        return self.df

    def processar(self, df_destino=None):
        """
        Sobrescreve o método processar de BaseGeralETL para controlar a ordem das etapas.
        """
        logging.debug(">>> In processar (BaseGeneroETL)")
        
        # Passo 1: Renomear colunas de origem para o modelo padrão
        self.renomear_colunas_origem_para_modelo()
        logging.debug(f"[DEBUG] Colunas após renomear_colunas_origem_para_modelo: {list(self.df.columns)}")
        
        # Passo 2: Ajustar tipos e realizar cálculos adicionais
        self.ajustar_tipos_e_calculos()
        logging.debug(f"[DEBUG] Colunas após ajustar_tipos_e_calculos: {list(self.df.columns)}")
        
        # Passo 3: Aplicar substituições para a coluna 'Objetivo'
        self.aplicar_substituicoes_objetivo()
        logging.debug(f"[DEBUG] Colunas após aplicar_substituicoes_objetivo: {list(self.df.columns)}")
        
        # Passo 4: Aplicar parametrização de campanha (lookup para Campanha e ID_Campanha)
        self.aplicar_parametrizacao_campanha_externa()
        logging.debug(f"[DEBUG] Colunas após aplicar_parametrizacao_campanha_externa: {list(self.df.columns)}")
        
        # Passo 5: Determinar o preview link (pode ser sobrescrito pelas subclasses)
        self.determine_ad_preview_link()
        logging.debug(f"[DEBUG] Colunas após determine_ad_preview_link: {list(self.df.columns)}")
        
        # Passo 6: Criar/atribuir Veiculo e ID_Veiculo
        self.criar_veiculo()
        logging.debug(f"[DEBUG] Colunas após criar_veiculo: {list(self.df.columns)}")
        
        # Passo 7: Remover colunas indesejadas
        self.remover_colunas_indesejadas()
        logging.debug(f"[DEBUG] Colunas após remover_colunas_indesejadas: {list(self.df.columns)}")
        
        # Passo 8: Reordenar as colunas conforme o modelo de Gênero
        self.reordenar_colunas_para_modelo()
        logging.debug(f"[DEBUG] Colunas após reordenar_colunas_para_modelo: {list(self.df.columns)}")
        
        # Passo 9: Gerar ID (a partir da concatenação de campos chave)
        self.gerar_id()
        logging.debug(f"[DEBUG] Colunas após gerar_id: {list(self.df.columns)}")
        
        # Passo 10: Gerar numeração sequencial dos registros (append only)
        self.df = gerar_numeracao(self.df, df_destino, linha_insercao=2, coluna='Numero')
        logging.debug(">>> Final do processar (BaseGeneroETL)")
        
        return self.df

# Classes específicas para cada plataforma

class MetaGeneroETL(BaseGeneroETL):
    def criar_veiculo(self):
        logging.debug(">>> In MetaGeneroETL.criar_veiculo (Chamando atribuir_veiculo_e_id_meta)")
        self.df = atribuir_veiculo_e_id_meta(self.df)
        return self.df

class TikTokGeneroETL(BaseGeneroETL):
    pass

class LinkedinGeneroETL(BaseGeneroETL):
    pass

class PinterestGeneroETL(BaseGeneroETL):
    """
    Subclasse para tratar peculiaridades do Pinterest na dimensão Gênero.
    A principal diferença é que precisamos gerar datas específicas do Pinterest
    e preencher os campos Nome_do_Conjunto_de_Anuncio / Nome_do_Anuncio utilizando 'Campaign name'
    (mantendo 'Campanha' e 'ID_Campanha' vindos do lookup).
    """
    def ajustar_tipos_e_calculos(self):
        logging.debug(">>> In PinterestGeneroETL.ajustar_tipos_e_calculos (Antes de super)")
        super().ajustar_tipos_e_calculos()
        logging.debug("[DEBUG] Gerando datas do Pinterest")
        self.df = generate_pinterest_dates(self.df)
        logging.debug(f"[DEBUG] Colunas após generate_pinterest_dates: {list(self.df.columns)}")
        logging.debug("[DEBUG] Aplicando preencher_campos_com_campanha para Nome_do_Conjunto_de_Anuncio e Nome_do_Anuncio")
        self.df = preencher_campos_com_campanha(self.df)
        logging.debug(f"[DEBUG] Colunas após preencher_campos_com_campanha: {list(self.df.columns)}")
        return self.df
