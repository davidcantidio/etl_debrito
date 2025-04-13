import pandas as pd
import logging

from utils.campanha_mapper import buscar_mapping
from utils.renomeacoes import aplicar_substituicoes_objetivo, renomear_colunas_origem_para_modelo
from utils.numeracao import gerar_numeracao
from utils.datas import transformar_para_date, converter_data
from utils.preview_links import build_pinterest_preview_link
from utils.common_linkedin import preencher_nomes_anuncio_linkedin
from utils.atribuicoes_via_lookup import atribuir_veiculo_e_id_meta, atribuir_id_veiculo_generico
from utils.campos_calculados import calcular_engajamento_total, inicializar_colunas_auxiliares
from utils.normalize import  converter_colunas_numericas
from utils.atribuicoes_via_lookup import aplicar_parametrizacao_campanha
from utils.organizar_dataframe import remover_colunas_indesejadas
from utils.campos_calculados import gerar_id
from utils.preview_links import determine_meta_ad_preview_link, generate_linkedin_ad_preview_link_from_lookup

from utils.fields_lists import GENERAL_MODEL_COLUMN_ORDER, NUMERIC_COLUMNS


from utils.organizar_dataframe import reordenar_colunas_para_modelo


class BaseGeralETL:
    def __init__(self, df, id_veiculo, veiculo, mapping_campanha=None, mapping_sigla=None):
        logging.debug(">>> In BaseGeralETL.__init__")
        self.df = df.copy()
        self.id_veiculo = id_veiculo
        self.veiculo = veiculo
        self.mapping_campanha = mapping_campanha or {}
        self.mapping_sigla = mapping_sigla or {}

    def renomear_colunas_origem_para_modelo(self):
        self.df = renomear_colunas_origem_para_modelo(self.df)

    def ajustar_tipos_e_calculos(self):
        self.df = converter_data(self.df, 'Data')
        self.df = converter_colunas_numericas(self.df, NUMERIC_COLUMNS)
        self.df = calcular_engajamento_total(self.df)
        self.df = inicializar_colunas_auxiliares(self.df)

    def aplicar_substituicoes_objetivo(self):
        self.df = aplicar_substituicoes_objetivo(self.df)

    def aplicar_parametrizacao_campanha_externa(self):
        self.df = aplicar_parametrizacao_campanha(self.df, self.mapping_campanha, self.mapping_sigla)

    def criar_veiculo(self):
        logging.debug(">>> In criar_veiculo (default) com atribuição genérica")
        self.df['Veiculo'] = self.veiculo
        self.df = atribuir_id_veiculo_generico(self.df)

    def remover_colunas_indesejadas(self):
        self.df = remover_colunas_indesejadas(self.df)

    def reordenar_colunas_para_modelo(self):
        self.df = reordenar_colunas_para_modelo(self.df, GENERAL_MODEL_COLUMN_ORDER)

    def gerar_id(self):
        self.df = gerar_id(self.df)


    def processar(self, df_destino=None):
        logging.debug(">>> In processar (BaseGeralETL)")
        self.renomear_colunas_origem_para_modelo()
        self.ajustar_tipos_e_calculos()
        self.aplicar_substituicoes_objetivo()
        self.aplicar_parametrizacao_campanha_externa()
        self.determine_ad_preview_link()
        self.criar_veiculo()
        self.remover_colunas_indesejadas()
        self.reordenar_colunas_para_modelo()
        self.gerar_id()
        self.df = gerar_numeracao(self.df, df_destino, linha_insercao=2, coluna='Numero')
        return self.df

    def determine_ad_preview_link(self):
        pass


class MetaGeralETL(BaseGeralETL):
    def determine_ad_preview_link(self):
        self.df = determine_meta_ad_preview_link(self.df)
    
    def criar_veiculo(self):
        logging.debug(">>> In MetaGeralETL.criar_veiculo")
        self.df = atribuir_veiculo_e_id_meta(self.df)


class LinkedinGeralETL(BaseGeralETL):
    def __init__(self, *args, mapping_preview=None, mapping_criativo=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.mapping_preview = mapping_preview or {}
        self.mapping_criativo = mapping_criativo or {}

    def ajustar_tipos_e_calculos(self):
        super().ajustar_tipos_e_calculos()
        self.df = preencher_nomes_anuncio_linkedin(self.df, self.mapping_criativo)

    def determine_ad_preview_link(self):
        if 'ID_Content' not in self.df.columns:
            self.df['URL_do_Anuncio'] = ""
        else:
            self.df['URL_do_Anuncio'] = self.df['ID_Content'].map(self.mapping_preview).fillna("")


from utils.datas import generate_pinterest_dates
from utils.preview_links import generate_pinterest_ad_preview_link

class PinterestGeralETL(BaseGeralETL):
    def ajustar_tipos_e_calculos(self):
        super().ajustar_tipos_e_calculos()
        self.df = generate_pinterest_dates(self.df)
        self.df = generate_pinterest_ad_preview_link(self.df)


class TiktokGeralETL(BaseGeralETL):
    def ajustes_preview(self):
        logging.debug(">>> In TiktokGeralETL.ajustes_preview")
        logging.debug(f"Inicio_da_Campanha preview: {self.df['Inicio_da_Campanha'].dropna().head(3) if 'Inicio_da_Campanha' in self.df.columns else 'Não encontrada'}")
        logging.debug(f"Fim_da_Campanha preview: {self.df['Fim_da_Campanha'].dropna().head(3) if 'Fim_da_Campanha' in self.df.columns else 'Não encontrada'}")
