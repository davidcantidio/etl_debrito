import pandas as pd
import numpy as np
import logging

from utils.campanha_mapper import buscar_mapping
from utils.renomeacoes import aplicar_substituicoes_objetivo, renomear_colunas_origem_para_modelo
from utils.numeracao import gerar_numeracao
from utils.datas import transformar_para_date, converter_data
from utils.preview_links import build_pinterest_preview_link, select_meta_preview_link
from utils.common_linkedin import buscar_nome_criativo_com_log
from utils.atribuicoes_via_lookup import atribuir_veiculo_e_id_meta, atribuir_id_veiculo_generico, aplicar_parametrizacao_campanha
from utils.campos_calculados import calcular_engajamento_total, inicializar_colunas_auxiliares
from utils.normalize import converter_colunas_numericas
from utils.organizar_dataframe import remover_colunas_indesejadas, reordenar_colunas_para_modelo


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
        colunas_numericas = [
            'Impressoes', 'Investimento', 'Cliques_no_Link', 'Video_Play',
            'Visualizacoes_ate_25', 'Visualizacoes_ate_50', 'Visualizacoes_ate_75', 'Visualizacoes_ate_100',
            'Reacoes', 'Compartilhamentos', 'Comentarios'
        ]
        self.df = converter_colunas_numericas(self.df, colunas_numericas)
        self.df = calcular_engajamento_total(self.df)
        self.df = inicializar_colunas_auxiliares(self.df)

    def aplicar_substituicoes_objetivo(self):
        self.df = aplicar_substituicoes_objetivo(self.df)

    def aplicar_parametrizacao_campanha_externa(self):
        logging.debug(">>> In aplicar_parametrizacao_campanha_externa")
        self.df = aplicar_parametrizacao_campanha(self.df, self.mapping_campanha, self.mapping_sigla)

    def criar_veiculo(self):
        logging.debug(">>> In criar_veiculo (default) com atribuição genérica")
        self.df['Veiculo'] = self.veiculo
        self.df = atribuir_id_veiculo_generico(self.df)

    def remover_colunas_indesejadas(self):
        self.df = remover_colunas_indesejadas(self.df)

    def reordenar_colunas_para_modelo(self):
        self.df = reordenar_colunas_para_modelo(self.df)

    def gerar_id(self):
        self.df['ID'] = self.df.apply(
            lambda row: f"{row['Data']}-{row['Campanha']}-{row['Impressoes']}-{row['Investimento']}-{row['Cliques_no_Link']}",
            axis=1
        )

    def processar(self, df_destino=None):
        logging.debug(">>> In processar (BaseGeralETL)")
        self.renomear_colunas_origem_para_modelo()
        self.ajustar_tipos_e_calculos()
        self.aplicar_substituicoes_objetivo()
        self.aplicar_parametrizacao_campanha_externa()
        self.ajustes_preview()
        self.criar_veiculo()
        self.remover_colunas_indesejadas()
        self.reordenar_colunas_para_modelo()
        self.gerar_id()
        self.df = gerar_numeracao(self.df, df_destino, linha_insercao=2, coluna='Numero')
        return self.df

    def ajustes_preview(self):
        pass


class MetaGeralETL(BaseGeralETL):
    def ajustes_preview(self):
        if 'Preview Link FB' in self.df.columns:
            self.df.rename(columns={'Preview Link FB': 'Preview_Link_FB'}, inplace=True)
        if 'URL_do_Anuncio' in self.df.columns and 'Preview_Link_FB' in self.df.columns:
            self.df['URL_do_Anuncio'] = self.df.apply(
                lambda row: select_meta_preview_link(row['URL_do_Anuncio'], row['Preview_Link_FB']), axis=1
            )

    def criar_veiculo(self):
        logging.debug(">>> In MetaGeralETL.criar_veiculo")
        self.df = atribuir_veiculo_e_id_meta(self.df)


class LinkedinGeralETL(BaseGeralETL):
    def __init__(self, *args, mapping_preview=None, mapping_criativo=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.mapping_preview = mapping_preview or {}
        self.mapping_criativo = mapping_criativo or {}

    def ajustes_preview(self):
        if 'ID_Content' not in self.df.columns:
            self.df['URL_do_Anuncio'] = ""
            self.df['Nome_do_Anuncio'] = ""
            self.df['Nome_do_Conjunto_de_Anuncio'] = ""
            return

        self.df['URL_do_Anuncio'] = self.df['ID_Content'].apply(
            lambda x: self.mapping_preview.get(str(x).strip(), "")
        )
        self.df['Nome_do_Anuncio'] = self.df['ID_Content'].apply(
            lambda utm: buscar_nome_criativo_com_log(utm, self.mapping_criativo)
        )
        self.df['Nome_do_Conjunto_de_Anuncio'] = self.df['Nome_do_Anuncio']


class PinterestGeralETL(BaseGeralETL):
    def ajustes_preview(self):
        if 'start' in self.df.columns:
            self.df['Inicio_da_Campanha'] = self.df['start'].apply(transformar_para_date)
        else:
            self.df['Inicio_da_Campanha'] = ""

        if 'end' in self.df.columns:
            self.df['Fim_da_Campanha'] = self.df['end'].apply(transformar_para_date)
        else:
            self.df['Fim_da_Campanha'] = ""

        match_col = [col for col in self.df.columns if col.strip().lower() == 'preview link']
        if match_col:
            self.df['URL_do_Anuncio'] = self.df[match_col[0]].apply(build_pinterest_preview_link)
        else:
            self.df['URL_do_Anuncio'] = ""


class TiktokGeralETL(BaseGeralETL):
    def ajustes_preview(self):
        logging.debug(">>> In TiktokGeralETL.ajustes_preview")
        logging.debug(f"Inicio_da_Campanha preview: {self.df['Inicio_da_Campanha'].dropna().head(3) if 'Inicio_da_Campanha' in self.df.columns else 'Não encontrada'}")
        logging.debug(f"Fim_da_Campanha preview: {self.df['Fim_da_Campanha'].dropna().head(3) if 'Fim_da_Campanha' in self.df.columns else 'Não encontrada'}")
