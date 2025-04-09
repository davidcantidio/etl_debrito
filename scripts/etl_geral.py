import pandas as pd
import numpy as np
import logging

from utils.campanha_mapper import buscar_mapping
from utils.objetivos import SUBSTITUICOES_OBJETIVO
from utils.numeracao import gerar_numeracao
from utils.datas import transformar_para_date
from utils.preview_links import construir_preview_link_pinterest, ajustar_preview_link
from utils.get_nome_campanha import obter_nome_por_utm_content
from utils.normalize import inferir_veiculo_meta_por_placement
from utils.google_sheets import carregar_aba_google_sheets
from utils.atribuicao_veiculo_nome_e_id import (
    atribuir_veiculo_e_id_meta,
    atribuir_id_veiculo_generico,
)


class BaseGeralETL:
    def __init__(self, df, id_veiculo, veiculo, mapping_campanha=None, mapping_sigla=None):
        logging.debug(">>> In BaseGeralETL.__init__")
        self.df = df.copy()
        self.id_veiculo = id_veiculo
        self.veiculo = veiculo
        self.mapping_campanha = mapping_campanha or {}
        self.mapping_sigla = mapping_sigla or {}
        self.substituicoes = {'Objetivo': SUBSTITUICOES_OBJETIVO}

    def renomear_colunas_origem_para_modelo(self):
        renomear = {
            'Date': 'Data', 'Account name': 'Nome_da_Conta', 'Advertiser name': 'Nome_da_Conta',
            'Campaign name': 'Campaign_name', 'Ad group name': 'Nome_do_Conjunto_de_Anuncio',
            'Ad set name': 'Nome_do_Conjunto_de_Anuncio', 'Ad name': 'Nome_do_Anuncio',
            'Campaign ID': 'Campaign_ID', 'Start': 'Inicio_da_Campanha', 'End': 'Fim_da_Campanha',
            'Campaign objective type': 'Objetivo', 'Campaign objective': 'Objetivo',
            'Placement': 'Placement', 'Preview Link': 'URL_do_Anuncio',
            'Content (utm)': 'ID_Content', 'Impressions': 'Impressoes', 'Cost': 'Investimento',
            'Link clicks': 'Cliques_no_Link', 'Clicks': 'Cliques_no_Link',
            'Video play actions': 'Video_Play', 'Video views': 'Video_Play',
            'Video watches at 25%': 'Visualizacoes_ate_25', 'Video watches at 50%': 'Visualizacoes_ate_50',
            'Video watches at 75%': 'Visualizacoes_ate_75', 'Video watches at 100%': 'Visualizacoes_ate_100',
            'Post reactions': 'Reacoes', 'Paid likes': 'Reacoes', 'Post shares': 'Compartilhamentos',
            'Paid shares': 'Compartilhamentos', 'Post comments': 'Comentarios', 'Paid comments': 'Comentarios'
        }
        self.df.rename(columns=renomear, inplace=True)
        logging.debug(f"Colunas após renomear: {list(self.df.columns)}")

    def ajustar_tipos_e_calculos(self):
        if 'Data' in self.df.columns:
            self.df['Data'] = pd.to_datetime(self.df['Data'], errors='coerce')

        numericas = ['Impressoes', 'Investimento', 'Cliques_no_Link', 'Video_Play',
                     'Visualizacoes_ate_25', 'Visualizacoes_ate_50', 'Visualizacoes_ate_75', 'Visualizacoes_ate_100',
                     'Reacoes', 'Compartilhamentos', 'Comentarios']
        for col in numericas:
            if col in self.df.columns:
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce').fillna(0)

        if all(x in self.df.columns for x in ['Reacoes', 'Compartilhamentos', 'Comentarios']):
            self.df['Engajamento_Total'] = self.df['Reacoes'] + self.df['Compartilhamentos'] + self.df['Comentarios']
        else:
            self.df['Engajamento_Total'] = 0

        self.df['Numero'] = self.df.get('Numero', np.nan)
        self.df['ID'] = self.df.get('ID', np.nan)

    def aplicar_substituicoes_objetivo(self):
        if 'Objetivo' in self.df.columns:
            for old, new in self.substituicoes['Objetivo'].items():
                self.df.loc[self.df['Objetivo'] == old, 'Objetivo'] = new

    def aplicar_parametrizacao_campanha_externa(self):
        if 'Campaign_name' not in self.df.columns:
            self.df['Campanha'] = ""
            self.df['ID_Campanha'] = ""
            return

        self.df['Campanha'] = self.df['Campaign_name'].apply(lambda x: buscar_mapping(self.mapping_campanha, x) or x)
        self.df['ID_Campanha'] = self.df['Campaign_name'].apply(lambda x: buscar_mapping(self.mapping_sigla, x))

    def criar_veiculo(self):
        logging.debug(">>> In criar_veiculo (default)")
        self.df['Veiculo'] = self.veiculo
        self.df['ID_Veiculo'] = self.id_veiculo

    def remover_colunas_indesejadas(self):
        for col in ['Placement', 'Campaign_ID', 'Campaign_name', 'Content_utm']:
            if col in self.df.columns:
                self.df.drop(columns=col, inplace=True)

    def reordenar_colunas_para_modelo(self):
        ordem = [
            'Numero', 'Data', 'Nome_da_Conta', 'Campanha', 'ID_Campanha', 'Veiculo', 'ID_Veiculo',
            'Nome_do_Conjunto_de_Anuncio', 'Nome_do_Anuncio', 'Inicio_da_Campanha', 'Fim_da_Campanha',
            'Objetivo', 'URL_do_Anuncio', 'ID_Content', 'Investimento', 'Impressoes', 'Cliques_no_Link',
            'Video_Play', 'Visualizacoes_ate_25', 'Visualizacoes_ate_50', 'Visualizacoes_ate_75', 'Visualizacoes_ate_100',
            'Reacoes', 'Compartilhamentos', 'Comentarios', 'Engajamento_Total', 'ID'
        ]
        for col in ordem:
            if col not in self.df.columns:
                self.df[col] = ""
        self.df = self.df[ordem]

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
                lambda row: ajustar_preview_link(row['URL_do_Anuncio'], row['Preview_Link_FB']), axis=1
            )

    def criar_veiculo(self):
        logging.debug(">>> In MetaIdadeETL.criar_veiculo")
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

        def debug_nome(utm):
            utm_str = str(utm).strip()
            nome = obter_nome_por_utm_content(utm_str, self.mapping_criativo)
            if not nome:
                logging.debug(f"utm_content '{utm_str}' N\u00c3O encontrado no mapping_criativo.")
            return nome

        self.df['Nome_do_Anuncio'] = self.df['ID_Content'].apply(debug_nome)
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
            self.df['URL_do_Anuncio'] = self.df[match_col[0]].apply(construir_preview_link_pinterest)
        else:
            self.df['URL_do_Anuncio'] = ""


class TiktokGeralETL(BaseGeralETL):
    def ajustes_preview(self):
        logging.debug(">>> In TiktokGeralETL.ajustes_preview")
        logging.debug(f"Inicio_da_Campanha preview: {self.df['Inicio_da_Campanha'].dropna().head(3) if 'Inicio_da_Campanha' in self.df.columns else 'Não encontrada'}")
        logging.debug(f"Fim_da_Campanha preview: {self.df['Fim_da_Campanha'].dropna().head(3) if 'Fim_da_Campanha' in self.df.columns else 'Não encontrada'}")

