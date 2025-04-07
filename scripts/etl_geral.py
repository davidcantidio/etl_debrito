# etl_geral.py
import pandas as pd
import numpy as np
import logging

from utils.campanha_mapper import buscar_mapping
from utils.objetivos import SUBSTITUICOES_OBJETIVO
from utils.numeracao import gerar_numeracao

# A definição da classe BaseGeralETL permanece inalterada:
class BaseGeralETL:
    """
    Classe base para processamento de dados 'gerais' de diferentes plataformas (Meta, TikTok, LinkedIn, etc.).
    """
    def __init__(
        self,
        df: pd.DataFrame,
        id_veiculo: int,
        veiculo: str,
        mapping_campanha: dict = None,
        mapping_sigla: dict = None
    ):
        self.df = df.copy()
        self.id_veiculo = id_veiculo
        self.veiculo = veiculo
        self.mapping_campanha = mapping_campanha or {}
        self.mapping_sigla = mapping_sigla or {}
        self.substituicoes = {
            'Objetivo': SUBSTITUICOES_OBJETIVO
        }

    def renomear_colunas_origem_para_modelo(self):
        renomear = {
            'Date': 'Data',
            'Account name': 'Nome_da_Conta',
            'Advertiser name': 'Nome_da_Conta',
            'Campaign name': 'Campaign_name',  # usado para lookup, depois removido
            'Ad group name': 'Nome_do_Conjunto_de_Anuncio',
            'Ad set name': 'Nome_do_Conjunto_de_Anuncio',
            'Ad name': 'Nome_do_Anuncio',
            'Campaign ID': 'Campaign_ID',
            'Start': 'Inicio_da_Campanha',
            'End': 'Fim_da_Campanha',
            'Campaign objective type': 'Objetivo',
            'Campaign objective': 'Objetivo',
            'Placement': 'Placement',
            'Preview Link': 'URL_do_Anuncio',    # Mapeia Preview Link para URL_do_Anuncio
            'Content (utm)': 'ID_Content',        # Mapeia Content (utm) para ID_Content
            'Impressions': 'Impressoes',
            'Cost': 'Investimento',
            'Link clicks': 'Cliques_no_Link',
            'Clicks': 'Cliques_no_Link',
            'Video play actions': 'Video_Play',
            'Video views': 'Video_Play',
            'Video watches at 25%': 'Visualizacoes_ate_25',
            'Video watches at 50%': 'Visualizacoes_ate_50',
            'Video watches at 75%': 'Visualizacoes_ate_75',
            'Video watches at 100%': 'Visualizacoes_ate_100',
            'Post reactions': 'Reacoes',
            'Paid likes': 'Reacoes',
            'Post shares': 'Compartilhamentos',
            'Paid shares': 'Compartilhamentos',
            'Post comments': 'Comentarios',
            'Paid comments': 'Comentarios'
        }
        self.df.rename(columns=renomear, inplace=True)

    def ajustar_tipos_e_calculos(self):
        if 'Data' in self.df.columns:
            self.df['Data'] = pd.to_datetime(self.df['Data'], errors='coerce')
        numeric_cols = [
            'Impressoes', 'Investimento', 'Cliques_no_Link',
            'Video_Play', 'Visualizacoes_ate_25', 'Visualizacoes_ate_50',
            'Visualizacoes_ate_75', 'Visualizacoes_ate_100',
            'Reacoes', 'Compartilhamentos', 'Comentarios'
        ]
        for col in numeric_cols:
            if col in self.df.columns:
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce').fillna(0)
        cols_engajamento = ['Reacoes', 'Compartilhamentos', 'Comentarios']
        if all(col in self.df.columns for col in cols_engajamento):
            self.df['Engajamento_Total'] = self.df['Reacoes'] + self.df['Compartilhamentos'] + self.df['Comentarios']
        else:
            self.df['Engajamento_Total'] = 0
        if 'Numero' not in self.df.columns:
            self.df['Numero'] = np.nan
        if 'ID' not in self.df.columns:
            self.df['ID'] = np.nan

    def aplicar_substituicoes_objetivo(self):
        col_obj = 'Objetivo'
        if col_obj in self.df.columns:
            for old_val, new_val in self.substituicoes['Objetivo'].items():
                mask = self.df[col_obj] == old_val
                self.df.loc[mask, col_obj] = new_val
        else:
            logging.warning("Nenhuma coluna 'Objetivo' encontrada para substituições.")

    def aplicar_parametrizacao_campanha_externa(self):
        if 'Campaign_name' not in self.df.columns:
            logging.warning("Coluna 'Campaign_name' não encontrada para lookup de campanha.")
            self.df['Campanha'] = ""
            self.df['ID_Campanha'] = ""
            return
        self.df['Campanha'] = self.df['Campaign_name'].apply(
            lambda x: buscar_mapping(self.mapping_campanha, x) or x
        )
        self.df['ID_Campanha'] = self.df['Campaign_name'].apply(
            lambda x: buscar_mapping(self.mapping_sigla, x)
        )

    def criar_veiculo(self):
        self.df['Veiculo'] = self.veiculo
        self.df['ID_Veiculo'] = self.id_veiculo

    def remover_colunas_indesejadas(self):
        cols_to_drop = ['Placement', 'Campaign_ID', 'Campaign_name', 'Content_utm']
        for c in cols_to_drop:
            if c in self.df.columns:
                self.df.drop(columns=[c], inplace=True)

    def reordenar_colunas_para_modelo(self):
        ordem = [
            'Numero',
            'Data',
            'Nome_da_Conta',
            'Campanha',
            'ID_Campanha',
            'Veiculo',
            'ID_Veiculo',
            'Nome_do_Conjunto_de_Anuncio',
            'Nome_do_Anuncio',
            'Inicio_da_Campanha',
            'Fim_da_Campanha',
            'Objetivo',
            'URL_do_Anuncio',
            'ID_Content',
            'Investimento',
            'Impressoes',
            'Cliques_no_Link',
            'Video_Play',
            'Visualizacoes_ate_25',
            'Visualizacoes_ate_50',
            'Visualizacoes_ate_75',
            'Visualizacoes_ate_100',
            'Reacoes',
            'Compartilhamentos',
            'Comentarios',
            'Engajamento_Total',
            'ID'
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

    def processar(self) -> pd.DataFrame:
        # Primeiro, renomeia as colunas para o modelo (incluindo deixar "Preview Link FB" intacto)
        self.renomear_colunas_origem_para_modelo()
        # Agora, ajuste o preview link antes que a coluna auxiliar seja removida
        self.ajustar_preview_link_meta()
        # Prossegue com as demais etapas do ETL
        self.ajustar_tipos_e_calculos()
        self.aplicar_substituicoes_objetivo()
        self.aplicar_parametrizacao_campanha_externa()
        self.criar_veiculo()
        self.remover_colunas_indesejadas()
        self.reordenar_colunas_para_modelo()
        self.gerar_id()
        self.df = gerar_numeracao(self.df, numero_inicial=1, coluna='Numero')
        return self.df

# ------------------------------------------------------------
# Classe específica para Meta que inclui o ajuste do Preview Link
# ------------------------------------------------------------
class MetaGeralETL(BaseGeralETL):
    def ajustar_preview_link_meta(self):
        logging.debug("Executando ajuste do preview link na MetaGeralETL")
        # Se existir a coluna 'Preview Link FB', renomeia para 'Preview_Link_FB'
        if 'Preview Link FB' in self.df.columns:
            self.df.rename(columns={'Preview Link FB': 'Preview_Link_FB'}, inplace=True)
        try:
            from utils.preview_link_meta import ajustar_preview_link
        except ImportError:
            logging.error("Não foi possível importar a função ajustar_preview_link de utils.preview_link_meta")
            return self.df
        if 'URL_do_Anuncio' in self.df.columns and 'Preview_Link_FB' in self.df.columns:
            self.df['URL_do_Anuncio'] = self.df.apply(
                lambda row: ajustar_preview_link(row['URL_do_Anuncio'], row['Preview_Link_FB']),
                axis=1
            )
        return self.df



# Outras subclasses permanecem inalteradas
class TiktokGeralETL(BaseGeralETL):
    pass

class LinkedinGeralETL(BaseGeralETL):
    pass

class PinterestGeralETL(BaseGeralETL):
    pass
