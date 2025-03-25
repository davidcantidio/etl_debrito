import pandas as pd
import numpy as np
import re
import unicodedata
import logging
from utils.campanha_mapper import buscar_mapping


class etl_geral_tk:
    def __init__(self, df):
        self.df = df.copy()
        # Substituições agora contém somente 'Objetivo'
        self.substituicoes = {
            'Objetivo': {
                'REACH': 'Alcance',
                'VIDEO_VIEWS': 'Visualização',
                'TRAFFIC': 'Tráfego',
                'TOPVIEW-BR-20241109': 'Alcance'
            }
        }

    def ajustar_tipos(self):
        self.df['Date'] = pd.to_datetime(self.df['Date'], errors='coerce')
        self.df['Schedule start time'] = pd.to_datetime(self.df['Schedule start time'], errors='coerce')
        self.df['Schedule end time'] = pd.to_datetime(self.df['Schedule end time'], errors='coerce')
        self.df['Cost'] = pd.to_numeric(self.df['Cost'], errors='coerce').round(2)

        self.df['Paid shares'] = pd.to_numeric(self.df['Paid shares'], errors='coerce')
        self.df['Paid likes'] = pd.to_numeric(self.df['Paid likes'], errors='coerce')
        self.df['Paid comments'] = pd.to_numeric(self.df['Paid comments'], errors='coerce')
        self.df.fillna({'Paid shares': 0, 'Paid likes': 0, 'Paid comments': 0}, inplace=True)

        self.df['Engajamento_Total'] = self.df['Paid shares'] + self.df['Paid comments'] + self.df['Paid likes']

        self.df['Numero'] = np.nan
        self.df.fillna({'Numero': 0}, inplace=True)

        self.df['Veiculo'] = 'Tiktok'
        self.df['ID'] = pd.Series(dtype='str')

    def atribuir_id_veiculo(self):
        # Atribui o ID_Veiculo fixo para Tiktok
        self.df['ID_Veiculo'] = 5

    def etl_dicionario(self, coluna_origem, coluna_destino, substituicoes_coluna):
        """
        Substitui valores em coluna_destino, caso encontre a chave em coluna_origem.
        """
        self.df[coluna_destino] = self.df[coluna_origem].apply(
            lambda x: next((valor for chave, valor in substituicoes_coluna.items() if chave in x), x)
        )

    def aplicar_substituicoes(self):
        """
        Aplica apenas as substituições existentes no dicionário self.substituicoes.
        Neste caso, apenas 'Objetivo'.
        """
        if 'Campaign objective type' in self.df.columns:
            self.etl_dicionario('Campaign objective type', 'Objetivo', self.substituicoes['Objetivo'])
        else:
            logging.warning("Coluna 'Campaign objective type' não encontrada para aplicar substituições de 'Objetivo'.")

    def remover_colunas(self):
        """
        Remove colunas desnecessárias do DataFrame.
        """
        remover_colunas = [
            'AG Placement', 
            'Campaign objective type', 
            'Campaign name', 
            'Campaign ID', 
            'utm_content parameter value'
        ]
        cols_existentes = [col for col in remover_colunas if col in self.df.columns]
        self.df.drop(columns=cols_existentes, inplace=True)

    def renomear_colunas(self):
        """
        Renomeia colunas para o modelo final.
        """
        renomear_colunas = {
            'Date': 'Data',
            'Advertiser name': 'Nome_da_Conta',
            'Ad group name': 'Nome_do_Conjunto_de_Anuncio',
            'Schedule start time': 'Inicio_da_Campanha',
            'Schedule end time': 'Fim_da_Campanha',
            'Profile image URL': 'URL_do_Anuncio',
            'Ad name': 'Nome_do_Anuncio',
            'Video views at 25%': 'Visualizacoes_ate_25',
            'Video views at 50%': 'Visualizacoes_ate_50',
            'Video views at 75%': 'Visualizacoes_ate_75',
            'Video views at 100%': 'Visualizacoes_ate_100',
            'Cost': 'Investimento',
            'Clicks': 'Cliques_no_Link',
            'Video views': 'Video_Play',
            'Impressions': 'Impressoes',
            'Paid likes': 'Reacoes',
            'Paid shares': 'Compartilhamentos',
            'Paid comments': 'Comentarios'
        }
        self.df.rename(columns=renomear_colunas, inplace=True)

    def reordenar_colunas(self):
        """
        Reordena as colunas para o modelo final: modeloGeral.
        """
        nova_ordem_colunas = [
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
        colunas_existentes = [c for c in nova_ordem_colunas if c in self.df.columns]
        self.df = self.df[colunas_existentes]

    def aplicar_parametrizacao_campanha_externa(self, mapping_campanha, mapping_sigla):
        """
        Aplica mapeamento externo para 'Campanha' e 'ID_Campanha' usando a coluna 'Campaign name'.
        """
        if 'Campaign name' not in self.df.columns:
            logging.warning("'Campaign name' não está disponível para mapeamento externo.")
            return

        self.df["Campanha"] = self.df["Campaign name"].apply(
            lambda x: buscar_mapping(mapping_campanha, x) or x
        )
        self.df["ID_Campanha"] = self.df["Campaign name"].apply(
            lambda x: buscar_mapping(mapping_sigla, x)
        )

    def processar(self):
        """
        Executa o pipeline principal do ETL para Tiktok, gerando o modelo final.
        """
        self.ajustar_tipos()
        self.aplicar_substituicoes()
        self.atribuir_id_veiculo()
        self.remover_colunas()
        self.renomear_colunas()
        self.reordenar_colunas()
        return self.df
