import pandas as pd
import numpy as np
import logging
from utils.campanha_mapper import buscar_mapping
from utils.normalize import normalize_campaign_name

class etl_genero_tiktok:
    def __init__(self, df, mapping_campanha=None, mapping_sigla=None):
        """
        ETL para dados de Gênero no TikTok.

        Parâmetros:
            df (pandas.DataFrame): DataFrame carregado da aba "Tiktok Genero"
            mapping_campanha (dict): Dicionário p/ lookup "Campaign name" → "Campanha"
            mapping_sigla (dict): Dicionário p/ lookup "Campaign name" → "ID_Campanha"
        """
        self.df = df.copy()
        # Remove espaços extras dos cabeçalhos
        self.df.columns = [col.strip() for col in self.df.columns]

        self.mapping_campanha = mapping_campanha or {}
        self.mapping_sigla = mapping_sigla or {}

        # Substituições específicas para 'Genero' e 'Objetivo'
        self.substituicoes = {
            'Gender': {
                'NONE': 'Sem identificação',
                'male': 'Homem',
                'female': 'Mulher'
            },
            'Objetivo': {
                'REACH': 'Alcance',
                'TRAFFIC': 'Tráfego',
                'VIDEO_VIEWS': 'Visualização',
                'LEAD_GENERATION': 'Geração de Leads'
                # etc., conforme necessário
            }
        }

    def ajustar_tipos(self):
        """
        Converte tipos de data e numéricos,
        cria colunas auxiliares (Numero, Veiculo, ID).
        """
        # Data
        self.df['Date'] = pd.to_datetime(self.df['Date'], errors='coerce')

        # Conversão de colunas numéricas
        numeric_cols = ['Cost', 'Impressions', 'Clicks', 'Video views at 100%']
        for col in numeric_cols:
            if col in self.df.columns:
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce').fillna(0)
        if 'Cost' in self.df.columns:
            self.df['Cost'] = self.df['Cost'].round(2)

        self.df['Numero'] = 0
        self.df['Veiculo'] = 'TikTok'
        self.df['ID'] = pd.Series(dtype='str')

    def renomear_colunas(self):
        """
        Renomeia colunas de origem -> destino, exceto 'Campaign name'.
        
        Colunas de origem (TikTok Genero):
          Date, Advertiser name, Campaign ID, Campaign name, Ad group name,
          Ad name, Gender, Impressions, Cost, Clicks, Video views at 100%, 
          (opcional: Campaign objective type)

        Colunas de destino (modeloGenero):
          numero, Data, Nome_da_Conta, ID_Veiculo, Veiculo, ID_Campanha, Campanha,
          Nome_do_Conjunto_de_Anuncio, Nome_do_Anuncio, Objetivo, Genero, Impressoes,
          Investimento, Cliques_no_Link, Visualizacoes_ate_100, ID
        """
        rename_map = {
            'Date': 'Data',
            'Advertiser name': 'Nome_da_Conta',
            'Ad group name': 'Nome_do_Conjunto_de_Anuncio',
            'Ad name': 'Nome_do_Anuncio',
            'Gender': 'Genero',
            'Impressions': 'Impressoes',
            'Campaign objective type': 'Objetivo',  # se existir
            'Cost': 'Investimento',
            'Clicks': 'Cliques_no_Link',
            'Video views at 100%': 'Visualizacoes_ate_100'
        }
        self.df.rename(columns=rename_map, inplace=True)

    def aplicar_substituicoes(self):
        """
        Aplica substituições para 'Genero' e 'Objetivo', se existirem.
          - 'Genero' → { 'NONE': 'Sem identificação', 'male': 'Homem', 'female': 'Mulher' }
          - 'Objetivo' → { 'TRAFFIC': 'Tráfego', 'REACH': 'Alcance', etc. }
        """
        # Se 'Genero' existir
        if 'Genero' in self.df.columns:
            self.df['Genero'] = self.df['Genero'].apply(
                lambda x: self.substituicoes['Gender'].get(x.lower().strip(), x)
                          if isinstance(x, str) else x
            )
        else:
            logging.warning("Coluna 'Genero' não encontrada para substituições.")

        # Se 'Objetivo' existir
        if 'Objetivo' in self.df.columns:
            self.df['Objetivo'] = self.df['Objetivo'].apply(
                lambda x: self.substituicoes['Objetivo'].get(x.upper().strip(), x)
                          if isinstance(x, str) else x
            )
        else:
            logging.info("Coluna 'Objetivo' não encontrada (talvez 'Campaign objective type' não exista).")

    def aplicar_parametrizacao_campanha_externa(self):
        """
        Cria colunas 'Campanha' e 'ID_Campanha' via lookup da coluna 'Campaign name'.
        """
        if 'Campaign name' not in self.df.columns:
            logging.warning("'Campaign name' não está disponível para mapeamento externo.")
            return
        
        self.df["Campanha"] = self.df["Campaign name"].apply(
            lambda x: buscar_mapping(self.mapping_campanha, x) or x
        )
        self.df["ID_Campanha"] = self.df["Campaign name"].apply(
            lambda x: buscar_mapping(self.mapping_sigla, x)
        )

    def atribuir_id_veiculo(self):
        """
        Atribui ID_Veiculo fixo para Tiktok (ex.: 5).
        """
        self.df['ID_Veiculo'] = 5

    def remover_colunas(self):
        """
        Remove colunas que não serão usadas no destino,
        por exemplo, 'Campaign ID', 'Campaign name'.
        """
        cols_remove = ['Campaign ID', 'Campaign name']
        self.df.drop(columns=[c for c in cols_remove if c in self.df.columns],
                     inplace=True, errors='ignore')

    def reordenar_colunas(self):
        """
        Reordena para 'modeloGenero':
          numero, Data, Nome_da_Conta, ID_Veiculo, Veiculo, ID_Campanha, Campanha,
          Nome_do_Conjunto_de_Anuncio, Nome_do_Anuncio, Objetivo, Genero, Impressoes,
          Investimento, Cliques_no_Link, Visualizacoes_ate_100, ID
        """
        ordem = [
            'Numero',
            'Data',
            'Nome_da_Conta',
            'ID_Veiculo',
            'Veiculo',
            'ID_Campanha',
            'Campanha',
            'Nome_do_Conjunto_de_Anuncio',
            'Nome_do_Anuncio',
            'Objetivo',
            'Genero',
            'Impressoes',
            'Investimento',
            'Cliques_no_Link',
            'Visualizacoes_ate_100',
            'ID'
        ]
        for col in ordem:
            if col not in self.df.columns:
                self.df[col] = ""
        self.df = self.df[ordem]

    def gerar_id(self):
        """
        Gera a coluna 'ID' único unindo Data, Campanha, Impressoes, Investimento, Cliques_no_Link, Genero.
        """
        self.df["ID"] = self.df.apply(
            lambda row: f"{row['Data']}-{row['Campanha']}-{row['Impressoes']}-{row['Investimento']}-{row['Cliques_no_Link']}-{row['Genero']}",
            axis=1
        )

    def processar(self):
        """
        Executa o ETL:
          1) ajustar_tipos
          2) renomear_colunas
          3) aplicar_substituicoes
          4) aplicar_parametrizacao_campanha_externa
          5) atribuir_id_veiculo
          6) remover_colunas
          7) reordenar_colunas
          8) gerar_id
          9) Força Veiculo = 'TikTok'
        """
        self.ajustar_tipos()
        self.renomear_colunas()
        self.aplicar_substituicoes()
        self.aplicar_parametrizacao_campanha_externa()
        self.atribuir_id_veiculo()
        self.remover_colunas()
        self.reordenar_colunas()
        self.gerar_id()

        # Força Veiculo
        self.df['Veiculo'] = 'TikTok'
        return self.df
