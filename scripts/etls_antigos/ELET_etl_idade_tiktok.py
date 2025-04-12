import pandas as pd
import numpy as np
import logging
from utils.campanha_mapper import buscar_mapping
from utils.normalize import normalize_campaign_name

class etl_idade_tiktok:
    def __init__(self, df, mapping_campanha=None, mapping_sigla=None):
        """
        ETL para dados de Idade no TikTok.
        
        :param df: DataFrame com as colunas de origem ("Tiktok Idade")
        :param mapping_campanha: dict p/ lookup "Campaign name" → "Campanha"
        :param mapping_sigla: dict p/ lookup "Campaign name" → "ID_Campanha"
        """
        self.df = df.copy()
        # Remove espaços dos cabeçalhos
        self.df.columns = [col.strip() for col in self.df.columns]

        self.mapping_campanha = mapping_campanha or {}
        self.mapping_sigla = mapping_sigla or {}

        # Substituições customizadas
        self.substituicoes = {
            'Age': {
                'NONE': 'Sem identificação'
            },
            'Objetivo': {
                'REACH': 'Alcance',
                'TRAFFIC': 'Tráfego',
                'VIDEO_VIEWS': 'Visualização',
                'LEAD_GENERATION': 'Geração de Leads'
            }
        }

    def ajustar_tipos(self):
        """
        Converte tipos e cria colunas auxiliares (Numero, Veiculo, ID).
        """
        self.df['Date'] = pd.to_datetime(self.df['Date'], errors='coerce')
        for numeric_col in ['Cost','Impressions','Clicks','Video views at 100%']:
            if numeric_col in self.df.columns:
                self.df[numeric_col] = pd.to_numeric(self.df[numeric_col], errors='coerce').fillna(0)
        self.df['Cost'] = self.df['Cost'].round(2) if 'Cost' in self.df.columns else 0
        
        self.df['Numero'] = 0
        self.df['Veiculo'] = 'TikTok'
        self.df['ID'] = pd.Series(dtype='str')

    def renomear_colunas(self):
        """
        Renomeia as colunas, exceto 'Campaign name'.
        Notamos que 'Campaign objective type' vira 'Objetivo' ANTES da substituição.
        """
        rename_map = {
            'Date': 'Data',
            'Advertiser name': 'Nome_da_Conta',
            'Ad group name': 'Nome_do_Conjunto_de_Anuncio',
            'Ad name': 'Nome_do_Anuncio',
            'Age': 'Idade',
            'Impressions': 'Impressoes',
            'Campaign objective type': 'Objetivo',
            'Cost': 'Investimento',
            'Clicks': 'Cliques_no_Link',
            'Video views at 100%': 'Visualizacoes_ate_100'
            # OBS: 'Campaign name' não renomeamos aqui
        }
        self.df.rename(columns=rename_map, inplace=True)

    def aplicar_substituicoes(self):
        """
        Aplica substituições:
         - Coluna 'Age' → substituição
         - Coluna 'Objetivo' → substituição (ex.: 'TRAFFIC' → 'Tráfego')
        """
        # Se 'Idade' existir:
        if 'Idade' in self.df.columns:
            self.df['Idade'] = self.df['Idade'].apply(
                lambda x: self.substituicoes['Age'].get(x.upper().strip(), x)
                          if isinstance(x, str) else x
            )
        else:
            logging.warning("Coluna 'Idade' não encontrada para substituições em Age.")

        # Se 'Objetivo' existir:
        if 'Objetivo' in self.df.columns:
            self.df['Objetivo'] = self.df['Objetivo'].apply(
                lambda x: self.substituicoes['Objetivo'].get(x.upper().strip(), x)
                          if isinstance(x, str) else x
            )
        else:
            logging.warning("Coluna 'Objetivo' não encontrada para substituir objetivos.")

    def aplicar_parametrizacao_campanha_externa(self):
        """
        Cria colunas 'Campanha' e 'ID_Campanha' usando 'Campaign name'
        e os dicionários de lookup (mapping_campanha / mapping_sigla).
        """
        if 'Campaign name' not in self.df.columns:
            logging.warning("'Campaign name' não existe no DF para lookup.")
            return
        
        self.df["Campanha"] = self.df["Campaign name"].apply(
            lambda x: buscar_mapping(self.mapping_campanha, x) or x
        )
        self.df["ID_Campanha"] = self.df["Campaign name"].apply(
            lambda x: buscar_mapping(self.mapping_sigla, x)
        )

    def atribuir_id_veiculo(self):
        self.df['ID_Veiculo'] = 5

    def remover_colunas(self):
        """
        Remove colunas que não queremos no final (Campaign ID, Campaign name).
        """
        cols_remover = ['Campaign ID','Campaign name']
        self.df.drop(columns=[c for c in cols_remover if c in self.df.columns],
                     inplace=True, errors='ignore')

    def reordenar_colunas(self):
        """
        Reordena p/ o modelo 'modeloIdade':
         Numero, Data, Nome_da_Conta, ID_Veiculo, Veiculo, ID_Campanha, Campanha,
         Nome_do_Conjunto_de_Anuncio, Nome_do_Anuncio, Objetivo, Idade, Impressoes,
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
            'Idade',
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
        Gera ID concatenando Data, Campanha, Impressoes, Investimento, Cliques_no_Link e Idade.
        """
        self.df["ID"] = self.df.apply(
            lambda row: f"{row['Data']}-{row['Campanha']}-{row['Impressoes']}-{row['Investimento']}-{row['Cliques_no_Link']}-{row['Idade']}",
            axis=1
        )

    def processar(self):
        """
        Executa o ETL completo:
          1) ajustar_tipos
          2) renomear_colunas (assim 'Campaign objective type' vira 'Objetivo' antes da substituicao)
          3) aplicar_substituicoes (Age, Objetivo)
          4) aplicar_parametrizacao_campanha_externa (lookup para 'Campanha', 'ID_Campanha')
          5) atribuir_id_veiculo
          6) remover_colunas (ex.: 'Campaign name')
          7) reordenar_colunas
          8) gerar_id
          9) Força 'Veiculo' = 'TikTok'
        """
        self.ajustar_tipos()
        # 1. Renomeia colunas (isso deixa 'Campaign objective type' => 'Objetivo')
        self.renomear_colunas()
        # 2. Aplica substituicoes (ex.: 'TRAFFIC' => 'Tráfego')
        self.aplicar_substituicoes()
        # 3. Cria 'Campanha' e 'ID_Campanha' via lookup (enquanto 'Campaign name' existe)
        self.aplicar_parametrizacao_campanha_externa()
        # 4. ID_Veiculo fixo
        self.atribuir_id_veiculo()
        # 5. Remove colunas que nao usamos mais
        self.remover_colunas()
        # 6. Reordena
        self.reordenar_colunas()
        # 7. Gera ID
        self.gerar_id()

        # 8. Forca Veiculo = 'TikTok'
        self.df['Veiculo'] = 'TikTok'

        return self.df
