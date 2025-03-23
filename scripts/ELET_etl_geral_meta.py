import pandas as pd
import numpy as np
import logging

class etl_geral_meta:
    def __init__(self, df, mapping_campanha=None, mapping_sigla=None):
        """
        Inicializa o ETL para os dados de META – Geral.
        :param df: DataFrame lido da aba de origem (Meta – Geral)
        :param mapping_campanha: dicionário que mapeia (lookup) o valor da coluna "Campaign name"
                                 para o valor desejado para a coluna Campanha (obtido da BI_PARAMETRIZAÇÃO – coluna "CAMPANHA OBRIGATÓRIO")
        :param mapping_sigla: dicionário que mapeia o valor da coluna "Campaign name" para o valor desejado para a coluna ID_Campanha
                              (obtido da BI_PARAMETRIZAÇÃO – coluna "SIGLA")
        """
        self.df = df.copy()
        self.mapping_campanha = mapping_campanha or {}
        self.mapping_sigla = mapping_sigla or {}

        self.substituicoes = {
            'Campanha': {
                # Substituições fixas adicionais, se necessárias.
            },
            'ID_Content': {
                # Substituições para ID_Content, se houver.
            },
            'Objetivo': {
                'OUTCOME_AWARENESS': 'Alcance',
                'OUTCOME_TRAFFIC': 'Tráfego',
                'OUTCOME_ENGAGEMENT': 'Engajamento'
            }
        }

    def ajustar_tipos(self):
        self.df['Date'] = pd.to_datetime(self.df['Date'], errors='coerce')
        self.df['Ad set start time'] = pd.to_datetime(self.df['Ad set start time'], errors='coerce')
        self.df['Ad set end time'] = pd.to_datetime(self.df['Ad set end time'], errors='coerce')
        self.df['Cost'] = pd.to_numeric(self.df['Cost'], errors='coerce').round(2)
        self.df['Post shares'] = pd.to_numeric(self.df['Post shares'], errors='coerce')
        self.df['Post reactions'] = pd.to_numeric(self.df['Post reactions'], errors='coerce')
        self.df['Post comments'] = pd.to_numeric(self.df['Post comments'], errors='coerce')
        self.df = self.df.fillna({'Post shares': 0, 'Post reactions': 0, 'Post comments': 0})
        self.df['Engajamento_Total'] = self.df['Post shares'] + self.df['Post comments'] + self.df['Post reactions']
        self.df['Numero'] = np.nan
        self.df['ID'] = np.nan

    def etl_dicionario(self, coluna_origem, coluna_destino, substituicoes_coluna):
        self.df[coluna_destino] = self.df[coluna_origem].apply(
            lambda x: next((valor for chave, valor in substituicoes_coluna.items() if chave in x), x)
            if isinstance(x, str) else x
        )

    def aplicar_substituicoes(self):
        self.etl_dicionario('Campaign name', 'Campanha', self.substituicoes['Campanha'])
        self.etl_dicionario('Ad name', 'ID_Content', self.substituicoes['ID_Content'])
        self.etl_dicionario('Campaign objective', 'Objetivo', self.substituicoes['Objetivo'])

    def buscar_mapping(self, mapping, valor):
        """
        Busca no dicionário de mapping uma chave que esteja contida em 'valor' (após normalização).
        Retorna o valor mapeado se encontrado; caso contrário, retorna "".
        """
        valor_norm = valor.strip().upper()
        for chave, v in mapping.items():
            if chave in valor_norm:
                return v
        return ""

    def aplicar_parametrizacao_campanha(self):
        """
        Realiza o cruzamento com a aba BI_PARAMETRIZAÇÃO.
        Utiliza o valor da coluna "Campaign name" (normalizado) para buscar nos dicionários:
          - mapping_campanha: para preencher a coluna "Campanha" (destino)
          - mapping_sigla: para preencher a coluna "ID_Campanha" (destino)
        Se não houver correspondência, mantém o valor original (ou vazio para ID_Campanha).
        """
        self.df["Campanha"] = self.df["Campaign name"].apply(
            lambda x: self.buscar_mapping(self.mapping_campanha, x) or x
        )
        self.df["ID_Campanha"] = self.df["Campaign name"].apply(
            lambda x: self.buscar_mapping(self.mapping_sigla, x)
        )

    def criar_veiculo(self):
        self.df['Veiculo'] = ""
        self.df.loc[self.df['Placement'].str.contains('facebook', case=False, na=False), 'Veiculo'] = 'Facebook'
        self.df.loc[self.df['Placement'].str.contains('instagram', case=False, na=False), 'Veiculo'] = 'Instagram'
        self.df.loc[self.df['Placement'].str.contains('unknow', case=False, na=False), 'Veiculo'] = 'Não Classificado'

    def atribuir_id_veiculo(self):
        self.df['ID_Veiculo'] = np.where(self.df['Placement'].str.contains('facebook', case=False, na=False), 2, np.nan)
        self.df['ID_Veiculo'] = np.where(self.df['Placement'].str.contains('instagram', case=False, na=False), 3, self.df['ID_Veiculo'])

    def remover_colunas(self):
        cols = ['Placement', 'Campaign objective', 'Campaign name', 'Campaign ID', 'Content (utm)']
        self.df.drop(columns=[c for c in cols if c in self.df.columns], inplace=True)

    def renomear_colunas(self):
        renomear = {
            'Date': 'Data',
            'Account name': 'Nome_da_Conta',
            'Ad set name': 'Nome_do_Conjunto_de_Anuncio',
            'Ad set start time': 'Inicio_da_Campanha',
            'Ad set end time': 'Fim_da_Campanha',
            'Ad creative image URL': 'URL_do_Anuncio',
            'Ad name': 'Nome_do_Anuncio',
            'Video watches at 25%': 'Visualizacoes_ate_25',
            'Video watches at 50%': 'Visualizacoes_ate_50',
            'Video watches at 75%': 'Visualizacoes_ate_75',
            'Video watches at 100%': 'Visualizacoes_ate_100',
            'Cost': 'Investimento',
            'Link clicks': 'Cliques_no_Link',
            'Video play actions': 'Video_Play',
            'Impressions': 'Impressoes',
            'Post reactions': 'Reacoes',
            'Post shares': 'Compartilhamentos',
            'Post comments': 'Comentarios'
        }
        self.df.rename(columns=renomear, inplace=True)

    def reordenar_colunas(self):
        ordem = [
            'Numero', 'Data', 'Nome_da_Conta', 'Campanha', 'ID_Campanha', 'Veiculo', 'ID_Veiculo',
            'Nome_do_Conjunto_de_Anuncio', 'Nome_do_Anuncio', 'Inicio_da_Campanha', 'Fim_da_Campanha',
            'Objetivo', 'URL_do_Anuncio', 'ID_Content', 'Investimento', 'Impressoes', 'Cliques_no_Link',
            'Video_Play', 'Visualizacoes_ate_25', 'Visualizacoes_ate_50', 'Visualizacoes_ate_75',
            'Visualizacoes_ate_100', 'Reacoes', 'Compartilhamentos', 'Comentarios',
            'Engajamento_Total', 'ID'
        ]
        self.df = self.df[[col for col in ordem if col in self.df.columns]]

    def gerar_id(self):
        """
        Gera uma coluna 'ID' única combinando várias colunas.
        Neste exemplo, usamos a combinação de 'Data', 'ID_Content', 'Impressoes', 'Investimento' e 'Cliques_no_Link'.
        """
        self.df["ID"] = self.df.apply(
            lambda row: f"{row['Data']}-{row['ID_Content']}-{row['Impressoes']}-{row['Investimento']}-{row['Cliques_no_Link']}-{row['ID_Content']}",
            axis=1
        )

    def processar(self):
        self.ajustar_tipos()
        self.aplicar_substituicoes()
        self.aplicar_parametrizacao_campanha()
        self.criar_veiculo()
        self.atribuir_id_veiculo()
        self.remover_colunas()
        self.renomear_colunas()
        self.reordenar_colunas()
        self.gerar_id()
        return self.df
