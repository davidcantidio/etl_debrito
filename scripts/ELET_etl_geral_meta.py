import pandas as pd
import numpy as np
import logging

class etl_geral_meta:
    def __init__(self, df, mapeamento_id_campanha=None):
        self.df = df
        self.mapeamento_id_campanha = mapeamento_id_campanha or {}

        self.substituicoes = {
            'Campanha': {
                # Substituições fixas (opcional)
            },
            'ID_Content': {
                # Substituições fixas (opcional)
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

    def aplicar_id_campanha_externo(self):
        logging.info("Aplicando mapeamento externo para ID_Campanha...")
        logging.info("Exemplo de valores em Campanha:")
        logging.info(self.df["Campanha"].dropna().unique()[:10])
        
        # Normaliza as chaves do dicionário para letras maiúsculas e sem espaços extras
        mapping = {str(k).strip().upper(): v for k, v in self.mapeamento_id_campanha.items()}
        logging.info(f"Exemplos de chaves normalizadas no mapeamento: {list(mapping.keys())[:10]}")

        def extrair_nome_base(campanha):
            if not isinstance(campanha, str):
                return ""
            partes = campanha.split("_")
            # Se os dois primeiros tokens são dígitos (ex.: "2025" e "2"), pegar o terceiro token
            if len(partes) >= 3 and partes[0].isdigit() and partes[1].isdigit():
                return partes[2].strip().upper()
            return campanha.strip().upper()

        # Aplica a função para extrair o nome base e mapeia para o ID_Campanha
        self.df['ID_Campanha'] = self.df['Campanha'].apply(
            lambda x: mapping.get(extrair_nome_base(x), "")
        )

    def criar_veiculo(self):
        # Inicializa com string vazia para evitar incompatibilidade de dtype
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

    def processar(self):
        self.ajustar_tipos()
        self.aplicar_substituicoes()
        self.aplicar_id_campanha_externo()
        self.criar_veiculo()
        self.atribuir_id_veiculo()
        self.remover_colunas()
        self.renomear_colunas()
        self.reordenar_colunas()

        # Formata as datas como "YYYY-MM-DD"
        self.df['Inicio_da_Campanha'] = self.df['Inicio_da_Campanha'].dt.strftime('%Y-%m-%d')
        self.df['Fim_da_Campanha'] = self.df['Fim_da_Campanha'].dt.strftime('%Y-%m-%d')

        # Gera um campo ID único combinando várias colunas
        self.df["ID"] = self.df.apply(
            lambda row: f"{str(row['Data'])}-{str(row['ID_Content']).strip()}-{str(row['Impressoes']).strip()}-{str(row['Investimento']).strip()}-{str(row['Cliques_no_Link']).strip()}-{str(row['ID_Content']).strip()}",
            axis=1
        )
        return self.df
