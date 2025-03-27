import pandas as pd
import numpy as np
import re
import unicodedata
import logging
from utils.campanha_mapper import buscar_mapping
from utils.normalize import normalize_campaign_name

class etl_geral_tk:
    def __init__(self, df, mapping_campanha=None, mapping_sigla=None):
        """
        Inicializa o ETL para os dados do Tiktok.

        Parâmetros:
            df (pandas.DataFrame): DataFrame lido da aba de origem (Tiktok)
            mapping_campanha (dict): Dicionário para mapear "Campaign name" para "Campanha"
            mapping_sigla (dict): Dicionário para mapear "Campaign name" para "ID_Campanha"
        """
        self.df = df.copy()
        self.mapping_campanha = mapping_campanha or {}
        self.mapping_sigla = mapping_sigla or {}
        self.substituicoes = {
            'Objetivo': {
                'REACH': 'Alcance',
                'VIDEO_VIEWS': 'Visualização',
                'TRAFFIC': 'Tráfego',
                'TOPVIEW-BR-20241109': 'Alcance'
            }
        }

    def ajustar_tipos(self):
        """
        Converte os tipos das colunas, formata datas e valores numéricos,
        calcula o engajamento total e extrai o ID_Content a partir da coluna
        "utm_content parameter value" (se presente).
        """
        self.df['Date'] = pd.to_datetime(self.df['Date'], errors='coerce')
        self.df['Schedule start time'] = pd.to_datetime(self.df['Schedule start time'], errors='coerce')
        self.df['Schedule end time'] = pd.to_datetime(self.df['Schedule end time'], errors='coerce')
        self.df['Cost'] = pd.to_numeric(self.df['Cost'], errors='coerce').round(2)

        self.df['Paid shares'] = pd.to_numeric(self.df['Paid shares'], errors='coerce')
        self.df['Paid likes'] = pd.to_numeric(self.df['Paid likes'], errors='coerce')
        self.df['Paid comments'] = pd.to_numeric(self.df['Paid comments'], errors='coerce')
        self.df.fillna({'Paid shares': 0, 'Paid likes': 0, 'Paid comments': 0}, inplace=True)

        self.df['Engajamento_Total'] = (
            self.df['Paid shares'] + self.df['Paid comments'] + self.df['Paid likes']
        )
        self.df['Numero'] = np.nan
        self.df.fillna({'Numero': 0}, inplace=True)
        self.df['Veiculo'] = 'Tiktok'
        self.df['ID'] = pd.Series(dtype='str')
        
        # Extração de ID_Content a partir da coluna "utm_content parameter value"
        if "utm_content parameter value" in self.df.columns:
            self.df["ID_Content"] = self.df["utm_content parameter value"].apply(
                lambda x: x.strip() if isinstance(x, str) else ""
            )
        else:
            self.df["ID_Content"] = ""

    def atribuir_id_veiculo(self):
        """Atribui o ID_Veiculo fixo para Tiktok."""
        self.df['ID_Veiculo'] = 5

    def etl_dicionario(self, coluna_origem, coluna_destino, substituicoes_coluna):
        """Aplica substituições com base no dicionário informado."""
        self.df[coluna_destino] = self.df[coluna_origem].apply(
            lambda x: next((valor for chave, valor in substituicoes_coluna.items() if chave in x), x)
            if isinstance(x, str) else x
        )

    def aplicar_substituicoes(self):
        """Aplica as substituições para a coluna 'Objetivo'."""
        if 'Campaign objective type' in self.df.columns:
            self.etl_dicionario('Campaign objective type', 'Objetivo', self.substituicoes['Objetivo'])
        else:
            logging.warning("Coluna 'Campaign objective type' não encontrada para aplicar substituições de 'Objetivo'.")

    def aplicar_parametrizacao_campanha_externa(self, mapping_campanha, mapping_sigla):
        """
        Aplica o lookup para preencher as colunas 'Campanha' e 'ID_Campanha'
        utilizando os mapeamentos externos com base em "Campaign name".
        Este método é chamado enquanto "Campaign name" ainda existe no DataFrame.
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
        logging.debug("Lookup aplicado em 'Campaign name' para gerar 'Campanha' e 'ID_Campanha'.")

    def criar_veiculo(self):
        """
        Define a coluna 'Veiculo' com base na coluna de placement.
        Usa 'Placement' se existir, caso contrário utiliza 'AG Placement'.
        """
        if 'Placement' in self.df.columns:
            placement_col = 'Placement'
        elif 'AG Placement' in self.df.columns:
            placement_col = 'AG Placement'
        else:
            logging.warning("Nenhuma coluna de placement encontrada; 'Veiculo' ficará vazio.")
            self.df['Veiculo'] = ""
            return

        self.df['Veiculo'] = ""
        self.df.loc[self.df[placement_col].str.contains('facebook', case=False, na=False), 'Veiculo'] = 'Facebook'
        self.df.loc[self.df[placement_col].str.contains('instagram', case=False, na=False), 'Veiculo'] = 'Instagram'
        self.df.loc[self.df[placement_col].str.contains('unknow', case=False, na=False), 'Veiculo'] = 'Não Classificado'

    def remover_colunas(self):
        """
        Remove colunas desnecessárias que não fazem parte do modelo final.
        Nota: "Campaign name" e "utm_content parameter value" são removidas após o lookup e extração.
        """
        cols = ['Placement', 'Campaign objective', 'Campaign name', 'Campaign ID', 'utm_content parameter value']
        self.df.drop(columns=[c for c in cols if c in self.df.columns], inplace=True)

    def renomear_colunas(self):
        """Renomeia as colunas para o padrão do modelo de destino."""
        renomear = {
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
        self.df.rename(columns=renomear, inplace=True)

    def reordenar_colunas(self):
        """
        Reordena as colunas conforme o modelo de destino.
        Se alguma coluna não existir, ela é criada com valor vazio.
        Modelo de destino:
          Numero, Data, Nome_da_Conta, Campanha, ID_Campanha, Veiculo, ID_Veiculo,
          Nome_do_Conjunto_de_Anuncio, Nome_do_Anuncio, Inicio_da_Campanha, Fim_da_Campanha,
          Objetivo, URL_do_Anuncio, ID_Content, Investimento, Impressoes, Cliques_no_Link,
          Video_Play, Visualizacoes_ate_25, Visualizacoes_ate_50, Visualizacoes_ate_75,
          Visualizacoes_ate_100, Reacoes, Compartilhamentos, Comentarios, Engajamento_Total, ID
        """
        ordem = [
            'Numero', 'Data', 'Nome_da_Conta', 'Campanha', 'ID_Campanha', 'Veiculo', 'ID_Veiculo',
            'Nome_do_Conjunto_de_Anuncio', 'Nome_do_Anuncio', 'Inicio_da_Campanha', 'Fim_da_Campanha',
            'Objetivo', 'URL_do_Anuncio', 'ID_Content', 'Investimento', 'Impressoes', 'Cliques_no_Link',
            'Video_Play', 'Visualizacoes_ate_25', 'Visualizacoes_ate_50', 'Visualizacoes_ate_75',
            'Visualizacoes_ate_100', 'Reacoes', 'Compartilhamentos', 'Comentarios',
            'Engajamento_Total', 'ID'
        ]
        for col in ordem:
            if col not in self.df.columns:
                self.df[col] = ""
        self.df = self.df[ordem]

    def gerar_id(self):
        """
        Gera uma coluna 'ID' única combinando os valores de Data, Campanha, Impressoes,
        Investimento e Cliques_no_Link.
        """
        self.df["ID"] = self.df.apply(
            lambda row: f"{row['Data']}-{row['Campanha']}-{row['Impressoes']}-{row['Investimento']}-{row['Cliques_no_Link']}",
            axis=1
        )

    def processar(self):
        """
        Executa o pipeline completo do ETL para Tiktok:
          1. Ajusta os tipos e extrai ID_Content.
          2. Aplica substituições internas.
          3. Aplica a parametrização externa (lookup) enquanto "Campaign name" ainda existe.
          4. Cria a coluna "Veiculo" e atribui o ID_Veiculo.
          5. Remove colunas desnecessárias (incluindo "Campaign name" e "utm_content parameter value").
          6. Renomeia e reordena as colunas conforme o modelo de destino.
          7. Gera o identificador único (ID).
        Retorna o DataFrame final.
        """
        self.ajustar_tipos()
        self.aplicar_substituicoes()
        # Aplica o lookup enquanto "Campaign name" ainda existe
        self.aplicar_parametrizacao_campanha_externa(self.mapping_campanha, self.mapping_sigla)
        self.criar_veiculo()
        self.atribuir_id_veiculo()
        self.remover_colunas()
        self.renomear_colunas()
        self.reordenar_colunas()
        self.gerar_id()
        self.df['Veiculo'] = 'TikTok'

        return self.df
