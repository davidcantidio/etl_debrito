import pandas as pd
import numpy as np
import re
import unicodedata
import logging
from utils.campanha_mapper import buscar_mapping

class etl_geral_tk:
    def __init__(self, df):
        """
        Inicializa o ETL para os dados do Tiktok.

        Parâmetros:
            df (pandas.DataFrame): DataFrame lido da aba de origem (Tiktok)
        """
        self.df = df.copy()
        # Dicionário para substituir valores da coluna 'Objetivo'
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
        e calcula o engajamento total a partir de Paid shares, Paid likes e Paid comments.
        Também define valores fixos para algumas colunas.
        """
        self.df['Date'] = pd.to_datetime(self.df['Date'], errors='coerce')
        self.df['Schedule start time'] = pd.to_datetime(self.df['Schedule start time'], errors='coerce')
        self.df['Schedule end time'] = pd.to_datetime(self.df['Schedule end time'], errors='coerce')
        self.df['Cost'] = pd.to_numeric(self.df['Cost'], errors='coerce').round(2)

        self.df['Paid shares'] = pd.to_numeric(self.df['Paid shares'], errors='coerce')
        self.df['Paid likes'] = pd.to_numeric(self.df['Paid likes'], errors='coerce')
        self.df['Paid comments'] = pd.to_numeric(self.df['Paid comments'], errors='coerce')
        self.df.fillna({'Paid shares': 0, 'Paid likes': 0, 'Paid comments': 0}, inplace=True)

        # Calcula o engajamento total
        self.df['Engajamento_Total'] = (
            self.df['Paid shares'] + self.df['Paid comments'] + self.df['Paid likes']
        )
        # Cria coluna "Numero" como zero (ou nula) e define o veículo fixo
        self.df['Numero'] = np.nan
        self.df.fillna({'Numero': 0}, inplace=True)
        self.df['Veiculo'] = 'Tiktok'
        # Inicializa a coluna ID (identificador único)
        self.df['ID'] = pd.Series(dtype='str')

    def atribuir_id_veiculo(self):
        """
        Atribui o ID_Veiculo fixo para Tiktok.
        """
        self.df['ID_Veiculo'] = 5

    def etl_dicionario(self, coluna_origem, coluna_destino, substituicoes_coluna):
        """
        Substitui valores em 'coluna_destino' com base nas chaves encontradas em 'coluna_origem'
        e usando o dicionário de substituições.
        """
        self.df[coluna_destino] = self.df[coluna_origem].apply(
            lambda x: next((valor for chave, valor in substituicoes_coluna.items() if chave in x), x)
            if isinstance(x, str) else x
        )

    def aplicar_substituicoes(self):
        """
        Aplica as substituições para a coluna 'Objetivo', utilizando o dicionário definido.
        """
        if 'Campaign objective type' in self.df.columns:
            self.etl_dicionario('Campaign objective type', 'Objetivo', self.substituicoes['Objetivo'])
        else:
            logging.warning("Coluna 'Campaign objective type' não encontrada para aplicar substituições de 'Objetivo'.")

    def remover_colunas(self):
        """
        Remove colunas desnecessárias que não fazem parte do modelo de destino.
        """
        remover = [
            'AG Placement', 
            'Campaign objective type', 
            'Campaign name', 
            'Campaign ID', 
            'utm_content parameter value'
        ]
        cols_existentes = [col for col in remover if col in self.df.columns]
        self.df.drop(columns=cols_existentes, inplace=True)

    def renomear_colunas(self):
        """
        Renomeia as colunas para o padrão do modelo de destino.
        """
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
        Reordena as colunas do DataFrame para que fiquem alinhadas com o modelo de destino.
        Se alguma coluna estiver ausente, ela é criada com valor vazio.
        
        Modelo de destino:
          Numero, Data, Nome_da_Conta, Campanha, ID_Campanha, Veiculo, ID_Veiculo,
          Nome_do_Conjunto_de_Anuncio, Nome_do_Anuncio, Inicio_da_Campanha, Fim_da_Campanha,
          Objetivo, URL_do_Anuncio, ID_Content, Investimento, Impressoes, Cliques_no_Link,
          Video_Play, Visualizacoes_ate_25, Visualizacoes_ate_50, Visualizacoes_ate_75,
          Visualizacoes_ate_100, Reacoes, Compartilhamentos, Comentarios, Engajamento_Total, ID
        """
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
        # Cria colunas ausentes com valor vazio
        for col in ordem:
            if col not in self.df.columns:
                self.df[col] = ""
        # Reordena conforme a ordem definida
        self.df = self.df[ordem]

    def aplicar_parametrizacao_campanha_externa(self, mapping_campanha, mapping_sigla):
        """
        Preenche as colunas 'Campanha' e 'ID_Campanha' utilizando os mapeamentos externos,
        com base no valor da coluna "Campaign name". Se não houver correspondência, mantém o valor original.
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

    def gerar_id(self):
        """
        Gera uma coluna 'ID' única concatenando os valores de Data, Campanha, Impressoes,
        Investimento e Cliques_no_Link.
        Essa coluna serve como identificador único para cada registro.
        """
        self.df["ID"] = self.df.apply(
            lambda row: f"{row['Data']}-{row['Campanha']}-{row['Impressoes']}-{row['Investimento']}-{row['Cliques_no_Link']}",
            axis=1
        )

    def processar(self):
        """
        Executa o pipeline completo do ETL para Tiktok:
          1. Ajusta os tipos de dados.
          2. Aplica as substituições internas.
          3. Atribui o ID_Veiculo.
          4. Remove colunas desnecessárias.
          5. Renomeia as colunas.
          6. Reordena as colunas conforme o modelo de destino.
          7. Gera o identificador único (ID).
        Retorna o DataFrame processado.
        """
        self.ajustar_tipos()
        self.aplicar_substituicoes()
        self.atribuir_id_veiculo()
        self.remover_colunas()
        self.renomear_colunas()
        self.reordenar_colunas()
        self.gerar_id()
        return self.df
