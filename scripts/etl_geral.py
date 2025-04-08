# scripts/etl_geral.py

import pandas as pd
import numpy as np
import logging

# Imports de utilitários
from utils.campanha_mapper import buscar_mapping
from utils.objetivos import SUBSTITUICOES_OBJETIVO
from utils.numeracao import gerar_numeracao

# Funções de normalização
from utils.normalize import normalize_columns, normalize_parametrizacao_values

# Se você tiver alguma função de preview ou criativo, importe aqui
# ex.: from utils.preview_links import ajustar_preview_link
from utils.get_nome_campanha import obter_nome_por_utm_content

class BaseGeralETL:
    def __init__(
        self,
        df: pd.DataFrame,
        id_veiculo: int,
        veiculo: str,
        mapping_campanha: dict = None,
        mapping_sigla: dict = None
    ):
        logging.debug(">>> In BaseGeralETL.__init__")
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
            'Campaign name': 'Campaign_name',
            'Ad group name': 'Nome_do_Conjunto_de_Anuncio',
            'Ad set name': 'Nome_do_Conjunto_de_Anuncio',
            'Ad name': 'Nome_do_Anuncio',
            'Campaign ID': 'Campaign_ID',
            'Start': 'Inicio_da_Campanha',
            'End': 'Fim_da_Campanha',
            'Campaign objective type': 'Objetivo',
            'Campaign objective': 'Objetivo',
            'Placement': 'Placement',
            'Preview Link': 'URL_do_Anuncio',
            'Content (utm)': 'ID_Content',
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
        logging.debug(f"Colunas após renomear: {list(self.df.columns)}")

    def ajustar_tipos_e_calculos(self):
        logging.debug(">>> In ajustar_tipos_e_calculos")
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

        # Calcula engajamento total se todas existirem
        cols_engajamento = ['Reacoes', 'Compartilhamentos', 'Comentarios']
        if all(x in self.df.columns for x in cols_engajamento):
            self.df['Engajamento_Total'] = (
                self.df['Reacoes'] + self.df['Compartilhamentos'] + self.df['Comentarios']
            )
        else:
            self.df['Engajamento_Total'] = 0

        if 'Numero' not in self.df.columns:
            self.df['Numero'] = np.nan
        if 'ID' not in self.df.columns:
            self.df['ID'] = np.nan
        logging.debug(f"DataFrame shape após ajustes: {self.df.shape}")

    def aplicar_substituicoes_objetivo(self):
        logging.debug(">>> In aplicar_substituicoes_objetivo")
        col_obj = 'Objetivo'
        if col_obj in self.df.columns:
            for old_val, new_val in self.substituicoes['Objetivo'].items():
                mask = self.df[col_obj] == old_val
                self.df.loc[mask, col_obj] = new_val
        else:
            logging.warning("Nenhuma coluna 'Objetivo' encontrada para substituições.")

    def aplicar_parametrizacao_campanha_externa(self):
        logging.debug(">>> In aplicar_parametrizacao_campanha_externa")
        if 'Campaign_name' not in self.df.columns:
            logging.warning("Coluna 'Campaign_name' não encontrada para lookup de campanha.")
            self.df['Campanha'] = ""
            self.df['ID_Campanha'] = ""
            return
        # Aplica mapping
        self.df['Campanha'] = self.df['Campaign_name'].apply(
            lambda x: buscar_mapping(self.mapping_campanha, x) or x
        )
        self.df['ID_Campanha'] = self.df['Campaign_name'].apply(
            lambda x: buscar_mapping(self.mapping_sigla, x)
        )
        logging.debug(">>> Campanha e ID_Campanha preenchidas (se mapearam)")

    def criar_veiculo(self):
        logging.debug(">>> In criar_veiculo")
        self.df['Veiculo'] = self.veiculo
        self.df['ID_Veiculo'] = self.id_veiculo

    def remover_colunas_indesejadas(self):
        logging.debug(">>> In remover_colunas_indesejadas")
        # Exemplo: descartar colunas que não queremos no modelo final
        cols_to_drop = ['Placement', 'Campaign_ID', 'Campaign_name', 'Content_utm']
        for c in cols_to_drop:
            if c in self.df.columns:
                self.df.drop(columns=[c], inplace=True)

    def reordenar_colunas_para_modelo(self):
        logging.debug(">>> In reordenar_colunas_para_modelo")
        ordem = [
            'Numero', 'Data', 'Nome_da_Conta', 'Campanha', 'ID_Campanha',
            'Veiculo', 'ID_Veiculo', 'Nome_do_Conjunto_de_Anuncio',
            'Nome_do_Anuncio', 'Inicio_da_Campanha', 'Fim_da_Campanha',
            'Objetivo', 'URL_do_Anuncio', 'ID_Content', 'Investimento',
            'Impressoes', 'Cliques_no_Link', 'Video_Play',
            'Visualizacoes_ate_25', 'Visualizacoes_ate_50',
            'Visualizacoes_ate_75', 'Visualizacoes_ate_100',
            'Reacoes', 'Compartilhamentos', 'Comentarios',
            'Engajamento_Total', 'ID'
        ]
        for col in ordem:
            if col not in self.df.columns:
                self.df[col] = ""
        self.df = self.df[ordem]

    def gerar_id(self):
        logging.debug(">>> In gerar_id")
        self.df['ID'] = self.df.apply(
            lambda row: (
                f"{row['Data']}-{row['Campanha']}-"
                f"{row['Impressoes']}-{row['Investimento']}-"
                f"{row['Cliques_no_Link']}"
            ),
            axis=1
        )

    def processar(self, df_destino=None) -> pd.DataFrame:
        logging.debug(">>> In processar (BaseGeralETL)")
        self.renomear_colunas_origem_para_modelo()
        self.ajustar_tipos_e_calculos()
        self.aplicar_substituicoes_objetivo()
        self.aplicar_parametrizacao_campanha_externa()

        # HOOKS de preview ou outras transformações podem ocorrer aqui:
        self.ajustes_preview()

        # Cria colunas de Veículo / ID_Veículo
        self.criar_veiculo()
        self.remover_colunas_indesejadas()
        self.reordenar_colunas_para_modelo()
        self.gerar_id()

        # Gera numeração final, considerando df_destino, se houver
        self.df = gerar_numeracao(self.df, df_destino=df_destino, linha_insercao=2, coluna='Numero')
        logging.debug(">>> processar finalizado, head do DF:")
        logging.debug(f"{self.df.head(5)}")
        return self.df

    def ajustes_preview(self):
        """
        Método-base que será sobrescrito nas subclasses específicas.
        """
        pass


######################################################################
class MetaGeralETL(BaseGeralETL):
    def ajustes_preview(self):
        logging.debug(">>> In MetaGeralETL.ajustes_preview")
        # Exemplo: se tivéssemos pré-visualização extra do FB
        pass

######################################################################
class LinkedinGeralETL(BaseGeralETL):
    def __init__(self, *args, mapping_preview=None, mapping_criativo=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.mapping_preview = mapping_preview or {}
        self.mapping_criativo = mapping_criativo or {}
        logging.debug(">>> In LinkedinGeralETL.__init__")
        logging.debug(f"mapping_preview keys: {list(self.mapping_preview.keys())}")
        logging.debug(f"mapping_criativo keys: {list(self.mapping_criativo.keys())}")

    def ajustes_preview(self):
        logging.debug(">>> In LinkedinGeralETL.ajustes_preview")

        if 'ID_Content' not in self.df.columns:
            logging.debug("Coluna 'ID_Content' não existe no DF, definindo campos vazios.")
            self.df['URL_do_Anuncio'] = ""
            self.df['Nome_do_Anuncio'] = ""
            self.df['Nome_do_Conjunto_de_Anuncio'] = ""
            return

        logging.debug(f"ID_Content sample:\n{self.df['ID_Content'].head(5)}")

        # 1) Ajustar URL_do_Anuncio
        self.df['URL_do_Anuncio'] = self.df['ID_Content'].apply(
            lambda x: self.mapping_preview.get(str(x).strip(), "")
        )

        # 2) Nome_do_Anuncio e Nome_do_Conjunto_de_Anuncio via mapping_criativo
        def debug_nome(utm):
            utm_str = str(utm).strip()
            nome = obter_nome_por_utm_content(utm_str, self.mapping_criativo)
            if not nome:
                logging.debug(f"utm_content '{utm_str}' NÃO encontrado no mapping_criativo.")
            return nome

        self.df['Nome_do_Anuncio'] = self.df['ID_Content'].apply(debug_nome)
        self.df['Nome_do_Conjunto_de_Anuncio'] = self.df['Nome_do_Anuncio']
        logging.debug("Nome_do_Anuncio e Nome_do_Conjunto_de_Anuncio preenchidos.")


######################################################################
class PinterestGeralETL(BaseGeralETL):
    def ajustes_preview(self):
        logging.debug(">>> In PinterestGeralETL.ajustes_preview")
        # Exemplo de preview para Pinterest, se precisar
        pass

######################################################################
class TiktokGeralETL(BaseGeralETL):
    pass

######################################################################
# Exemplo de uso de normalização dos valores da BI_PARAMETRIZAÇÃO:
# Você, no seu "append_only_new_geral.py", pode ler a planilha,
# e então:
#
#   from utils.normalize import normalize_columns, normalize_parametrizacao_values
#
#   df_parametrizacao.columns = normalize_columns(df_parametrizacao.columns)
#   df_parametrizacao = normalize_parametrizacao_values(df_parametrizacao)
#
#   ... só então extrair os mapeamentos ...
######################################################################
