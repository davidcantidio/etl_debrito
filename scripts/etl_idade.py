from utils.normalize import normalizar_faixa_etaria
from utils.campanha_mapper import buscar_mapping
import pandas as pd
import logging
from utils.objetivos import SUBSTITUICOES_OBJETIVO
from utils.normalize import inferir_veiculo_meta_por_placement

class BaseIdadeETL:
    def __init__(self, df, mapping_campanha=None, mapping_sigla=None, veiculo=""):
        self.df = df.copy()
        self.df.columns = [col.strip() for col in self.df.columns]
        self.mapping_campanha = mapping_campanha or {}
        self.mapping_sigla = mapping_sigla or {}
        self.veiculo = veiculo

    def ajustar_tipos(self):
        self.df['Date'] = pd.to_datetime(self.df['Date'], errors='coerce')
        for col in ['Cost', 'Impressions', 'Link clicks', 'Video watches at 100%']:
            if col in self.df.columns:
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce').fillna(0)
        if 'Cost' in self.df.columns:
            self.df['Cost'] = self.df['Cost'].round(2)
        self.df['Numero'] = 0
        self.df['Veiculo'] = self.veiculo
        self.df['ID'] = pd.Series(dtype='str')

    def renomear_colunas(self):
        rename_map = {
            'Date': 'Data',
            'Account name': 'Nome_da_Conta',
            'Ad set name': 'Nome_do_Conjunto_de_Anuncio',
            'Ad name': 'Nome_do_Anuncio',
            'Age': 'Idade',
            'Impressions': 'Impressoes',
            'Campaign objective type': 'Objetivo',
            'Cost': 'Investimento',
            'Link clicks': 'Cliques_no_Link',
            'Video watches at 100%': 'Visualizacoes_ate_100'
        }
        self.df.rename(columns=rename_map, inplace=True)

    def aplicar_normalizacoes(self):
        if 'Idade' in self.df.columns:
            self.df['Idade'] = self.df['Idade'].apply(normalizar_faixa_etaria)
        if 'Objetivo' in self.df.columns:
            self.df['Objetivo'] = self.df['Objetivo'].apply(lambda x: SUBSTITUICOES_OBJETIVO.get(str(x).upper().strip(), x))

    def aplicar_parametrizacao_campanha_externa(self):
        if 'Campaign name' in self.df.columns:
            self.df['Campanha'] = self.df['Campaign name'].apply(
                lambda x: buscar_mapping(self.mapping_campanha, x) or x
            )
            self.df['ID_Campanha'] = self.df['Campaign name'].apply(
                lambda x: buscar_mapping(self.mapping_sigla, x)
            )
        else:
            self.df['Campanha'] = ""
            self.df['ID_Campanha'] = ""

    def atribuir_id_veiculo(self, id_veiculo):
        self.df['ID_Veiculo'] = id_veiculo

    def remover_colunas(self):
        for col in ['Campaign ID', 'Campaign name']:
            if col in self.df.columns:
                self.df.drop(columns=col, inplace=True)

    def reordenar_colunas(self):
        ordem = [
            'Numero', 'Data', 'Nome_da_Conta', 'ID_Veiculo', 'Veiculo', 'ID_Campanha', 'Campanha',
            'Nome_do_Conjunto_de_Anuncio', 'Nome_do_Anuncio', 'Objetivo', 'Idade',
            'Impressoes', 'Investimento', 'Cliques_no_Link', 'Visualizacoes_ate_100', 'ID'
        ]
        for col in ordem:
            if col not in self.df.columns:
                self.df[col] = ""
        self.df = self.df[ordem]

    def gerar_id(self):
        self.df['ID'] = self.df.apply(
            lambda row: f"{row['Data']}-{row['Campanha']}-{row['Impressoes']}-{row['Investimento']}-{row['Cliques_no_Link']}-{row['Idade']}",
            axis=1
        )

    def processar(self):
        self.ajustar_tipos()
        self.renomear_colunas()
        self.aplicar_normalizacoes()
        self.aplicar_parametrizacao_campanha_externa()
        self.remover_colunas()
        self.reordenar_colunas()
        self.gerar_id()
        self.df['Veiculo'] = self.veiculo
        return self.df


class MetaIdadeETL(BaseIdadeETL):
    def __init__(self, df, mapping_campanha=None, mapping_sigla=None):
        super().__init__(df, mapping_campanha, mapping_sigla, veiculo='Meta')
    
    def criar_veiculo(self):
        logging.debug(">>> In MetaGeralETL.criar_veiculo (usando Placement)")
        self.df = inferir_veiculo_meta_por_placement(self.df)
        self.df['ID_Veiculo'] = self.id_veiculo 

class TikTokIdadeETL(BaseIdadeETL):
    def __init__(self, df, mapping_campanha=None, mapping_sigla=None):
        super().__init__(df, mapping_campanha, mapping_sigla, veiculo='TikTok')

class LinkedinIdadeETL(BaseIdadeETL):
    def __init__(self, df, mapping_campanha=None, mapping_sigla=None):
        super().__init__(df, mapping_campanha, mapping_sigla, veiculo='LinkedIn')

class PinterestIdadeETL(BaseIdadeETL):
    def __init__(self, df, mapping_campanha=None, mapping_sigla=None):
        super().__init__(df, mapping_campanha, mapping_sigla, veiculo='Pinterest')
