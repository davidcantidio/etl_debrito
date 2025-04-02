import pandas as pd
import numpy as np
import logging

from utils.campanha_mapper import buscar_mapping
from utils.geolocalizacao import obter_estado_de_regiao


class etl_regiao_tiktok:
    def __init__(
        self,
        df: pd.DataFrame,
        mapping_campanha: dict = None,
        mapping_sigla: dict = None,
        cache_estados: dict = None,
        cache_municipios: dict = None,
        id_veiculo: int = 5
    ):
        self.df = df.copy()
        self.mapping_campanha = mapping_campanha or {}
        self.mapping_sigla = mapping_sigla or {}
        self.cache_estados = cache_estados or {}
        self.cache_municipios = cache_municipios or {}
        self.id_veiculo = id_veiculo

        self.substituicoes = {
            'Objetivo': {
                'REACH': 'Alcance',
                'VIDEO_VIEWS': 'Visualizacao',
                'TRAFFIC': 'Tráfego'
            }
        }

        self._dict_correspondencia_regiao = {}

    def ajustar_tipos(self):
        self.df['Date'] = pd.to_datetime(self.df['Date'], errors='coerce')
        self.df['Cost'] = pd.to_numeric(self.df['Cost'], errors='coerce').round(2)
        self.df['Impressions'] = pd.to_numeric(self.df['Impressions'], errors='coerce')
        self.df['Clicks'] = pd.to_numeric(self.df['Clicks'], errors='coerce')

        if 'Video views at 100%' in self.df.columns:
            self.df['Video views at 100%'] = pd.to_numeric(self.df['Video views at 100%'], errors='coerce').fillna(0)
        else:
            self.df['Video views at 100%'] = 0

        self.df.fillna({'Impressions': 0, 'Clicks': 0, 'Video views at 100%': 0}, inplace=True)
        self.df['Numero'] = 0
        self.df['Veiculo'] = 'TikTok'
        self.df['ID'] = pd.Series(dtype='str')

    def aplicar_substituicoes_objetivo(self):
        col_obj = 'Campaign objective type'
        if col_obj in self.df.columns:
            for old_val, new_val in self.substituicoes['Objetivo'].items():
                mask = self.df[col_obj] == old_val
                self.df.loc[mask, col_obj] = new_val
        else:
            logging.warning("Coluna 'Campaign objective type' nao encontrada para substituicoes de Objetivo.")

    def aplicar_parametrizacao_campanha_externa(self):
        col_campaign = 'Campaign name'
        if col_campaign not in self.df.columns:
            logging.warning("Coluna 'Campaign name' nao existe. Parametrizacao de campanha nao sera aplicada.")
            return

        self.df['Campanha'] = self.df[col_campaign].apply(lambda x: buscar_mapping(self.mapping_campanha, x) or x)
        self.df['ID_Campanha'] = self.df[col_campaign].apply(lambda x: buscar_mapping(self.mapping_sigla, x))

    def obter_estado_via_geolocalizacao(self):
        col_province = 'Province name'
        if col_province not in self.df.columns:
            logging.warning(f"Coluna '{col_province}' nao encontrada. Nao sera possivel identificar estado/regiao.")
            self.df['Estado'] = 'Nao identificado'
            return

        estados_calculados = []
        for idx, row in self.df.iterrows():
            prov_original = row[col_province]
            estado = obter_estado_de_regiao(
                prov_original,
                cache_municipios=self.cache_municipios,
                cache_estados=self.cache_estados
            )
            estados_calculados.append(estado)
            self._dict_correspondencia_regiao[prov_original] = estado

        self.df['Estado'] = estados_calculados

    def atribuir_id_veiculo(self):
        self.df['ID_Veiculo'] = self.id_veiculo

    def remover_colunas_desnecessarias(self):
        cols_to_drop = ['Campaign ID', 'Province name']
        for c in cols_to_drop:
            if c in self.df.columns:
                self.df.drop(columns=[c], inplace=True)

    def renomear_colunas(self):
        col_mapping = {
            'Date': 'Data',
            'Advertiser name': 'Nome_da_Conta',
            'Ad group name': 'Nome_do_Conjunto_de_Anuncio',
            'Ad name': 'Nome_do_Anuncio',
            'Impressions': 'Impressoes',
            'Cost': 'Investimento',
            'Clicks': 'Cliques_no_Link',
            'Video views at 100%': 'Visualizacoes_ate_100',
            'Campaign objective type': 'Objetivo',   # <-- Adicione esta linha
            'Estado': 'Regiao'
        }
        self.df.rename(columns=col_mapping, inplace=True)


    def reordenar_colunas(self):
        desired_order = [
            'Numero', 'Data', 'Nome_da_Conta', 'Veiculo', 'ID_Veiculo', 'Campanha', 'ID_Campanha',
            'Nome_do_Conjunto_de_Anuncio', 'Nome_do_Anuncio','Objetivo', 'Regiao', 'Impressoes', 'Investimento',
            'Cliques_no_Link', 'Visualizacoes_ate_100', 'ID'
        ]
        for col in desired_order:
            if col not in self.df.columns:
                self.df[col] = ""
        self.df = self.df[desired_order]

    def gerar_id_unico(self):
        self.df['ID'] = self.df.apply(
            lambda row: f"{row['Data']}-{row['Campanha']}-{row['Regiao']}-{row['Impressoes']}-{row['Investimento']}-{row['Cliques_no_Link']}",
            axis=1
        )

    def exibir_correspondencia_regiao(self, como_retorno: bool = False):
        if como_retorno:
            return self._dict_correspondencia_regiao
        else:
            print("Dicionario de correspondencia (Province name -> Estado/Regiao):")
            for k, v in self._dict_correspondencia_regiao.items():
                print(f"  '{k}' => '{v}'")

    def processar(self) -> pd.DataFrame:
        self.ajustar_tipos()
        self.aplicar_substituicoes_objetivo()
        self.aplicar_parametrizacao_campanha_externa()
        self.obter_estado_via_geolocalizacao()
        self.atribuir_id_veiculo()
        self.remover_colunas_desnecessarias()
        self.renomear_colunas()
        self.reordenar_colunas()
        self.gerar_id_unico()
        return self.df
