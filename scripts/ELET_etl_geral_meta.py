import pandas as pd
import numpy as np
import re
import unicodedata

class etl_geral_meta:
    def __init__(self, df):
        self.df = df
        self.substituicoes = {
            'Campanha': { 
                'ELEBR-2024-SEG0089' : 'Segurança Elétrica 2024',
                'ELEBR-2024-SEG0087' : 'Segurança Elétrica 2024',
                'ELEBR-2024-SEG0088' : 'Segurança Elétrica 2024',
                'ELEBR-2024-SEG0090' : 'Segurança Elétrica 2024',
                'ELÉTRICA_ACAO_ART-ELEBR-2024-SEG0091' : 'Segurança Elétrica 2024',
                'ELÉTRICA_ACAO_ART-ELEBR-2024-SEG0092' : 'Segurança Elétrica 2024',
                '2024_8_ENERGIA QUE NÃO PARA - COMERCIALIZAÇÃO_CONV_CPM [DEZ/24]' : 'Always On - Comercialização',
                '2025_1_ALWAYS ON - COMERCIALIZAÇÃO_TRAF_COMERCIALIZACAO _CPC_VIDEO': 'Always On - Comercialização',
                '2025_1_ALWAYS ON - COMERCIALIZAÇÃO_TRAF_COMERCIALIZACAO _CPC_CARD': 'Always On - Comercialização',
                '2025_1_ALWAYS ON - COMERCIALIZAÇÃO_TRAF_COMERCIALIZACAO _CPC' : 'Always On - Comercialização',
                '2025_1_ALWAYS ON - INSTITUCIONAL_VIEW_INSTITUCIONAL _CPV_VIDEO': 'Always On - Institucional',
                '2025_1_ALWAYS ON - INSTITUCIONAL_VIEW_ENERGIA_QUE_NAO_PARA_INSTITUCIONAL_VISUALIZAÇÃO': 'Always On - Institucional',
                '2025_1_ALWAYS ON - INSTITUCIONAL_VIEW_INSTITUCIONAL _CPV' : 'Always On - Institucional',
                '2025_1_ALWAYS ON_VIEW_INSTITUCIONAL _CPV ': 'Always On - Institucional',
                '2025_1_ALWAYS ON - INSTITUCIONAL_ALC_INSTITUCIONAL _CPM' : 'Always On - Institucional',
                '2025_1_ALWAYS ON - INSTITUCIONAL_VIEW_INSTITUCIONAL _CPV' : 'Always On - Institucional',
                '2025_1_SEGURANÇA ELÉTRICA_ALC_CPM': 'Segurança Elétrica',
                '2025_2_SEGURANÇA ELÉTRICA_ALC_CPM': 'Segurança Elétrica',
                '2025_2_SEGURANÇA ELÉTRICA_VIEW_CPV' : 'Segurança Elétrica',
                '2025_1_ALWAYS ON - COMERCIALIZAÇÃO_TRAF_COMERCIALIZACAO _CPC': 'Always On - Comercialização',
                '2025_2_ALWAYS ON - COMERCIALIZAÇÃO_CONS_ENERGIA_QUE_NAO_PARA_COMERCIALIZACAO_CONSIDERAÇÃO': 'Always On - Comercialização',
                '2025_2_ALWAYS ON - INSTITUCIONAL_ALC_ENERGIA_QUE_NAO_PARA_INSTITUCIONAL_ALCANCE' : 'Always On - Institucional', 
                '2025_2_ALWAYS ON - INSTITUCIONAL_VIEW_ENERGIA_QUE_NAO_PARA_INSTITUCIONAL_VISUALIZAÇÃO' : 'Always On - Institucional',
                '2025_2_ALWAYS ON - COMERCIALIZAÇÃO_VIEW_ENERGIA_QUE_NAO_PARA_COMERCIALIZACAO_VISUALIZAÇÃO' : 'Always On - Comercialização'
            },
            'ID_Content': {
                'art-elebr-2024-seg0000' : 'art_elebr_2025_seg0000',
                'art-elebr-2024-seg0001' : 'art_elebr_2025_seg0001',
                'art-elebr-2024-seg0042' : 'art_elebr_2025_seg0042',
                'art-elebr-2024-seg0006' : 'art_elebr_2025_seg0006',
                'art-elebr-2024-seg0007' : 'art_elebr_2025_seg0007',
                'art-elebr-2024-seg0008' : 'art_elebr_2025_seg0008',
                'art-elebr-2024-seg0003' : 'art_elebr_2025_seg0003',
                'art-elebr-2024-seg0004' : 'art_elebr_2025_seg0004',
                'art-elebr-2024-seg0005' : 'art_elebr_2025_seg0005',
                'art-elebr-2024-seg0000' : 'art_elet_2025_seg0000',
                'art-elebr-2024-seg0001' : 'art_elet_2025_seg0001',
                'art_elebr_2025_seg0009' : 'art_elet_2025_seg0009',
                'art_elebr_2025_seg0010' : 'art_elet_2025_seg0010',
                'art_elebr_2025_seg0011' : 'art_elet_2025_seg0011',
                'art_elebr_2025_seg0012' : 'art_elet_2025_seg0012',
                'art_elebr_2025_seg0049' : 'art_elet_2025_seg0049',
                'art_elebr_2025_seg0050' : 'art_elet_2025_seg0050',
                'art_elebr_2025_seg0051' : 'art_elet_2025_seg0051'
            },
            'ID_Campanha':{
                'Oportunidade - Makai': 'art-elebr-2024-opt-mak',
                'Segurança Elétrica 2024': 'art-elebr-2024-seg',
                'Always On - Dia Mundial da Energia': 'art-elebr-2024-aon-dme',
                'Always On - Dia Mundial do Meio Ambiente': 'art-elebr-2024-aon-dma',
                'Oportunidade - Time do Flamengo': 'art-elebr-2024-opn-fla',
                'Oportunidade - Prêmio Valor Inovação': 'art-elebr-2024-opn-pvi',
                'Energia que não para - Comercialização': 'art-elebr-2024-eqnp-com',
                'Energia que não para - Institucional': 'art-elebr-2024-eqnp-inst',
                'Always On - Institucional' : 'art_elet_2025_awn_inst',
                'Always On - Comercialização' : 'art_elet_2025_awn_com',
                'Oportunidade': 'art_elet_2025_opt',
                'Segurança Elétrica': 'art_elet_2025_seg'
            },
            'Objetivo':{
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
        self.df.fillna({'Numero':0}, inplace=True)
        self.df['ID'] = np.nan
    
    def etl_dicionario(self, coluna_origem, coluna_destino, substituicoes_coluna):
        self.df[coluna_destino] = self.df[coluna_origem].apply(
            lambda x: next((valor for chave, valor in substituicoes_coluna.items() if chave in x), x)
        )

    def aplicar_substituicoes(self):
        self.etl_dicionario('Campaign name', 'Campanha', self.substituicoes['Campanha'])
        self.etl_dicionario('Ad name', 'ID_Content', self.substituicoes['ID_Content'])
        self.etl_dicionario('Campaign objective', 'Objetivo', self.substituicoes['Objetivo'])
        self.etl_dicionario('Campanha', 'ID_Campanha', self.substituicoes['ID_Campanha'])
        self.etl_dicionario('Content (utm)', 'ID_Content', self.substituicoes['ID_Content'])
        
    def criar_veiculo(self):
        # Inicializa a coluna com string vazia para evitar problemas de dtype
        self.df['Veiculo'] = ""
        self.df.loc[self.df['Placement'].str.contains('facebook', case=False, na=False), 'Veiculo'] = 'Facebook'
        self.df.loc[self.df['Placement'].str.contains('instagram', case=False, na=False), 'Veiculo'] = 'Instagram'
        self.df.loc[self.df['Placement'].str.contains('unknow', case=False, na=False), 'Veiculo'] = 'Não Classificado'

    def atribuir_id_veiculo(self):
        self.df['ID_Veiculo'] = np.nan
        self.df['ID_Veiculo'] = np.where(self.df['Placement'].str.contains('facebook', case=False, na=False), 2, np.nan)
        self.df['ID_Veiculo'] = np.where(self.df['Placement'].str.contains('instagram', case=False, na=False), 3, self.df['ID_Veiculo'])

    def remover_colunas(self):
        remover_colunas = ['Placement', 'Campaign objective', 'Campaign name', 'Campaign ID', 'Content (utm)']
        self.df.drop(columns=remover_colunas, inplace=True)

    def renomear_colunas(self):
        renomear_colunas = {
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
        self.df.rename(columns=renomear_colunas, inplace=True)
    
    def reordenar_colunas(self):
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
        self.df = self.df[nova_ordem_colunas]

    def processar(self):
        self.ajustar_tipos()
        self.aplicar_substituicoes()
        self.criar_veiculo()
        self.atribuir_id_veiculo()
        self.remover_colunas()
        self.renomear_colunas()
        self.reordenar_colunas()
        # Geração do campo ID a partir de algumas colunas; 
        # aqui, garantimos que as colunas usadas sejam convertidas para string
        self.df['ID'] = self.df.apply(
            lambda row: f"{str(row['Data'])}-{str(row['ID_Content']).strip()}-{str(row['Impressoes']).strip()}-{str(row['Investimento']).strip()}-{str(row['Cliques_no_Link']).strip()}-{str(row['ID_Content']).strip()}",
            axis=1
        )
        return self.df
