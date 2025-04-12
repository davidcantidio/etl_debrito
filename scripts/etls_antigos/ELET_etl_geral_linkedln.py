import pandas as pd
import numpy as np
import re
import unicodedata
import logging
from utils.campanha_mapper import buscar_mapping


class etl_geral_linkedin:
    def __init__(self, df):
        self.df = df
        self.substituicoes = {
            'Campanha': { 
                '2024_5_ALWAYS ON _ALC_CPM': 'Always On - Dia Mundial da Energia',
                '2024_7_BR_ALC_CPM_INTERESSES EM OLIMPIADAS, GINASTICA, ESPORTES, ENERGIA': 'Oportunidade - Time do Flamengo',
                '2024_8_ALWAYS ON _ALC_CPM' : 'Always On - Institucional',
                '2024_8_ENERGIA QUE NÃO PARA - COMERCIALIZAÇÃO_CONS_CPM' : 'Always On - Comercialização',
                '2024_8_BR_VÍDEO_VÍDEO - AGORA VOCÊ COMPRA ENERGIA DA ELETROBRAS_ACAO_ART-ELEBR-2024-EQNP-COM0044' : 'Always On - Comercialização',
                'ENERGIA QUE NÃO PARA - INSTITUCIONAL' : 'Always On - Institucional',
                'ACAO_ART-ELEBR-2024-AON-OPT-MAK0082' : 'Oportunidade - Makai',
                'ACAO_ART-ELEBR-2024-AON-OPT-MAK0083' : 'Oportunidade - Makai',
                'ACAO_ART-ELEBR-2024-AON-OPT-MAK0084': 'Oportunidade - Makai',
                'ACAO_ART-ELEBR-2024-AON-OPT-MAK0085' : 'Oportunidade - Makai',
                'ACAO_ART-ELEBR-2024-AON-OPT-MAK0086' : 'Oportunidade - Makai',
                'ELEBR-2024-SEG0089' : 'Segurança Elétrica',
                'ELEBR-2024-SEG0087' : 'Segurança Elétrica',
                'ELEBR-2024-SEG0088' : 'ELEBR-2024-SEG0088',
                'ELEBR-2024-SEG0090' : 'Segurança Elétrica',
                'ELÉTRICA_ACAO_ART-ELEBR-2024-SEG0091' : 'Segurança Elétrica',
                'ELÉTRICA_ACAO_ART-ELEBR-2024-SEG0092' : 'Segurança Elétrica',
                'MAKAI' : 'Oportunidade - Makai',
                'SEGURANÇA ELÉTRICA' : 'Segurança Elétrica', 
                'art-elebr-2024-eqnp-inst0113' : 'Always On - Institucional',
                'art-elebr-2024-eqnp-inst0114': 'Always On - Institucional',
                'art-elebr-2024-eqnp-inst0116' : 'Always On - Institucional',
                'art-elebr-2024-eqnp-inst0115': 'Always On - Institucional',
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
                '2025_1_SEGURANÇA ELÉTRICA_ALC_CPM' : 'Segurança Elétrica',
                '2025_2_SEGURANÇA ELÉTRICA_ALC_CPM' : 'Segurança Elétrica',
                '2025_2_SEGURANÇA ELÉTRICA_VIEW_CPV' : 'Segurança Elétrica',
                '2025_1_ALWAYS ON - COMERCIALIZAÇÃO_TRAF_COMERCIALIZACAO _CPC' : 'Always On - Comercialização',
                '2025_2_ALWAYS ON - COMERCIALIZAÇÃO_CONS_ENERGIA_QUE_NAO_PARA_COMERCIALIZACAO_CONSIDERAÇÃO': 'Always On - Comercialização',
                '2025_2_ALWAYS ON - INSTITUCIONAL_ALC_ENERGIA_QUE_NAO_PARA_INSTITUCIONAL_ALCANCE' : 'Always On - Institucional', 
                '2025_2_ALWAYS ON - INSTITUCIONAL_VIEW_ENERGIA_QUE_NAO_PARA_INSTITUCIONAL_VISUALIZAÇÃO' : 'Always On - Institucional',
                '2025_2_ALWAYS ON - COMERCIALIZAÇÃO_VIEW_ENERGIA_QUE_NAO_PARA_COMERCIALIZACAO_VISUALIZAÇÃO' : 'Always On - Comercialização'
            },
            'ID_Content': {
                '2024_7_BR_VÍDEO_REELS - ATLETAS FLAMENGO OLIMPÍADAS - IMPULSIONADO_ACAO_ELET-AW0025':'art-elebr-2024-opn-fla0025',
                '2024_6_BR_CARROSSEL_FUTURO_VERDE_CARROSSEL_ACAO_ELET-AW0011':'art-elebr-2024-aon-dma0011',
                '2024_9_BR_CARD_CARD - ABF - SOU FRANQUEADO_ACAO_AW-NEW0401': 'aw-new0401',
                '2024_9_BR_CARD_CARD - ABF - SOU FRANQUEADO_ACAO_AW-NEW0402':'aw_new0402',
                '2024_6_BR_CARD_CARD-PARCERIAS_ACAO_ESG-JJ0008':'esg-jj00082',
                '2024_9_BR_CARD_WHATSAPP-LEADS-PLANEJAMENTO_ACAO_AW-NEW0448': 'aw-new0448',
                '2024_9_BR_CARD_WHATSAPP-LEADS-PLANEJAMENTO_ACAO_AW-NEW0449': 'aw-new0449',
                '2024_9_BR_CARD_WHATSAPP-LEADS-POS-VENDA_ACAO_AW-NEW0446': 'aw-new0446',
                '2024_9_BR_CARD_WHATSAPP-LEADS-POS-VENDA_ACAO_AW-NEW0447': 'aw-new0447',
                '2024_10_BR_VÍDEO_REELS - JACIRA DOCE_ACAO_ART-SBRAE-2024-COP0504': 'art-sbrae-2024-cop0504',
                '2024_10_BR_VÍDEO_REELS - MATHEUS Total spentA_ACAO_ART-SBRAE-2024-COP0502': 'art-sbrae-2024-cop0502',
                '2024_10_BR_VÍDEO_REELS - JACIRA DOCE_ACAO_ART-SBRAE-2024-COP0505': 'art-sbrae-2024-cop0505',
                '2024_10_BR_VÍDEO_REELS - MATHEUS Total spentA_ACAO_ART-SBRAE-2024-COP0503': 'art-sbrae-2024-cop0503',
                '2024_7_BR_VÍDEO_REELS - ATLETAS FLAMENGO OLIMPÍADAS - IMPULSIONADO_ACAO_ELET-AW0025' : 'art-elebr-2024-opn-fla0025',
                '2024_6_BR_CARROSSEL_FUTURO_VERDE_CARROSSEL_ACAO_ELET-AW0011': 'art-elebr-2024-aon-dma0011',
                '2024_8_BR_VÍDEO_VÍDEO - AGORA VOCÊ COMPRA ENERGIA DA ELETROBRAS_ACAO_ART-ELEBR-2024-EQNP-COM0044' : 'art-elebr-2024-eqnp-com0044',
                '2024_8_BR_VÍDEO_VÍDEO - AGORA VOCÊ COMPRA ENERGIA DA ELETROBRAS_ACAO_ART-ELEBR-2024-EQNP-COM0045' : 'art-elebr-2024-eqnp-com0045',
                '2024_11_BR_VÍDEO_VIDEO - CONTEXTUALIZAÇÃO FAZENDA 2 - AGRO_ACAO_ART-ELEBR-2024-EQNP-COM0242' : 'art-elebr-2024-eqnp-com0242', 
                '2024_11_BR_VÍDEO_VIDEO - CONTEXTUALIZAÇÃO FAZENDA 2 - AGRO_ACAO_ART-ELEBR-2024-EQNP-COM0243' : 'art-elebr-2024-eqnp-com0243',
                '2024_11_ART-ELEBR-2024-EQNP-COM_BR_CONV_CARD_CPM_CARD - POST ESTÁTICO 1080X1080  - GERAL__ACAO_ART-ELEBR-2024-EQNP-COM0236':'art-elebr-2024-eqnp-com0236', 
                '2024_11_ART-ELEBR-2024-EQNP-COM_BR_CONV_CARD_CPM_CARD 2 - POST ESTÁTICO 1080X1080  - GERAL__ACAO_ART-ELEBR-2024-EQNP-COM0237':'art-elebr-2024-eqnp-com0237',
                '2025_1_ART_ELET_2025_AWN_COM_BR_TRAF_CARD_CPC_ELETROBRAS_CONTEUDO_ALWAYS-ON_JAN_LINKDIN_COMERCIALIZACAO_ESTATICO-1_COMERCIALIZACAO GERAL_ACAO_ART_ELET_2025_AWN_COM0063':'art_elet_2025_awn_com0063',
                '2025_1_ART_ELET_2025_AWN_COM_BR_TRAF_CARD_CPC_ELETROBRAS_CONTEUDO_ALWAYS-ON_JAN_LINKDIN_COMERCIALIZACAO_ESTATICO-2_COMERCIALIZACAO GERAL_ACAO_ART_ELET_2025_AWN_COM0064':'art_elet_2025_awn_com0064',
                '2025_1_ART_ELET_2025_AWN_COM_BR_TRAF_CARD_CPC_ENERGIA-PARA-IMPULSIONAR_AS 18+ BR EMPRESAS_ACAO_ART_ELET_2025_AWN_COM0051' : 'art_elet_2025_awn_com0051',
                '2024_10_ART-ELEBR-2024-EQNP-COM_BR_CONV_VÍDEO_CPC_VÍDEO - CAFÉ DA RAQUEL__ACAO_ART-ELEBR-2024-EQNP-COM0160':'art-elebr-2024-eqnp-com0160',
                '2024_10_ART-ELEBR-2024-EQNP-COM_BR_CONV_VÍDEO_CPC_VÍDEO - ESCRITÓRIO__ACAO_ART-ELEBR-2024-EQNP-COM0161': 'art-elebr-2024-eqnp-com0161',
                '2024_11_ART-ELEBR-2024-EQNP-COM_BR_CONS_VÍDEO_CPC_VÍDEO - GIRO GERAL__ACAO_ART-ELEBR-2024-EQNP-COM0265': 'art-elebr-2024-eqnp-com0265',
                '2024_11_ART-ELEBR-2024-EQNP-COM_BR_CONS_VÍDEO_CPC_VÍDEO - LABORATÓRIO__ACAO_ART-ELEBR-2024-EQNP-COM0266': 'art-elebr-2024-eqnp-com0266',
                '2024_11_ART-ELEBR-2024-EQNP-COM_BR_CONS_VÍDEO_CPM_VIDEO - CONTEXTUALIZAÇÃO FAZENDA 1 - GERAL__ACAO_ART-ELEBR-2024-EQNP-COM0234': 'art-elebr-2024-eqnp-com0234',
                '2024_11_ART-ELEBR-2024-EQNP-COM_BR_CONS_VÍDEO_CPM_VIDEO - CONTEXTUALIZAÇÃO FAZENDA 2 - AGRO__ACAO_ART-ELEBR-2024-EQNP-COM0235': 'art-elebr-2024-eqnp-com0235',
                '2024_10_ART-ELEBR-2024-SEG_BR_VIEW_VÍDEO_CPV_VÍDEO - GENTE_GERAL_ACAO_ART-ELEBR-2024-EQNP-INST0170': 'art-elebr-2024-eqnp-inst0170',
                '2024_11_ART-ELEBR-2024-EQNP-INST_BR_ALC_VÍDEO_CPM_VÍDEO - A ENERGIA DOS BRASILEIROS__ACAO_ART-ELEBR-2024-EQNP-INST0264': 'art-elebr-2024-eqnp-inst0264',
                'art-elebr-2024-aon-opt-mak0082':'art-elebr-2024-opt-mak0082',
                'art-elebr-2024-aon-opt-mak0083':'art-elebr-2024-opt-mak0083',
                'art-elebr-2024-aon-opt-mak0084': 'art-elebr-2024-opt-mak0084',
                'art-elebr-2024-aon-opt-mak0085': 'art-elebr-2024-opt-mak0085',
                'art-elebr-2024-aon-opt-mak0086': 'art-elebr-2024-opt-mak0086',
                'elet-aw0008' : 'art-elebr-2024-aon-dme0008',
                'elet-aw0007' : 'art-elebr-2024-aon-dme0007',
                'elet-aw0009' : 'art-elebr-2024-aon-dme0009',
                'elet-aw0022' : 'art-elebr-2024-aon-dma0022',
                'elet-aw0023' : 'art-elebr-2024-aon-dme0023',
                'elet-aw0024' : 'art-elebr-2024-aon-dma0024',
                'elet-aw0029' : 'art-elebr-2024-opn-fla0029',
                'elet-aw0028' : 'art-elebr-2024-opn-fla0028',
                'elet-aw0030' : 'art-elebr-2024-opn-pvi0030',
                'elet-aw0031' : 'art-elebr-2024-opn-pvi0031',
                'elet-aw0032' : 'art-elebr-2024-opn-pvi0032',
                'elet-aw0039' : 'art-elebr-2024-eqnp-inst0039',
                'elet-aw0041' : 'art-elebr-2024-eqnp-inst0041',
                'elet-aw0042' : 'art-elebr-2024-eqnp-inst0042',
                'elet-aw0043' : 'art-elebr-2024-eqnp-com0043',
                'elet-aw0044' : 'art-elebr-2024-eqnp-com0044',
                'elet-aw0045' : 'art-elebr-2024-eqnp-com0045',
                'elet-aw0046' : 'art-elebr-2024-eqnp-inst0046',
                'elet-aw0047' : 'art-elebr-2024-eqnp-inst0047',
                'elet-aw0048' : 'art-elebr-2024-eqnp-inst0048',
                'elet-aw0050' : 'art-elebr-2024-eqnp-com0050',                
                'elet-aw0053' : 'art-elebr-2024-eqnp-com0053',
                'elet-aw0054' : 'art-elebr-2024-eqnp-com0054',
                'elet-aw0055' : 'art-elebr-2024-eqnp-com0055',
                'elet-aw0056' : 'art-elebr-2024-eqnp-com0056',
                'elet-aw0035' :'art_elet_2025_awn_com0035',
                'elet-aw0034' :'art_elet_2025_awn_com0034',
                'elet-aw0033' :'art_elet_2025_awn_com0033',
                'elet-aw0032' : 'art_elet_2025_awn_inst0032',
                'elet-aw0031' : 'art_elet_2025_awn_inst0031',
                'art-elebr-2024-seg0000' :' art_elet_2025_seg0000',
                'art-elebr-2024-seg0001' : 'art_elet_2025_seg0001',
                'art_elebr_2025_seg0009' : 'art_elet_2025_seg0009',
                'art_elebr_2025_seg0010' : 'art_elet_2025_seg0010',
                'art_elebr_2025_seg0011' : 'art_elet_2025_seg0011',
                'art_elebr_2025_seg0012' : 'art_elet_2025_seg0012',
                'art_elebr_2025_seg0049' : 'art_elet_2025_seg0049',
                'art_elebr_2025_seg0050' : 'art_elet_2025_seg0050',
                'art_elebr_2025_seg0051' : 'art_elet_2025_seg0051',    
             },
            'ID_Campanha':{
                'Oportunidade - Makai': 'art-elebr-2024-opt-mak',
                'Segurança Elétrica': 'art-elebr-2024-seg',
                'Always On - Dia Mundial da Energia': 'art-elebr-2024-aon-dme',
                'Always On - Dia Mundial do Meio Ambiente': 'art-elebr-2024-aon-dma',
                'Oportunidade - Time do Flamengo': 'art-elebr-2024-opn-fla',
                'Oportunidade - Prêmio Valor Inovação': 'art-elebr-2024-opn-pvi',
                'Energia que não para - Comercialização': 'art-elebr-2024-eqnp-com',
                'Energia que não para - Institucional': 'art-elebr-2024-eqnp-inst',
                'Segurança Elétrica': 'art-elebr-2024-seg',
                'Always On - Comercialização':'art_elet_2025_awn_com',
                'Always On - Institucional' : 'art_elet_2025_awn_inst'
            },
            'Objetivo':{
                'BRAND_AWARENESS': 'Alcance',
                'WEBSITE_VISIT': 'Tráfego',
                'VIDEO_VIEW': 'Engajamento',
                'LEAD_GENERATION' : 'Conversão'
            }
        }

    def ajustar_tipos(self):
        self.df['Date'] = pd.to_datetime(self.df['Date'], errors='coerce')
        self.df['Campaign start date'] = pd.to_datetime(self.df['Campaign start date'], errors='coerce')
        self.df['Campaign end date'] = pd.to_datetime(self.df['Campaign end date'], errors='coerce')
        self.df['Total spent'] = pd.to_numeric(self.df['Total spent'], errors='coerce').round(2)
        self.df['Shares'] = pd.to_numeric(self.df['Shares'], errors='coerce')
        self.df['Reactions'] = pd.to_numeric(self.df['Reactions'], errors='coerce')
        self.df['Comments'] = pd.to_numeric(self.df['Comments'], errors='coerce')
        self.df = self.df.fillna({'Shares': 0, 'Reactions': 0, 'Comments': 0})
        self.df['Engajamento_Total'] = self.df['Shares'] + self.df['Comments'] + self.df['Reactions'] 
        self.df['Numero'] = np.nan
        self.df.fillna({'Numero':0}, inplace=True)
        self.df['Veiculo'] = 'Linkedin'
        self.df['ID'] = np.nan
        self.df['Utm Content'] = np.nan

    def etl_dicionario(self, coluna_origem, coluna_destino, substituicoes_coluna):
        self.df[coluna_destino] = self.df[coluna_origem].apply(
            lambda x: buscar_mapping(substituicoes_coluna, x)
        )

    def aplicar_substituicoes(self):
        self.etl_dicionario('Campaign name', 'Campanha', self.substituicoes['Campanha'])
        self.etl_dicionario('Campaign objective type', 'Objetivo', self.substituicoes['Objetivo'])
        self.etl_dicionario('Campaign name', 'ID_Campanha', self.substituicoes['ID_Campanha'])

    def remover_colunas(self):
        remover_colunas = ['Campaign objective type', 'Campaign name', 'Campaign ID', 'Creative text']
        self.df.drop(columns=remover_colunas, inplace=True)

    def renomear_colunas(self):
        renomear_colunas = {
            'Date': 'Data',
            'Account name': 'Nome_da_Conta',
            'Campaign group name': 'Nome_do_Conjunto_de_Anuncio',
            'Creative Direct Sponsored Content name': 'Nome_do_Anuncio',
            'Campaign start date': 'Inicio_da_Campanha',
            'Campaign end date': 'Fim_da_Campanha',
            'Creative thumbnail URL': 'URL_do_Anuncio',
            'Ad name': 'Nome_do_Anuncio',
            'Video views at 25%': 'Visualizacoes_ate_25',
            'Video views at 50%': 'Visualizacoes_ate_50', 
            'Video views at 75%': 'Visualizacoes_ate_75',
            'Video completions ': 'Visualizacoes_ate_100',
            'Total spent' : 'Investimento',
            'Clicks' : 'Cliques_no_Link',
            'Video starts' : 'Video_Play',
            'Impressions': 'Impressoes',
            'Reactions': 'Reacoes',
            'Shares': 'Compartilhamentos',
            'Comments': 'Comentarios'
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
        self.df['ID_Content'] = self.df['Creative text'].apply(lambda x: x.lower() if isinstance(x, str) else x)
        self.remover_colunas()
        self.renomear_colunas()
        self.reordenar_colunas()
        return self.df
