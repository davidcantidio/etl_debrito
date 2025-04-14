import logging

# Mapeamento de objetivos de campanha para nomes padronizados
SUBSTITUICOES_OBJETIVO = {
    'AWARENESS': 'Alcance',
    'COMMUNITY_INTERACTION': 'Engajamento',
    'APP_PROMOTION': 'Promoção do app',
    'REACH': 'Alcance',
    'VIDEO_VIEWS': 'Visualização',
    'TRAFFIC': 'Tráfego',
    'CONVERSIONS': 'Conversões',
    'LEAD_GENERATION': 'Geração de Leads',
    'OUTCOME_AWARENESS': 'Alcance',
    'OUTCOME_TRAFFIC': 'Tráfego',
    'OUTCOME_ENGAGEMENT': 'Engajamento',
    'OUTCOME_SALES': 'Vendas',
    'OUTCOME_APP+PROMOTION': 'Promoção do app',
    'BRAND_AWARENESS': 'Alcance',
    'WEBSITE_VISITS': 'Tráfego',
    'ENGAGEMENT': 'Engajamento',
    'WEBSITE_CONVERSIONS': 'Conversões',
    'CONSIDERATION': 'Tráfego',
    'CATALOG_SALES': 'Vendas',
    'WEBSITE_VISIT': 'Tráfego'
}

def aplicar_substituicoes_objetivo(df):
    """
    Substitui valores da coluna 'Objetivo' com base no dicionário SUBSTITUICOES_OBJETIVO.
    """
    if 'Objetivo' in df.columns:
        for old, new in SUBSTITUICOES_OBJETIVO.items():
            df.loc[df['Objetivo'] == old, 'Objetivo'] = new
        logging.debug("[aplicar_substituicoes_objetivo] Substituições aplicadas na coluna 'Objetivo'")
    else:
        logging.warning("[aplicar_substituicoes_objetivo] Coluna 'Objetivo' não encontrada no DataFrame.")
    return df


def renomear_colunas_origem_para_modelo(df):
    """
    Renomeia colunas de plataformas para os nomes padronizados do modelo geral.
    Ignora colunas ausentes sem erro.
    """
    renomear = {
        'Date': 'Data', 'Account name': 'Nome_da_Conta', 'Advertiser name': 'Nome_da_Conta',
        'Campaign name': 'Campaign_name', 'Ad group name': 'Nome_do_Conjunto_de_Anuncio',
        'Ad set name': 'Nome_do_Conjunto_de_Anuncio', 'Ad name': 'Nome_do_Anuncio',
        'Campaign ID': 'Campaign_ID', 'Start': 'Inicio_da_Campanha', 'End': 'Fim_da_Campanha',
        'Campaign objective type': 'Objetivo', 'Campaign objective': 'Objetivo',
        'Placement': 'Placement', 'Preview Link': 'URL_do_Anuncio',
        'Content (utm)': 'ID_Content', 'Impressions': 'Impressoes', 'Cost': 'Investimento',
        'Link clicks': 'Cliques_no_Link', 'Clicks': 'Cliques_no_Link',
        'Video play actions': 'Video_Play', 'Video views': 'Video_Play',
        'Video watches at 25%': 'Visualizacoes_ate_25', 'Video watches at 50%': 'Visualizacoes_ate_50',
        'Video watches at 75%': 'Visualizacoes_ate_75', 'Video watches at 100%': 'Visualizacoes_ate_100',
        'Post reactions': 'Reacoes', 'Paid likes': 'Reacoes', 'Post shares': 'Compartilhamentos',
        'Paid shares': 'Compartilhamentos', 'Post comments': 'Comentarios', 'Paid comments': 'Comentarios','Age': 'Faixa_Etaria'
    }
    df.rename(columns=renomear, inplace=True)
    logging.debug("[renomear_colunas_origem_para_modelo] Colunas após renomear: %s", list(df.columns))
    return df
