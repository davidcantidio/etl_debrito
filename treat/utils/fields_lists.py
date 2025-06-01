# utils/fields_lists.py

# Componentes base reutilizáveis
DIMENSIONS_BASE_COLUMNS_ORDER = [
    'Numero',
    'Data',
    'Nome_da_Conta',
    'Campanha',
    'ID_Campanha'
]

NUMERIC_COLUMNS = [
    'Impressoes',
    'Investimento',
    'Cliques_no_Link',
    'Video_Play',
    'Visualizacoes_ate_25',
    'Visualizacoes_ate_50',
    'Visualizacoes_ate_75',
    'Visualizacoes_ate_100',
    'Reacoes',
    'Compartilhamentos',
    'Comentarios'
]

# Modelo geral (completo)
GENERAL_MODEL_COLUMN_ORDER = [
    'Numero',
    'date',
    'account_name',
    'Campanha',
    'ID_Campanha',
    'Veiculo',
    'ID_Veiculo',
    'ad_group_name',
    'ad_name',
    'start',
    'end',
    'objective',
    'URL_do_Anuncio',
    'utm_content',
    'cost',
    'impressions',
    'link_clicks',
    'video_play',
    'video_watches_25',
    'video_watches_50',
    'video_watches_75',
    'video_watched_100',
    'post_reactions',
    'post_shares',
    'post_comments',
    'Engajamento_Total',
    'ID'
]


# Modelo específico para ETL de Idade
AGE_MODEL_COLUMN_ORDER = [
    'Numero',
    'Data',
    'Nome_da_Conta',
    'ID_Veiculo',
    'Veiculo',
    'ID_Campanha',
    'Campanha',
    'Nome_do_Conjunto_de_Anuncio',
    'Nome_do_Anuncio',
    'Objetivo',
    'Idade',
    'Impressoes',
    'Investimento',
    'Cliques_no_Link',
    'Visualizacoes_ate_100',
    'ID'
]

# Modelo específico para ETL de Gender
GENDER_MODEL_COLUMN_ORDER = [
    'Numero',
    'Data',
    'Nome_da_Conta',
    'ID_Veiculo',
    'Veiculo',
    'ID_Campanha',
    'Campanha',
    'Nome_do_Conjunto_de_Anuncio',
    'Nome_do_Anuncio',
    'Objetivo',
    'Genero',
    'Impressoes',
    'Investimento',
    'Cliques_no_Link',
    'Visualizacoes_ate_100',
    'ID'
]

# Modelo específico para ETL de Região
REGION_MODEL_COLUMN_ORDER = [
    'Numero',
    'Data',
    'Nome_da_Conta',
    'ID_Veiculo',
    'Veiculo',
    'ID_Campanha',
    'Campanha',
    'Nome_do_Conjunto_de_Anuncio',
    'Nome_do_Anuncio',
    'Objetivo',
    'Estado',
    'Impressoes',
    'Investimento',
    'Cliques_no_Link',
    'Visualizacoes_ate_100',
    'ID'
]


# Modelo específico para ETL de Reach
REACH_MODEL_COLUMN_ORDER =  [
    'Numero',
    'Data',
    'Nome_da_Conta',
    'Veiculo',
    'ID_Veiculo',
    'ID_Campanha',
    'Campanha',
    'Nome_do_Conjunto_de_Anuncio',
    'Nome_do_Anuncio',
    'Objetivo',
    'Alcance',
    'Impressoes',
    'ID'
]
