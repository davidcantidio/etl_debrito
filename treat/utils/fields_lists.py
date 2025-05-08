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
