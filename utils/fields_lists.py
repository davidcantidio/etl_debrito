# utils/fields_lists.py

# Componentes base reutilizáveis
DIMENSIONS_BASE_COLUMNS_ORDER = [
    'Numero', 'Data', 'Nome_da_Conta', 'Campanha', 'ID_Campanha'
]

NUMERIC_COLUMNS = [
    'Impressoes', 'Investimento', 'Cliques_no_Link',
    'Video_Play', 'Visualizacoes_ate_25', 'Visualizacoes_ate_50',
    'Visualizacoes_ate_75', 'Visualizacoes_ate_100',
    'Reacoes', 'Compartilhamentos', 'Comentarios'
]

METRICS_BASE_COLUMNS_ORDER = NUMERIC_COLUMNS + ['ID']

# Modelo geral (completo)
GENERAL_MODEL_COLUMN_ORDER = DIMENSIONS_BASE_COLUMNS_ORDER + [
    'Veiculo', 'ID_Veiculo', 'Nome_do_Conjunto_de_Anuncio', 'Nome_do_Anuncio',
    'Inicio_da_Campanha', 'Fim_da_Campanha', 'Objetivo', 'URL_do_Anuncio', 'ID_Content'
] + NUMERIC_COLUMNS + ['Engajamento_Total'] + ['ID']

# Modelos específicos por dimensão
AGE_MODEL_COLUMN_ORDER = DIMENSIONS_BASE_COLUMNS_ORDER + ['Faixa_Etaria'] + METRICS_BASE_COLUMNS_ORDER
GENDER_MODEL_COLUMN_ORDER = DIMENSIONS_BASE_COLUMNS_ORDER + ['Genero'] + METRICS_BASE_COLUMNS_ORDER
REGION_MODEL_COLUMN_ORDER = DIMENSIONS_BASE_COLUMNS_ORDER + ['Regiao', 'Estado'] + METRICS_BASE_COLUMNS_ORDER
REACH_MODEL_COLUMN_ORDER = DIMENSIONS_BASE_COLUMNS_ORDER + ['Veiculo', 'Posicionamento', 'Alcance'] + ['Impressoes', 'ID']
