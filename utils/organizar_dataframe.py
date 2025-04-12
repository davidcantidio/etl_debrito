import logging

def remover_colunas_indesejadas(df):
    """
    Remove colunas que não fazem parte do modelo final.
    """
    colunas_para_remover = ['Placement', 'Campaign_ID', 'Campaign_name', 'Content_utm']
    for col in colunas_para_remover:
        if col in df.columns:
            df.drop(columns=col, inplace=True)
            logging.debug(f"Coluna removida: {col}")
    return df

def reordenar_colunas_para_modelo(df):
    """
    Reordena e garante a presença das colunas do modelo final.
    """
    ordem = [
        'Numero', 'Data', 'Nome_da_Conta', 'Campanha', 'ID_Campanha', 'Veiculo', 'ID_Veiculo',
        'Nome_do_Conjunto_de_Anuncio', 'Nome_do_Anuncio', 'Inicio_da_Campanha', 'Fim_da_Campanha',
        'Objetivo', 'URL_do_Anuncio', 'ID_Content', 'Investimento', 'Impressoes', 'Cliques_no_Link',
        'Video_Play', 'Visualizacoes_ate_25', 'Visualizacoes_ate_50', 'Visualizacoes_ate_75', 'Visualizacoes_ate_100',
        'Reacoes', 'Compartilhamentos', 'Comentarios', 'Engajamento_Total', 'ID'
    ]
    for col in ordem:
        if col not in df.columns:
            df[col] = ""
            logging.debug(f"Coluna adicionada (vazia): {col}")
    df = df[ordem]
    return df
