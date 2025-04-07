# utils/numeracao.py

def gerar_numeracao(df, numero_inicial=1, coluna='Numero'):
    """
    Gera uma numeração sequencial para as linhas de um DataFrame.

    Parâmetros:
        df (pandas.DataFrame): DataFrame no qual a numeração será inserida.
        numero_inicial (int, opcional): Número inicial da sequência. Padrão é 1.
        coluna (str, opcional): Nome da coluna onde a numeração será inserida. Padrão é 'Numero'.
    
    Retorna:
        pandas.DataFrame: O DataFrame com a coluna de numeração atualizada.
    """
    df[coluna] = range(numero_inicial, numero_inicial + len(df))
    return df
