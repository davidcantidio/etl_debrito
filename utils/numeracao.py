# utils/numeracao.py

import pandas as pd

def gerar_numeracao(df, df_destino=None, linha_insercao=2, coluna='Numero'):
    """
    Gera uma numeração sequencial para as linhas de um DataFrame de novos dados,
    levando em conta a numeração já existente na planilha (df_destino).

    Parâmetros:
        df (pandas.DataFrame): DataFrame dos novos dados a serem inseridos.
        df_destino (pandas.DataFrame, opcional): DataFrame já existente na planilha, com a coluna de numeração.
            Se fornecido e não vazio, a numeração dos novos dados começará a partir do maior valor existente + 1.
        linha_insercao (int, opcional): Caso df_destino não seja fornecido, assume que os dados na planilha 
            começam na linha 'linha_insercao' (por exemplo, 2 se a linha 1 for cabeçalho), e o primeiro número 
            gerado será (linha_insercao - 1).
        coluna (str, opcional): Nome da coluna onde a numeração será inserida. Padrão é 'Numero'.

    Retorna:
        pandas.DataFrame: O DataFrame com a coluna de numeração atualizada.
    """
    if df_destino is not None and not df_destino.empty and coluna in df_destino.columns:
        # Converte a coluna para numérico, ignorando erros
        serie = pd.to_numeric(df_destino[coluna], errors='coerce')
        ultimo_numero = serie.max()
        if pd.isna(ultimo_numero):
            ultimo_numero = linha_insercao - 1
        else:
            ultimo_numero = int(ultimo_numero)
        numero_inicial = ultimo_numero + 1
    else:
        numero_inicial = linha_insercao - 1

    df[coluna] = range(numero_inicial, numero_inicial + len(df))
    return df
