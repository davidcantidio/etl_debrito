# utils/numeracao.py
import pandas as pd

def gerar_numeracao(df, df_destino=None, linha_insercao=2, coluna='Numero'):
    """
    Gera uma numeração sequencial para as linhas de um DataFrame de novos dados,
    levando em conta a numeração já existente na planilha (df_destino).

    Se df_destino for fornecido e contiver a coluna, essa função converte os valores
    para numérico e utiliza o maior valor + 1 como ponto de partida.
    Caso contrário, usa o valor padrão (linha_insercao - 1).

    Parâmetros:
        df (pandas.DataFrame): DataFrame dos novos dados.
        df_destino (pandas.DataFrame, opcional): DataFrame já existente na planilha, contendo a coluna de numeração.
        linha_insercao (int, opcional): Valor base caso df_destino esteja vazio (por exemplo, 2 se a linha 1 for cabeçalho).
        coluna (str, opcional): Nome da coluna onde a numeração será inserida. Padrão é 'Numero'.
    
    Retorna:
        pandas.DataFrame: O DataFrame com a coluna de numeração atualizada.
    """
    numero_inicial = linha_insercao - 1  # valor padrão se não houver dados no destino
    if df_destino is not None and not df_destino.empty:
        # Tente localizar a coluna; se não achar "Numero", pode haver problemas de capitalização ou espaços.
        col_dest = None
        for col in df_destino.columns:
            if col.strip().lower() == coluna.strip().lower():
                col_dest = col
                break
        if col_dest:
            # Converte os valores para numérico
            serie = pd.to_numeric(df_destino[col_dest], errors='coerce')
            ultimo = serie.max()
            if pd.notna(ultimo):
                numero_inicial = int(ultimo) + 1
    df[coluna] = range(numero_inicial, numero_inicial + len(df))
    return df
