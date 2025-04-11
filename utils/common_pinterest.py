import logging

def preencher_campos_com_campanha(df):
    """
    Para plataformas como Pinterest onde não há Ad Name,
    replica 'Campaign name' nos campos de destino:
    - Campanha
    - Nome_do_Anuncio
    - Nome_do_Conjunto_de_Anuncio
    """
    logging.debug(">>> In preencher_campos_com_campanha (Pinterest)")

    if 'Campaign name' not in df.columns:
        logging.warning("Coluna 'Campaign name' não encontrada. Não será possível preencher campos derivados.")
        df['Campanha'] = ""
        df['Nome_do_Anuncio'] = ""
        df['Nome_do_Conjunto_de_Anuncio'] = ""
        return df

    df['Campanha'] = df['Campaign name']
    df['Nome_do_Anuncio'] = df['Campaign name']
    df['Nome_do_Conjunto_de_Anuncio'] = df['Campaign name']
    return df
