import logging


def preencher_campos_com_campanha(df):
    """
    Para plataformas como Pinterest, Age, Gender e Region,
    mantém os valores de 'Campanha' e 'ID_Campanha'
    e aproveita a coluna 'Campaign_name' para preencher:
      - Nome_do_Conjunto_de_Anuncio
      - Nome_do_Anuncio
    """
    logging.debug(">>> In preencher_campos_com_campanha (Pinterest)")

    if "Campaign_name" not in df.columns:
        logging.warning(
            "Coluna 'Campaign_name' não encontrada. Não será possível preencher campos de anúncio."
        )
        df["Nome_do_Conjunto_de_Anuncio"] = ""
        df["Nome_do_Anuncio"] = ""
        return df

    # Mantém 'Campanha' e 'ID_Campanha' vindos do lookup.
    # Usa 'Campaign_name' apenas para Nome_do_Conjunto_de_Anuncio/Anuncio.
    df["Nome_do_Conjunto_de_Anuncio"] = df["Campaign_name"]
    df["Nome_do_Anuncio"] = df["Campaign_name"]

    return df
