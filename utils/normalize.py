# utils/normalize.py

import unicodedata

def normalize_campaign_name(value):
    if not isinstance(value, str):
        return value
    return value.strip().upper()

def normalize_nome(nome):
    if not isinstance(nome, str):
        return ""
    nome = nome.strip().lower()
    nome = unicodedata.normalize("NFKD", nome)
    nome = ''.join([c for c in nome if not unicodedata.combining(c)])
    return nome
