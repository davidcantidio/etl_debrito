# campanha_mapper.py
from utils.normalize import normalize_campaign_name

def buscar_mapping(mapping, valor):
    """
    Recebe um dicionário 'mapping' e uma string 'valor' representando o nome da campanha.
    Normaliza o valor removendo espaços e convertendo para maiúsculas, e verifica se alguma chave
    do dicionário está contida no valor normalizado.
    
    Se encontrada, retorna o valor mapeado; caso contrário, retorna o próprio valor original.
    """
    if not isinstance(valor, str):
        return valor
    valor_norm = normalize_campaign_name(valor)
    for chave, mapped_value in mapping.items():
        # Se a chave (presumidamente maiúscula) estiver contida em valor_norm, retorna o mapeamento
        if chave in valor_norm:
            return mapped_value
    return valor
