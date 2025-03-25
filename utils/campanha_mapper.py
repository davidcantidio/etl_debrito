# utils/campanha_mapper.py

def buscar_mapping(mapping, valor):
    """
    Recebe um dicionário 'mapping' e uma string 'valor' representando o nome da campanha.
    Normaliza o valor removendo espaços e convertendo para maiúsculas, e procura por uma chave que esteja contida no valor.
    Retorna o valor mapeado se encontrado; caso contrário, retorna o próprio valor original.
    
    Exemplo:
        mapping = {"CAMPANHA A": "Campanha A Mapeada", "CAMPANHA B": "Campanha B Mapeada"}
        buscar_mapping(mapping, "Campanha a especial")  -> "Campanha A Mapeada"
    """
    if not isinstance(valor, str):
        return valor
    valor_norm = valor.strip().upper()
    for chave, mapped_value in mapping.items():
        if chave in valor_norm:
            return mapped_value
    return valor

