import json
from geolocalizacao import (
    carregar_cache_estados,
    carregar_cache_municipios,
    obter_estado_de_regiao
)

# Caminhos para os arquivos de cache
path_cache_estados = "utils/cache_estados.json"
path_cache_municipios = "utils/cache_municipios_estados.json"

# Carrega os dicionários
cache_estados = carregar_cache_estados(path_cache_estados)
cache_municipios = carregar_cache_municipios(path_cache_municipios)

# Lista de regiões para testar
valores_para_teste = [
    "Acre (state)",
    "Rio de Janeiro (state)",
    "Unknown",
    "São Paulo (state)",
    "Federal District",
    "Sao Paulo",
    "Amapa",
    "Para",
    "-1",
    "BR-OTHER",
    "Greater Cuiaba",
    "Brazil: Maranhao",
    "Greater São Paulo Area",
    "Brazil: Sao Paulo",
    "Greater Richmond Region",
    "London Area, United Kingdom",
    "Los Angeles Metropolitan Area",
    "Greater Ribeirão Preto",
    "Greater São Luís Area",
    "Charlotte Metro",
    "Bogotá D.C. Metropolitan Area",
    "Greater Belem",
    "Vitoria, Brazil Metropolitan Area",
    "State of Piaui"
]

# Dicionário para guardar os resultados
resultados = {}

# Executa o teste
for valor in valores_para_teste:
    estado = obter_estado_de_regiao(valor, cache_estados, cache_municipios)
    resultados[valor] = estado

# Exibe o resultado como JSON formatado
print(json.dumps(resultados, indent=2, ensure_ascii=False))
