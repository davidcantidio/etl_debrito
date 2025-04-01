import json
import os
import unicodedata

# -----------------------------------------------------------------------------
# Normalização de strings (remoção de acentos e caracteres especiais)
def normalize_string(s: str) -> str:
    if not isinstance(s, str):
        return ""
    # Remoção de acentos e caracteres especiais
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("ASCII")
    # Converte para lower e remove espaços extras
    s = s.lower().strip()
    return s

# -----------------------------------------------------------------------------
def carregar_cache_estados(path_cache_estados: str) -> dict:
    """
    Lê o JSON com a lista (ou dicionário) de estados do Brasil normalizados.
    Exemplo de formato (JSON):
      {
        "acre": "Acre",
        "alagoas": "Alagoas",
        "amapa": "Amapá",
        ...
      }
    """
    if not os.path.exists(path_cache_estados):
        return {}
    with open(path_cache_estados, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data  # {'acre': 'Acre', 'amapa': 'Amapá', ...}

def carregar_cache_municipios(path_cache_municipios: str) -> dict:
    """
    Lê o JSON contendo o mapeamento { "municipio_normalizado": "EstadoCorrespondente", ... }.
    """
    if not os.path.exists(path_cache_municipios):
        return {}
    with open(path_cache_municipios, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data

# -----------------------------------------------------------------------------
def limpeza_basica(regiao_bruta: str) -> str:
    """
    Aplica limpeza e remoção de padrões conhecidos na string.
    Ex.: "Greater São Paulo Area" -> "São Paulo"
         "Brazil: Bahia" -> "Bahia"
         "State of Piaui" -> "Piaui"
         "Federal District" -> "distrito federal" (a gente trata depois)
         "Acre (state)" -> "Acre"
         ...
    """
    if not isinstance(regiao_bruta, str):
        return ""

    texto = regiao_bruta.strip()

    # Se for alguma string especial que indica "Não identificado"
    if texto in ["-", "unknown", "-1", "br - other", "br-other"]:
        return ""  # sinalizamos string vazia => “Não identificado” depois

    # Remoção de prefixos e sufixos
    # "Brazil: " ...
    texto = texto.replace("Brazil: ", "")
    # "State of " ...
    texto = texto.replace("State of ", "")
    # "(state)" ...
    texto = texto.replace("(state)", "")
    # "federal district" => vira "distrito federal" (para bater no cache de estados)
    texto = texto.replace("Federal District", "Distrito Federal")
    # "Greater " ...
    texto = texto.replace("Greater ", "")
    # " metropolitan area"
    texto = texto.replace(" metropolitan area", "")
    # Se terminar com " area", remover
    if texto.lower().endswith(" area"):
        texto = texto[: -len(" area")].strip()
    # Se terminar com " metro", remover
    if texto.lower().endswith(" metro"):
        texto = texto[: -len(" metro")].strip()

    return texto.strip()

# -----------------------------------------------------------------------------
def obter_estado_de_regiao(regiao: str, cache_municipios: dict, cache_estados: dict) -> str:
    if not isinstance(regiao, str):
        return "Não identificado"

    regiao = regiao.strip().lower()

    # Regras específicas de limpeza
    if regiao in ["-", "unknown", "-1", "br - other", "br-other"]:
        return "Não identificado"

    regiao = (
        regiao.replace("brazil: ", "")
        .replace("state of ", "")
        .replace("(state)", "")
        .replace("greater ", "")
        .replace("federal district", "distrito federal")
        .replace(" metropolitan area", "")
        .replace(", brazil metropolitan area", "")
        .replace(" brazil", "")  # <<< IMPORTANTE: remove " brazil" que sobrou
        .replace(" area", "")
        .replace(" metro", "")
        .replace(",", "")   # remove a vírgula
        .strip()
    )

    regiao_normalizada = normalize_string(regiao)

    # Primeiro tenta bater com o cache de estados
    estado_direto = cache_estados.get(regiao_normalizada)
    if estado_direto:
        return estado_direto

    # Se não encontrou, tenta bater com os municípios
    estado_por_municipio = cache_municipios.get(regiao_normalizada)
    if estado_por_municipio:
        return estado_por_municipio

    return "Não identificado"

# -----------------------------------------------------------------------------
# Exemplo de script principal de teste
if __name__ == "__main__":
    # Arquivos de cache
    path_cache_estados = "utils/cache_estados.json"
    path_cache_municipios = "utils/cache_municipios_estados.json"

    # Carregamos as duas estruturas
    dict_estados = carregar_cache_estados(path_cache_estados)  # ex.: {"acre": "Acre", ...}
    dict_municipios = carregar_cache_municipios(path_cache_municipios)  # ex.: {"vitoria": "Espírito Santo", ...}

    # Exemplo de lista de valores para teste
    valores_para_teste = [
        "Acre (state)",
        "Federal District",
        "Amapa",
        "-1",
        "BR-OTHER",
        "Brazil: Maranhao",
        "Greater São Paulo Area",
        "London Area, United Kingdom",
        "Los Angeles Metropolitan Area",
        "Charlotte Metro",
        "Bogotá D.C. Metropolitan Area",
        "Greater Belem",
        "Vitoria, Brazil Metropolitan Area",
        "State of Piaui"
    ]

    resultados = {}
    for valor in valores_para_teste:
        estado = obter_estado_de_regiao(
            valor,
            cache_estados=dict_estados,
            cache_municipios=dict_municipios
        )
        resultados[valor] = estado

    import json
    print(json.dumps(resultados, indent=2, ensure_ascii=False))
