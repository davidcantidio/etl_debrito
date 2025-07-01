# trat.utils.geo_normalize

import json
import os
import unicodedata
from typing import Any, Tuple


def carregar_caches_padrao() -> Tuple[dict, dict]:
    path_estados = os.path.join(os.path.dirname(__file__), "cache_estados.json")
    path_municipios = os.path.join(
        os.path.dirname(__file__), "cache_municipios_estados.json"
    )

    cache_estados = carregar_cache_estados(path_estados)
    cache_municipios = carregar_cache_municipios(path_municipios)
    return cache_estados, cache_municipios


# -----------------------------------------------------------------------------
# Normalização de strings (remoção de acentos e caracteres especiais)
def normalize_string(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("ASCII")
    return s.lower().strip()


# -----------------------------------------------------------------------------


def carregar_cache_estados(path_cache_estados: str) -> dict:
    if not os.path.exists(path_cache_estados):
        return {}
    with open(path_cache_estados, "r", encoding="utf-8") as f:
        return json.load(f)


def carregar_cache_municipios(path_cache_municipios: str) -> dict:
    if not os.path.exists(path_cache_municipios):
        return {}
    with open(path_cache_municipios, "r", encoding="utf-8") as f:
        return json.load(f)


# -----------------------------------------------------------------------------


def limpeza_basica(regiao_bruta: Any) -> str:
    if not isinstance(regiao_bruta, str):
        return ""
    texto = regiao_bruta.strip()
    if texto.lower() in ["-", "unknown", "-1", "br - other", "br-other"]:
        return ""

    texto = texto.replace("Brazil: ", "")
    texto = texto.replace("State of ", "")
    texto = texto.replace("(state)", "")
    texto = texto.replace("Federal District", "Distrito Federal")
    texto = texto.replace("Greater ", "")
    texto = texto.replace(" metropolitan area", "")
    if texto.lower().endswith(" area"):
        texto = texto[: -len(" area")].strip()
    if texto.lower().endswith(" metro"):
        texto = texto[: -len(" metro")].strip()
    return texto.strip()


# -----------------------------------------------------------------------------


def obter_estado_de_regiao(
    regiao: Any, cache_municipios: dict, cache_estados: dict
) -> str:
    if not isinstance(regiao, str):
        return "Não identificado"
    r = regiao.strip().lower()
    if r in ["-", "unknown", "-1", "br - other", "br-other"]:
        return "Não identificado"
    # limpeza e padronização para lookup
    r = (
        r.replace("brazil: ", "")
        .replace("state of ", "")
        .replace("(state)", "")
        .replace("greater ", "")
        .replace("federal district", "distrito federal")
        .replace(" metropolitan area", "")
        .replace(", brazil metropolitan area", "")
        .replace(" brazil", "")
        .replace(" area", "")
        .replace(" metro", "")
        .replace(",", "")
        .strip()
    )
    chave = normalize_string(r)
    # tentativa com estados
    if chave in cache_estados:
        return cache_estados[chave]
    # tentativa com municípios
    if chave in cache_municipios:
        return cache_municipios[chave]
    return "Não identificado"


# -----------------------------------------------------------------------------
# Carrega caches apenas uma vez
CACHE_ESTADOS, CACHE_MUNICIPIOS = carregar_caches_padrao()


def normalize_region(regiao_bruta: Any) -> str:
    """
    Normaliza um valor de região livre:
      1) limpeza básica (remoção de prefixos/sufixos, acentos, etc)
      2) lookup via cache de estados/municípios
    """
    texto_limpo = limpeza_basica(regiao_bruta)
    return obter_estado_de_regiao(texto_limpo, CACHE_MUNICIPIOS, CACHE_ESTADOS)
