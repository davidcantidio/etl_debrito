# treat/utils/normalize.py
"""
Funções utilitárias de normalização que rodam 100 % em memória.
Nenhuma rotina deste módulo faz chamadas à API do Google Sheets.
"""

from __future__ import annotations

import datetime as _dt
import re
import unicodedata
from typing import Optional, Any

import pandas as pd

# ----------------------------------------------------------------------
# Datas helpers
# ----------------------------------------------------------------------

_TIME_STAMP_RE = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")


def _try_manual_parse(val: str) -> Optional[_dt.date]:
    """Tenta converter 'YYYY-MM-DD HH:MM:SS' em date ou devolve None."""
    if not isinstance(val, str):
        return None
    txt = val.strip()
    if not _TIME_STAMP_RE.fullmatch(txt):
        return None
    try:
        return _dt.datetime.strptime(txt, "%Y-%m-%d %H:%M:%S").date()
    except ValueError:
        return None


def converter_data(df: pd.DataFrame, coluna: str) -> pd.DataFrame:
    """Converte uma coluna datetime/string para YYYY-MM-DD (date)."""
    if coluna in df.columns:
        parsed = pd.to_datetime(df[coluna], errors="coerce")
        df[coluna] = parsed.dt.date.where(~parsed.isna(), "")
    return df


# ----------------------------------------------------------------------
# Normalização de textos
# ----------------------------------------------------------------------


def normalize_nome(val: Any) -> str:
    """
    Padroniza rótulos de dimensão (idade, gênero, região, placement...).

    - Converte para string
    - Remove acentos
    - Converte para minúsculas
    - Tira espaços extras, quebras de linha, non-breaking spaces
    - Substitui separadores estranhos (–, —, /) por hífen simples
    - Remove parênteses, colchetes e textos auxiliares
    """
    if val is None:
        return ""

    # 1) força str e strip
    txt = str(val).strip()

    # 2) normaliza unicode e remove acentos
    txt = unicodedata.normalize("NFKD", txt)
    txt = "".join(ch for ch in txt if not unicodedata.combining(ch))

    # 3) converte para lower case
    txt = txt.lower()

    # 4) substitui separadores por "-"
    txt = re.sub(r"[–—/]", "-", txt)

    # 5) remove parênteses e conteúdo opcional
    txt = re.sub(r"\s*\(.*?\)", "", txt)

    # 6) colapsa espaços múltiplos
    txt = re.sub(r"\s+", " ", txt).strip()

    return txt

def _strip_lower_noaccent(s: str) -> str:
    """strip → lower → remove acentos."""
    cleaned = s.strip().lower()
    norm = unicodedata.normalize("NFKD", cleaned)
    return "".join(c for c in norm if not unicodedata.combining(c))


def normalize_campaign_name(value):
    """Normaliza nome de campanha: strip + upper (somente se for string)."""
    return value.strip().upper() if isinstance(value, str) else value


def normalize_campaign_series(series: pd.Series) -> pd.Series:
    """Normaliza Series de nomes de campanha (strip/lower/rem. acentos)."""
    return series.astype(str).map(_strip_lower_noaccent)


def normalize_columns(columns: pd.Index) -> pd.Index:
    """Normaliza nomes de colunas → strip/lower/rem. acentos."""
    return (
        columns.astype(str)
        .str.strip()
        .str.replace("\n", " ", regex=False)
        .map(_strip_lower_noaccent)
    )


def normalize_parametrizacao_values(
    df: pd.DataFrame, cols: list[str] | None = None
) -> pd.DataFrame:
    """Aplica `_strip_lower_noaccent` aos valores string das colunas escolhidas."""
    out = df.copy()
    targets = cols or out.columns.tolist()
    for col in targets:
        if col in out.columns:
            out[col] = out[col].apply(
                lambda v: _strip_lower_noaccent(v) if isinstance(v, str) else ""
            )
    return out


# ----------------------------------------------------------------------
# Veículo / Placement
# ----------------------------------------------------------------------


def extract_meta_platform_from_placement(placement: str) -> str:
    """
    Infere o veículo (“Facebook” ou “Instagram”) a partir do texto de placement.
    """
    if not isinstance(placement, str):
        return "Facebook"

    text = placement.lower()
    tokens = re.findall(r"[a-z]+", text)

    if any(t in {"instagram", "ig"} for t in tokens):
        return "Instagram"
    if any(t in {"facebook", "fb", "audience", "audiencenetwork"} for t in tokens):
        return "Facebook"
    return "Facebook"  # default


# ----------------------------------------------------------------------
# Demográfico
# ----------------------------------------------------------------------


def normalize_gender(value) -> str:
    """Mapeia valores de gênero para ‘Homem’ / ‘Mulher’ / ‘Não classificado’."""
    if not isinstance(value, str):
        return "Não classificado"

    val = value.strip().lower()
    if val in {"female", "feminino"}:
        return "Mulher"
    if val in {"male", "masculino"}:
        return "Homem"
    if val in {"", "unknown", "others", "none", "-"}:
        return "Não classificado"
    return val.capitalize()


def normalize_age(valor) -> str:
    """Normaliza faixas etárias para padrões do dashboard."""
    if not isinstance(valor, str):
        return "Não classificado"
    v = valor.strip().lower()

    pin_map = {
        "0-17": "Não classificado",
        "18-24": "18-24",
        "25-34": "25-34",
        "35-49": "35-44",
        "45-49": "45-54",
        "50-64": "55+",
        "65+": "55+",
    }
    if v in pin_map:
        return pin_map[v]
    if v in {"", "none", "unknown", "others"}:
        return "Não classificado"
    if v in {"55-64", "65+"}:
        return "55+"
    return v


# ----------------------------------------------------------------------
# Números
# ----------------------------------------------------------------------


def _clean_numeric_series(s: pd.Series) -> pd.Series:
    """Converte strings br/pt 1.234,56 → 1234.56 (float)."""
    s = s.astype(str)
    s = s.str.replace("\u00a0", "", regex=False)  # NB-space
    s = s.str.replace(r"\.(?=\d{3}(?:\.|,))", "", regex=True)  # separ. milhar
    s = s.str.replace(",", ".", regex=False)  # decimal
    return pd.to_numeric(s, errors="coerce").fillna(0)


def convert_numeric_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Aplica `_clean_numeric_series` em lista de colunas (somando duplicadas)."""
    out = df.copy()
    for col in columns:
        if col not in out.columns:
            continue
        obj = out[col]

        # Coluna única
        if isinstance(obj, pd.Series):
            out[col] = _clean_numeric_series(obj)
            continue

        # Coluna duplicada (DataFrame de colunas repetidas)
        cleaned_parts = [
            _clean_numeric_series(obj.iloc[:, i]) for i in range(obj.shape[1])
        ]
        out[col] = pd.concat(cleaned_parts, axis=1).sum(axis=1)
        out = out.loc[:, ~out.columns.duplicated(keep="first")]
    return out


def format_columns_to_comma_decimal(
    df: pd.DataFrame, cols: list[str], decimals: int = 2
) -> pd.DataFrame:
    """Formata floats para string BR (vírgula decimal, sem milhar)."""
    for col in cols:
        if col not in df.columns:
            continue
        df[col] = df[col].apply(
            lambda x: f"{x:.{decimals}f}".replace(".", ",") if pd.notna(x) else ""
        )
    return df


def normalize_vehicle(v: str) -> str:
    if not v or str(v).strip() == "":
        return ""
    raw = str(v).strip().lower()

    MAP = {
        "facebook":  "FB",
        "instagram": "IG",
        "insta":     "IG",
        "ig":        "IG",
        "youtube":   "YouTube",
        "yt":        "YouTube",
        "tiktok":    "TikTok",
        # adicione outros se necessário
    }

    return MAP.get(raw, raw.title())
