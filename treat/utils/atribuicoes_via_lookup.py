from __future__ import annotations

import logging
import time   # ← ESQUECEU de importar
import pandas as pd

from treat.bi_param_utils import BIParamLookup
from treat.utils.normalize import extract_meta_platform_from_placement
from treat.utils.campanha_mapper import buscar_mapping

log = logging.getLogger(__name__)

PLATFORM_TO_VEICULO = {
    "tiktok":    "TikTok",
    "linkedin":  "LinkedIn",
    "pinterest": "Pinterest",
    "twitter":   "Twitter",
    "youtube":   "YouTube",
    "facebook":  "Facebook",   # normalize a chave para lowercase
    "instagram": "Instagram",
    # adicione outros conforme necessário
}


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────


class SourceLookup:
    """
    Carrega a aba 'SOURCE' **apenas uma vez** (cache + TTL),
    mas usando o SheetsFetcher em vez de gspread direto.
    """
    _df: pd.DataFrame | None = None
    _last_load: float = 0
    _TTL = 60 * 10  # 10 minutos

    @classmethod
    def _ensure_df(cls, fetcher) -> None:
        """
        Se não temos df ou TTL expirou, carregue via fetcher.get(["SOURCE"]).
        fetcher deve ser uma instância de SheetsFetcher.
        """
        agora = time.time()
        if cls._df is None or (agora - cls._last_load) > cls._TTL:
            try:
                # fetcher.get retorna um dict {nome_aba: DataFrame}
                df_source = fetcher.get(["SOURCE"])["SOURCE"]
            except Exception as exc:
                log.warning("Não foi possível ler aba SOURCE via fetcher: %s", exc)
                cls._df = pd.DataFrame()  # vazio como fallback
                cls._last_load = agora
                return

            # Normaliza a coluna “Descrição da Mídia” para lowercase
            if "Descrição da Mídia" in df_source.columns:
                df_source["Descrição da Mídia"] = (
                    df_source["Descrição da Mídia"]
                    .astype(str)
                    .str.strip()
                    .str.lower()
                )
                cls._df = df_source
            else:
                log.warning("Aba SOURCE não tem coluna 'Descrição da Mídia'.")
                cls._df = pd.DataFrame()

            cls._last_load = agora

    @classmethod
    def get_mapping(cls, fetcher) -> dict[str, str]:
        """
        Retorna { descrição_mídia_lower: ID_Veiculo } usando o DataFrame cacheado.
        fetcher: instância de SheetsFetcher que já está autenticada (builtins.fetcher).
        """
        cls._ensure_df(fetcher)
        if cls._df is None or cls._df.empty:
            return {}
        # Constrói o mapeamento final
        return dict(zip(
            cls._df["Descrição da Mídia"],
            cls._df["ID_Veiculo"]
        ))


# ─────────────────────────────────────────────────────────────────────────────
# 1) ID_VEICULO GENÉRICO
# ─────────────────────────────────────────────────────────────────────────────

def atribuir_id_veiculo_generico(
    df: pd.DataFrame,
    source_map: dict[str, str]
) -> pd.DataFrame:
    """
    Usa o dicionário source_map para preencher a coluna 'ID_Veiculo' a partir de 'Veiculo'.
    • source_map: {descrição_mídia_lower: ID_Veiculo}
    • Não faz nenhuma chamada ao Google Sheets.
    """
    # Garante que a coluna 'ID_Veiculo' exista
    if "ID_Veiculo" not in df.columns:
        df["ID_Veiculo"] = ""

    # Mapeia cada valor de 'Veiculo' (em lowercase) para o ID correspondente
    df["ID_Veiculo"] = (
        df["Veiculo"].astype(str)
        .str.strip()
        .str.lower()
        .map(source_map)
        .fillna("")
    )

    return df

# ─────────────────────────────────────────────────────────────────────────────
# 2) META – inferir Veiculo e ID_Veiculo a partir de placement
# ─────────────────────────────────────────────────────────────────────────────

def atribuir_veiculo_e_id_meta(
    df: pd.DataFrame,
    source_map: dict[str, str]
) -> pd.DataFrame:
    """
    Cria/atualiza as colunas 'Veiculo' e 'ID_Veiculo' usando a coluna 'placement' do Meta.
    • Não faz nenhuma chamada ao Google Sheets: espera receber `source_map` já carregado.
    • `source_map` deve ser um dicionário {descrição_mídia_lower: ID_Veiculo}.
    """
    log.debug(">>> atribuir_veiculo_e_id_meta")

    # Garante que as colunas existem
    for col in ("Veiculo", "ID_Veiculo"):
        if col not in df.columns:
            df[col] = ""

    # Só faz algo se houver a coluna 'placement'
    if "placement" in df.columns:
        # Preenche a coluna 'Veiculo' a partir de 'placement'
        df["Veiculo"] = df["placement"].apply(
            lambda p: extract_meta_platform_from_placement(p) if isinstance(p, str) else ""
        ).fillna("")

        # Converter para minúsculo e buscar no source_map
        df["ID_Veiculo"] = (
            df["Veiculo"].astype(str)
            .str.strip()
            .str.lower()
            .map(source_map)
            .fillna("")
        )

    return df

# ─────────────────────────────────────────────────────────────────────────────
# 3) LINKEDIN / TWITTER – veiculo via Criativo
# ─────────────────────────────────────────────────────────────────────────────

def atribuir_veiculo_por_criativo(
    df: pd.DataFrame,
    bi_lookup: BIParamLookup
) -> pd.DataFrame:
    """
    Lookup de 'Ad name' → 'Veiculo' usando o mapeamento cached em BIParamLookup.
    • bi_lookup: instância de BIParamLookup já inicializada no pipeline.
    • Não faz nova leitura da planilha; usa bi_lookup._map_columns para obter criativo→veículos.
    """
    log = logging.getLogger(__name__)
    log.debug(">>> atribuir_veiculo_por_criativo (usando BIParamLookup)")

    # Tenta obter o dicionário raw_map = {CRIATIVO_EMMAIÚSCULO: ("VeiculoNome",)}
    try:
        raw_map = bi_lookup._map_columns(
            key_kw="criativo",
            val_kws=["veículos"],
            upper_keys=True
        )
    except KeyError:
        log.warning("Colunas 'CRIATIVO' ou 'VEÍCULOS' não encontradas em BI_PARAMETRIZAÇÃO.")
        raw_map = {}

    # Converte raw_map para um dict simples {criativo_lower: veiculo_nome}
    mapping: dict[str, str] = {
        k.strip().lower(): vals[0].strip()
        for k, vals in raw_map.items()
        if vals and vals[0] is not None
    }

    # Garante que haja coluna 'Ad name'
    if "Ad name" not in df.columns:
        df["Veiculo"] = ""
        return df

    # Preenche 'Veiculo' mapeando 'Ad name' (lower/strip) → mapping
    df["Veiculo"] = (
        df["Ad name"].astype(str).str.strip().str.lower()
        .map(mapping)
        .fillna("")
    )

    return df


# ─────────────────────────────────────────────────────────────────────────────
# 4) PINTEREST – replica campaign_name em colunas destino
# ─────────────────────────────────────────────────────────────────────────────

def preencher_campos_com_campanha(df: pd.DataFrame) -> pd.DataFrame:
    """
    Copia 'Campaign name' para campos de campanha no Pinterest.
    """
    log.debug(">>> preencher_campos_com_campanha (Pinterest)")
    if "Campaign name" not in df.columns:
        log.warning(
            "Coluna 'Campaign name' ausente; não será possível preencher campos."
        )
        df["Campanha"] = ""
        df["Nome_do_Anuncio"] = ""
        df["Nome_do_Conjunto_de_Anuncio"] = ""
        return df

    df["Campanha"] = df["Campaign name"]
    df["Nome_do_Anuncio"] = df["Campaign name"]
    df["Nome_do_Conjunto_de_Anuncio"] = df["Campaign name"]
    return df


# ─────────────────────────────────────────────────────────────────────────────
# 5) Campanha – parametrização genérica
# ─────────────────────────────────────────────────────────────────────────────

def aplicar_parametrizacao_campanha(
    df: pd.DataFrame,
    mapping_campanha: dict[str, str],
    mapping_sigla: dict[str, str],
) -> pd.DataFrame:
    """
    Preenche 'Campanha' e 'ID_Campanha' a partir de 'Campaign_name'.
    """
    log.debug(">>> aplicar_parametrizacao_campanha")
    if "Campaign_name" not in df.columns:
        df["Campanha"] = ""
        df["ID_Campanha"] = ""
        return df

    df["Campanha"] = df["Campaign_name"].apply(
        lambda x: buscar_mapping(mapping_campanha, x) or x
    )
    df["ID_Campanha"] = df["Campaign_name"].apply(
        lambda x: buscar_mapping(mapping_sigla, x)
    )
    return df


def atribuir_veiculo_por_prefixo(
    df: pd.DataFrame,
    prefixo: str,
    source_map: dict[str, str]
) -> pd.DataFrame:
    """
    Define 'Veiculo' com base em um prefixo fixo e preenche 'ID_Veiculo' usando source_map.
    • prefixo: nome da plataforma (e.g., "meta", "tiktok", etc.).
    • source_map: dicionário {descrição_mídia_lower: ID_Veiculo}.
    """
    # Determina nome do veículo a partir do prefixo
    veic = PLATFORM_TO_VEICULO.get(prefixo.lower(), prefixo.capitalize())

    # Garante que a coluna 'Veiculo' exista
    if "Veiculo" not in df.columns:
        df["Veiculo"] = ""

    # Preenche 'Veiculo' somente onde estiver vazio
    df["Veiculo"] = df["Veiculo"].where(df["Veiculo"].str.strip() != "", veic)

    # Preenche 'ID_Veiculo' usando source_map
    df["ID_Veiculo"] = (
        df["Veiculo"].astype(str)
        .str.strip()
        .str.lower()
        .map(source_map)
        .fillna("")
    )

    return df

# ─────────────────────────────────────────────────────────────────────────────
# ALIASES PARA COMPATIBILIDADE (nomes antigos)
# ─────────────────────────────────────────────────────────────────────────────

# alias para quem importava 'atribuir_veiculo_meta'
atribuir_veiculo_meta = atribuir_veiculo_e_id_meta
