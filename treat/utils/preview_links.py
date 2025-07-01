import logging

import numpy as np
import pandas as pd


def determine_meta_ad_preview_link(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preenche/atualiza 'URL_do_Anuncio' para relatórios Meta:

    Regras de prioridade por linha
    ------------------------------
    1) Se 'URL_do_Anuncio' já tem valor não-vazio → preserva.
    2) Caso contrário:
       • Se Veiculo == "Facebook"   → usa preview_link_fb; se vazio, usa preview_link_ig.
       • Se Veiculo == "Instagram" → usa preview_link_ig; se vazio, usa preview_link_fb.
       • Outro / sem Veiculo       → usa preview_link_ig; se vazio, usa preview_link_fb.

    Observações
    -----------
    • Colunas de preview podem vir com espaços ou capitalização diferente:
      'Preview Link IG', 'preview_link_ig ', etc.  São normalizadas via strip/lower.
    • Se a coluna desejada não existir, pula para o fallback seguinte.
    """

    log = logging.getLogger(__name__)

    # ---------------- normalização de cabeçalhos ---------------- #
    colmap = {c.lower().strip(): c for c in df.columns}
    ig_col = colmap.get("preview_link_ig")
    fb_col = colmap.get("preview_link_fb")
    veic_col = colmap.get("veiculo")  # pode não existir

    if ig_col is None and fb_col is None:
        log.warning(
            "[determine_meta_ad_preview_link] Nenhuma coluna preview_* encontrada."
        )
        return df

    # ---------------- garante coluna destino -------------------- #
    if "URL_do_Anuncio" not in df.columns:
        df["URL_do_Anuncio"] = ""
    else:
        # normaliza valores já existentes (NaN → "")
        df["URL_do_Anuncio"] = (
            df["URL_do_Anuncio"]
            .astype(str)
            .where(~df["URL_do_Anuncio"].isin([np.nan, "nan", "NaN"]), "")
            .str.strip()
        )

    # ---------------- função de escolha por linha --------------- #
    def _choose(row: pd.Series) -> str:
        current = row["URL_do_Anuncio"].strip()
        if current:
            return current

        veic = str(row.get(veic_col, "")).strip().lower() if veic_col else ""
        ig = str(row.get(ig_col, "")).strip() if ig_col else ""
        fb = str(row.get(fb_col, "")).strip() if fb_col else ""

        if veic == "facebook":
            return fb or ig
        elif veic == "instagram":
            return ig or fb
        else:  # genérico / sem veículo
            return ig or fb

    # aplica vetorizado com Series.map (mais rápido que apply linha-a-linha)
    df["URL_do_Anuncio"] = df.apply(_choose, axis=1)

    return df


def generate_linkedin_ad_preview_link_from_lookup(
    df_parametrizacao: pd.DataFrame,
) -> dict:
    """
    Gera um dicionário {utm_content: preview} para ser usado no preenchimento de preview do LinkedIn.
    """
    COL_UTM = "utm_content"
    COL_PREVIEW = "ad_preview_link"

    if (
        COL_UTM not in df_parametrizacao.columns
        or COL_PREVIEW not in df_parametrizacao.columns
    ):
        logging.warning(
            "Colunas 'utm_content' ou 'preview' não encontradas em BI_PARAMETRIZAÇÃO."
        )
        return {}

    mapping = df_parametrizacao[[COL_UTM, COL_PREVIEW]].dropna()
    mapping = mapping.astype(str).drop_duplicates(subset=[COL_UTM])
    preview_dict = dict(zip(mapping[COL_UTM], mapping[COL_PREVIEW]))

    logging.debug("Exemplo de mapeamentos de preview gerados para LinkedIn:")
    for k, v in list(preview_dict.items())[:5]:
        logging.debug(f"{k} -> {v}")

    return preview_dict


def build_pinterest_preview_link(id_pin: str) -> str:
    """
    Constrói a URL de preview pública de um Pin do Pinterest a partir do seu ID.

    Args:
        id_pin (str or int): ID do Pin (ex: "1234567890")

    Returns:
        str: URL completa (ex: "https://www.pinterest.com/pin/1234567890")
    """
    if not id_pin or str(id_pin).strip() == "":
        return ""
    return f"https://www.pinterest.com/pin/{str(id_pin).strip()}"


def generate_pinterest_ad_preview_link(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preenche a coluna 'URL_do_Anuncio' com base na coluna 'Preview Link'
    aplicando a função build_pinterest_preview_link().
    """
    match_col = [col for col in df.columns if col.strip().lower() == "preview link"]
    if match_col:
        df["URL_do_Anuncio"] = df[match_col[0]].apply(build_pinterest_preview_link)
    else:
        df["URL_do_Anuncio"] = ""
    return df


def generate_tiktok_ad_preview_link(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preenche 'URL_do_Anuncio' em relatórios TikTok copiando diretamente
    o valor de 'ad_preview_link', sem sobrescrever valores já existentes.
    """
    # garante a coluna de destino
    if "URL_do_Anuncio" not in df.columns:
        df["URL_do_Anuncio"] = ""

    # aplica vetoricamente: se URL_do_Anuncio vazio, usa ad_preview_link
    if "ad_preview_link" in df.columns:
        dest = df["URL_do_Anuncio"].astype(str).fillna("")
        src = df["ad_preview_link"].astype(str).fillna("").str.strip()
        df["URL_do_Anuncio"] = dest.where(dest.str.strip() != "", src)
    return df
