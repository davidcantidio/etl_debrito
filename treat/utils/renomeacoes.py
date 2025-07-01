import logging
from typing import Dict
import pandas as pd

# Mapeamento de objetivos de campanha para nomes padronizados
SUBSTITUICOES_OBJETIVO = {
    "AWARENESS": "Alcance",
    "COMMUNITY_INTERACTION": "Engajamento",
    "APP_PROMOTION": "Promoção do app",
    "REACH": "Alcance",
    "VIDEO_VIEWS": "Visualização",
    "TRAFFIC": "Tráfego",
    "CONVERSIONS": "Conversões",
    "LEAD_GENERATION": "Geração de Leads",
    "OUTCOME_AWARENESS": "Alcance",
    "OUTCOME_TRAFFIC": "Tráfego",
    "OUTCOME_ENGAGEMENT": "Engajamento",
    "OUTCOME_SALES": "Vendas",
    "OUTCOME_APP+PROMOTION": "Promoção do app",
    "BRAND_AWARENESS": "Alcance",
    "WEBSITE_VISITS": "Tráfego",
    "ENGAGEMENT": "Engajamento",
    "WEBSITE_CONVERSIONS": "Conversões",
    "CONSIDERATION": "Tráfego",
    "CATALOG_SALES": "Vendas",
    "WEBSITE_VISIT": "Tráfego",
}


def aplicar_substituicoes_objetivo(df):
    """
    Substitui valores da coluna 'Objetivo' com base no dicionário SUBSTITUICOES_OBJETIVO.
    """
    if "objective" in df.columns:
        for old, new in SUBSTITUICOES_OBJETIVO.items():
            df.loc[df["objective"] == old, "objective"] = new
        logging.debug(
            "[aplicar_substituicoes_objetivo] Substituições aplicadas na coluna 'Objetivo'"
        )
    else:
        logging.warning(
            "[aplicar_substituicoes_objetivo] Coluna 'Objetivo' não encontrada no DataFrame."
        )
    return df


def renomear_colunas_origem_para_modelo(
    df: pd.DataFrame, mapping: Dict[str, str]
) -> pd.DataFrame:
    """
    Renomeia as colunas de 'df' conforme o dicionário 'mapping'
    (chave = nome de coluna atual; valor = nome de coluna no modelo).
    Retorna um novo DataFrame com as colunas renomeadas.
    """
    return df.rename(columns=mapping)


renomeacao_geral = {
    "Date": "date",
    "Account name": "account_name",
    "Campaign name": "campaign_name",
    "Ad group name": "ad_group_name",
    "Ad name": "ad_name",
    "Content (utm)": "utm_content",
    "Ad ID": "ad_id",
    "Campaign ID": "campaign_id",
    "Start": "start",
    "End": "end",
    "Campaign objective type": "objective",
    "Preview_Link_IG": "preview_link_ig",
    "Preview_Link_FB": "preview_link_fb",
    "Placement": "placement",
    "Impressions": "impressions",
    "Cost": "cost",
    "Link clicks": "link_clicks",
    "Video watches at 25%": "video_watches_25",
    "Video watches at 50%": "video_watches_50",
    "Video watches at 75%": "video_watches_75",
    "Video watches at 100%": "video_watched_100",
    "Post reactions": "post_reactions",
    "Post shares": "post_shares",
    "Post comments": "post_comments",
    "Video_Play": "video_play",
    "Preview_Link_FB": "URL_do_Anuncio",
    "Preview_Link_IG": "URL_do_Anuncio",
    "URL_do_Anuncio": "URL_do_Anuncio",
}

renomeacao_metaIdade = {
    "Date": "date",
    "Age": "age",
    "Account name": "account_name",
    "Campaign name": "campaign_name",
    "Campaign ID": "campaign_id",
    "Ad name": "ad_name",
    "Ad set name": "ad_group_name",
    "Campaign objective type": "objective",
    "Placement": "placement",
    "Ad ID": "ad_id",
    "Impressions": "impressions",
    "Cost": "cost",
    "Video watches at 100%": "video_watched_100",
    "Link clicks": "link_clicks",
}


renomeacao_metaGenero = {
    "Date": "date",
    "Gender": "gender",
    "Account name": "account_name",
    "Campaign name": "campaign_name",
    "Campaign ID": "campaign_id",
    "Ad name": "ad_name",
    "Ad set name": "ad_group_name",
    "Campaign objective type": "objective",
    "Placement": "placement",
    "Ad ID": "ad_id",
    "Impressions": "impressions",
    "Cost": "cost",
    "Video watches at 100%": "video_watched_100",
    "Link clicks": "link_clicks",
}

renomeacao_metaAlcance = {
    "Date": "date",
    "Account name": "account_name",
    "Campaign name": "campaign_name",
    "Placement": "placement",
    "Ad group name": "ad_group_name",
    "Ad name": "ad_name",
    "Campaign objective type": "objective",
    "Reach": "reach",
    "Impressions": "impressions",
}

# bi_parametrizacao_column_map.py
BI_PARAM_COLUMN_MAP = {
    # ===== META-DEFINIÇÃO DA CAMPANHA =====
    "NOME CAMPANHA": "campaign_name",
    "PLATAFORMA/ REDE/PORTAL": "platform_network_portal",
    "VEÍCULOS": "vehicle",
    "CATEGORIA obrigatório": "category",
    "FORMATO obrigatório": "format",
    "REGIÃO obrigatório": "region",
    "REMARKETING/AÇÃO obrigatório": "remarketing_or_action",
    "START": "start",
    "END": "end",
    # ===== CRIATIVO / PRÉ-VISUALIZAÇÃO =====
    "CRIATIVO/INFLUENCIADOR - Obrigatório": "ad_name_raw",
    "AD_PREVIEW": "ad_preview_link",
    "URL DE DESTINO (Livre)": "landing_page_url",
    # ===== RESPONSÁVEIS & OBJETIVOS =====
    "RESPONSÁVEL Obrigatório": "responsible",
    "OBJETIVO obrigatório": "objective",
    "TIPO DE COMPRA obrigatório": "purchase_type",
    "SEGMENTAÇÃO obrigatória! Escreva de forma resumida": "segmentation",
    # ===== RAW FIELDS vindos da mídia =====
    "ID": "utm_content_raw",
    "utm_source_raw": "utm_source_raw",
    "Remarketing e Ação": "remarketing_or_action_raw",
    "Agência": "agency_raw",
    "SIGLA": "utm_campaign_raw",
    "url": "base_urn",
    "Editoria": "type_comunication",
    # ===== UTM FINAIS =====
    "utm_source": "utm_source",
    "utm_medium": "utm_medium",
    "utm_medium2": "utm_medium2",
    "formato": "formato",
    "utm_campaign": "utm_campaign",
    "Objetivo": "objective",  # duplicado na fonte → mesmo canônico
    "Região": "region",
    "utm_term": "utm_term",
    "utm_content": "utm_content",
    "url_final_raw": "url_final_raw",
    # ===== TAXONOMY (padrão interno) =====
    "CAMPANHA": "taxonomy_campaign_name",
    "CONJUNTO DE ANÚNCIO (não aplicável para Linkedin)": "taxonomy_ad_group_name",
    "CRIATIVO": "taxonomy_ad_name",
    # ===== PARAMETRIZAÇÃO DE LINKS =====
    "GERAR LINK?": "generate_parametrized_link",
    "URL PARAMETRIZADA": "parametrized_link",
    # ===== TAXONOMY PARA REDES SOCIAIS =====
    "CampanhaRedes": "taxonomy_campaign_name_social",
    "ConjuntoAnunciosRedes": "taxonomy_ad_group_name_social",
    "CriativoRedes": "taxonomy_ad_name_social",
}
