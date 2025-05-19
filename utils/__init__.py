# utils/__init__.py

# Importa funcionalidades de normalização e transformação
from .normalize import (
    normalize_campaign_name,
    normalize_gender,
    normalize_age,
    normalize_columns,
    normalize_parametrizacao_values,
    infer_vehicle_meta_by_placement,
    assign_vehicle_by_creative,
)
from .datas import transformar_para_date, converter_data, generate_pinterest_dates
from .renomeacoes import renomear_colunas_origem_para_modelo, aplicar_substituicoes_objetivo
from .preview_links import (
    determine_meta_ad_preview_link,
    generate_linkedin_ad_preview_link_from_lookup,
    build_pinterest_preview_link,
    generate_pinterest_ad_preview_link
)
# Você pode incluir outras funções utilitárias conforme necessário
