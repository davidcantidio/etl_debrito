# utils/__init__.py

# Importa funcionalidades de normalização e transformação
from .normalize import (
    normalize_campaign_name,
    normalizar_genero,
    normalizar_faixa_etaria,
    normalize_columns,
    normalize_parametrizacao_values,
    inferir_veiculo_meta_por_placement,
    atribuir_veiculo_por_criativo,
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
