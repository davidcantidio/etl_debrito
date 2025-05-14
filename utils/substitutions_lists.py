# substitutions_lists.py

"""
Contém todas as listas de substituições manuais para campos específicos,
centralizando exceções que devem ser aplicadas em diferentes ETLs.
"""

ID_CONTENT_REPLACEMENTS: dict[str, str] = {
    "influenciador_gabi_bailas": "dbt_sbrae_2025_catalisa0001",
    "dbt_sbrae_2025_cer_pan0006": "dbt_sbrae_2025_pan0006",
    "dbt_sbrae_2025_pan0008":"dbt_sbrae_2025_cer_pan0008",
    "dbt_sbrae_2025_cer_pan0012": "dbt_sbrae_2025_cer_pan0011",
    "dbt_sbrae_2025_emp_fem0199":"dbt_sbrae_2025_emp_fem0198",
    "dbt_sbrae_2025_cer_pan0202":"dbt_sbrae_2025_cer_pan0201",
    "dbt_sbrae_2025_cer_pan0146": "dbt_sbrae_2025_pan0146",
    "dbt_sbrae_2025_cer_pan0155" : "dbt_sbrae_2025_cer_pan0154",
}
"""Substituições específicas para o campo 'ID_Content'.

Chave: valor exato vindo de 'Content (utm)' (após strip e lowercase).  
Valor: string que deve substituir o original em 'ID_Content'.  
"""

CAMPAIGN_NAME_REPLACEMENTS: dict[str, str] = {
    "TOPVIEW-20250321-Q-20250301460526-2025031002130": "2025_3_EMPREENDEDORISMO FEMININO_ALC_COMERCIALIZAÇÃO_CPM",
}
"""Substituições específicas para o campo 'Campaign name'.

Chave: valor exato vindo da origem (após strip e lowercase).  
Valor: string que deve substituir o nome da campanha.  
"""

AD_GROUP_NAME_REPLACEMENTS: dict[str, str] = {
    "TOPVIEW-BR-20250321-Q-20250301460526-2025031076487": "2025_3_BR_ALC_CPM_BRASIL",
}
"""Substituições específicas para o campo 'Ad group name'.

Chave: valor exato vindo da origem (após strip e lowercase).  
Valor: string que deve substituir o nome do grupo de anúncio.  
"""


AD_NAME_REPLACEMENTS: dict[str, str] = {
    "Ad name2025-03-20 18:28:41": "2025_3_BR_VÍDEO_RAFA_ACAO_DBT_SBRAE_2025_EMP_FEM0075",
    "Mari Kruger": "2025_2_BR_VÍDEO_MARI_KRUGER_ACAO_DBT_SBRAE_2025_CATALISA0002",
    "2025_3_BR_VÍDEO_RESPOSTA_BRANDED_MISSION_1_ACAO_DBT_SBRAE_2025_EMP_FEM0062": "2025_3_BR_VÍDEO_RESPOSTA_BRANDED_MISSION_1_ACAO_DBT_SBRAE_2025_EMP_FEM0061"
}
"""Substituições específicas para o campo 'Ad name'.

Chave: valor exato vindo da origem (após strip e lowercase).  
Valor: string que deve substituir o nome do grupo de anúncio.  
"""
