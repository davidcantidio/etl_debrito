# substitutions_lists.py

"""
Contém todas as listas de substituições manuais para campos específicos,
centralizando exceções que devem ser aplicadas em diferentes ETLs.
"""

ID_CONTENT_REPLACEMENTS: dict[str, str] = {
    "influenciador_gabi_bailas": "dbt_sbrae_2025_catalisa0001",
    "dbt_sbrae_2025_emp_fem0204" : "dbt_sbrae_2025_emp_fem0203",
}

"""Substituições específicas para o campo 'ID_Content'.

Chave: valor exato vindo de 'Content (utm)' (após strip e lowercase).  
Valor: string que deve substituir o original em 'ID_Content'.  
"""

CAMPAIGN_NAME_REPLACEMENTS: dict[str, str] = {
    "topview-20250321-q-20250301460526-2025031002130": "2025_3_EMPREENDEDORISMO FEMININO_ALC_COMERCIALIZAÇÃO_CPM",
    "2025_3_INOVA CERRADO E PANTANAL_ALC__CPM": "2025_3_INOVA CERRADO E PANTANAL_ALC_COMERCIALIZAÇÃO_CPM",
}
"""Substituições específicas para o campo 'Campaign name'.

Chave: valor exato vindo da origem (após strip e lowercase).  
Valor: string que deve substituir o nome da campanha.  
"""

AD_GROUP_NAME_REPLACEMENTS: dict[str, str] = {
    "topview-br-20250321-q-20250301460526-2025031076487": "2025_3_EMPREENDEDORISMO FEMININO_ALC_COMERCIALIZAÇÃO_CPM",
}
"""Substituições específicas para o campo 'Ad group name'.

Chave: valor exato vindo da origem (após strip e lowercase).  
Valor: string que deve substituir o nome do grupo de anúncio.  
"""


AD_NAME_REPLACEMENTS: dict[str, str] = {
    "Ad name2025-03-20 18:28:41": "2025_3_BR_VÍDEO_RESPOSTA_BRANDED_MISSION_1_ACAO_DBT_SBRAE_2025_EMP_FEM0062",
    "Mari Kruger": "2025_2_BR_VÍDEO_MARI_KRUGER_ACAO_DBT_SBRAE_2025_CATALISA0002",
}
"""Substituições específicas para o campo 'Ad name'.

Chave: valor exato vindo da origem (após strip e lowercase).  
Valor: string que deve substituir o nome do grupo de anúncio.  
"""
