# substitutions_lists.py

"""
Contém todas as listas de substituições manuais para campos específicos,
centralizando exceções que devem ser aplicadas em diferentes ETLs.
"""

ID_CONTENT_REPLACEMENTS: dict[str, str] = {
    "influenciador_gabi_bailas": "dbt_sbrae_2025_catalisa0001",
}
"""Substituições específicas para o campo 'ID_Content'.

Chave: valor exato vindo de 'Content (utm)' (após strip e lowercase).  
Valor: string que deve substituir o original em 'ID_Content'.  
"""

CAMPAIGN_NAME_REPLACEMENTS: dict[str, str] = {
    "topview-20250321-q-20250301460526-2025031002130": "2025_3_EMPREENDEDORISMO FEMININO_ALC_COMERCIALIZAÇÃO_CPM",
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
