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
