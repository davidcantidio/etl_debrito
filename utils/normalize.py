# utils/normalize.py

import unicodedata
import pandas as pd
import logging

def normalize_campaign_name(value):
    if not isinstance(value, str):
        return value
    return value.strip().upper()

def normalize_nome(nome):
    if not isinstance(nome, str):
        return ""
    nome = nome.strip().lower()
    nome = unicodedata.normalize("NFKD", nome)
    nome = ''.join([c for c in nome if not unicodedata.combining(c)])
    return nome

def normalize_columns(columns: pd.Index) -> pd.Index:
    """
    Normaliza nomes de colunas:
    - Converte para string
    - Remove espaços extras
    - Substitui quebras de linha
    - Remove acentos
    - Converte para lowercase
    """
    return (
        columns.astype(str)
        .str.strip()
        .str.replace('\n', ' ', regex=False)
        .str.lower()
        .map(lambda x: unicodedata.normalize("NFKD", x))
        .map(lambda x: ''.join(c for c in x if not unicodedata.combining(c)))
    )

def normalize_parametrizacao_values(df: pd.DataFrame, cols: list[str] = None) -> pd.DataFrame:
    """
    Aplica normalização (lowercase, sem acento, strip) nas colunas indicadas.
    Se 'cols' for None, aplica em todas as colunas do DataFrame.
    """
    df = df.copy()

    def normaliza_valor(val):
        if not isinstance(val, str):
            return ""
        val = val.strip().lower()
        val = unicodedata.normalize("NFKD", val)
        return ''.join(c for c in val if not unicodedata.combining(c))

    target_cols = cols if cols else df.columns
    for col in target_cols:
        if col in df.columns:
            df[col] = df[col].apply(normaliza_valor)

    return df

def normalizar_faixa_etaria(valor) -> str:
    """
    Normaliza faixas etárias para uso em dashboards e relatórios:
    - Converte '55-64' e '65+' em '55+'
    - Converte None, '', 'unknown', 'others', 'none' para 'Não classificado'
    - Retorna o valor limpo nos demais casos
    """
    if not isinstance(valor, str):
        return "Não classificado"

    valor = valor.strip().lower()
    if valor in {"", "none", "unknown", "others"}:
        return "Não classificado"
    if valor in {"55-64", "65+"}:
        return "55+"
    return valor



def inferir_veiculo_meta_por_placement(df: pd.DataFrame) -> pd.DataFrame:
    """
    Define a coluna 'Veiculo' com base no conteúdo da coluna 'Placement'.
    Regras:
        - Se contiver 'facebook' ou 'audience': 'Facebook'
        - Se contiver 'instagram': 'Instagram'
        - Caso contrário: 'Meta'
    """
    def extrair_veiculo(placement):
        if not isinstance(placement, str):
            return "Meta"
        placement = placement.lower()
        if "facebook" in placement or "audience" in placement:
            return "Facebook"
        elif "instagram" in placement:
            return "Instagram"
        return "Facebook"

    df = df.copy()
    df['Veiculo'] = df.get('Placement', "").apply(extrair_veiculo)
    return df


def atribuir_veiculo_por_criativo(df: pd.DataFrame) -> pd.DataFrame:
    """
    Faz lookup do campo 'Ad name' (que representa o CRIATIVO) na aba BI_PARAMETRIZAÇÃO
    e preenche o campo 'Veiculo' com base na coluna 'VEÍCULOS'.
    """
    logging.debug(">>> In atribuir_veiculo_por_criativo (via Ad name → CRIATIVO)")

    from utils.google_sheets import carregar_aba_google_sheets, CREDS_PATH, SPREADSHEET_URL

    # Carrega a aba BI_PARAMETRIZAÇÃO (começando da linha 2)
    df_param = carregar_aba_google_sheets(CREDS_PATH, SPREADSHEET_URL, "BI_PARAMETRIZAÇÃO", header_row_index=1)

    # Normaliza as colunas para facilitar acesso
    df_param.columns = [col.strip().upper() for col in df_param.columns]

    if 'CRIATIVO' not in df_param.columns or 'VEÍCULOS' not in df_param.columns:
        logging.warning("Colunas 'CRIATIVO' ou 'VEÍCULOS' não encontradas em BI_PARAMETRIZAÇÃO.")
        df['Veiculo'] = ""
        return df

    # Cria dicionário de mapeamento: CRIATIVO → VEÍCULO
    mapping = dict(zip(df_param['CRIATIVO'].astype(str).str.strip(), df_param['VEÍCULOS'].astype(str).str.strip()))

    # Aplica mapeamento ao campo 'Ad name'
    df['Veiculo'] = df['Ad name'].astype(str).str.strip().map(mapping).fillna("")

    return df

def normalizar_genero(valor) -> str:
    """
    Normaliza valores de gênero:
    - 'female', 'feminino' → 'Mulher'
    - 'male', 'masculino' → 'Homem'
    - vazios, 'unknown', 'others', 'none', '-' → 'Não classificado'
    - outros valores → capitalizados
    """
    if not isinstance(valor, str):
        return "Não classificado"

    valor = valor.strip().lower()
    if valor in {"female", "feminino"}:
        return "Mulher"
    elif valor in {"male", "masculino"}:
        return "Homem"
    elif valor in {"", "unknown", "others", "none", "-"}:
        return "Não classificado"
    return valor.capitalize()



def converter_colunas_numericas(df, colunas):
    """
    Converte colunas especificadas para tipo numérico, preenchendo com zero os valores inválidos.
    """
    for col in colunas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    return df


# Dentro de utils/normalize.py

def extrair_veiculo(placement: str) -> str:
    if not isinstance(placement, str):
        return "Facebook"  # fallback padrão

    placement = placement.lower()
    if "facebook" in placement:
        return "Facebook"
    elif "instagram" in placement:
        return "Instagram"
    # fallback: só Facebook ou Instagram são possíveis neste contexto
    return "Facebook"

