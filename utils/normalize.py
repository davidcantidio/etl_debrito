# utils/normalize.py

import unicodedata
import pandas as pd
import logging


def normalize_campaign_name(value):
    """
    Normaliza o nome de campanha:

    - Se for string, faz strip() e converte para uppercase.
    - Se não for string, retorna o valor inalterado.

    Parâmetros:
        value: qualquer objeto; se for string, será normalizado.

    Retorna:
        str | qualquer: nome de campanha normalizado em uppercase, ou
                        o valor original se não for string.
    """
    logger = logging.getLogger(__name__)
    logger.debug("normalize_campaign_name: recebendo %r", value)

    if not isinstance(value, str):
        logger.debug(
            "normalize_campaign_name: valor não é string, retornando original %r",
            value,
        )
        return value

    normalized = value.strip().upper()
    logger.debug(
        "normalize_campaign_name: normalizado de %r para %r",
        value,
        normalized,
    )
    return normalized


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
    Converte colunas com números no formato brasileiro ('.' milhar + ',' decimal)
    para tipo numérico, preenchendo com zero os valores inválidos.
    """
    for col in colunas:
        if col in df.columns:
            s = df[col].astype(str)
            # 1) limpa espaços não‑quebráveis
            s = s.str.replace("\u00a0", "")
            # 2) remove pontos de milhar
            s = s.str.replace(r"\.(?=\d{3}(?:\.|,))", "", regex=True)
            # 3) torna vírgula em ponto decimal
            s = s.str.replace(",", ".")
            # 4) converte
            df[col] = pd.to_numeric(s, errors="coerce").fillna(0)
    return df


def format_columns_to_comma_decimal(df, cols, decimals=2):
    """
    Converte floats em strings BR sem separador de milhar:
    12345.67 → '12345,67'
    """
    for col in cols:
        if col in df.columns:
            def fmt(x):
                try:
                    # formata com ponto decimal e casas fixas
                    s = f"{x:.{decimals}f}"   # ex: "12345.67"
                    return s.replace(".", ",")  # ex: "12345,67"
                except:
                    return ""
            df[col] = df[col].apply(fmt)
    return df


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


def apply_arbitrary_id_content_replacements(
    df: pd.DataFrame,
    mapping_excecoes: dict[str, str]
) -> pd.DataFrame:
    """
    Aplica substituições manuais para o campo 'ID_Content' com base em exceções.

    Para cada linha, normaliza o valor atual de 'ID_Content' (strip + lowercase)
    e, se existir em `mapping_excecoes`, substitui pelo valor mapeado.
    Caso contrário, mantém o original.

    Parâmetros:
        df (pd.DataFrame): DataFrame de entrada contendo a coluna 'ID_Content'.
        mapping_excecoes (dict[str, str]): Dicionário de exceções a aplicar.
            - Chave: valor original normalizado de 'ID_Content'.
            - Valor: nova string a escrever em 'ID_Content'.

    Retorna:
        pd.DataFrame: cópia do DataFrame com a coluna 'ID_Content' atualizada
                      segundo as exceções definidas.
    """
    logger = logging.getLogger(__name__)

    if "ID_Content" not in df.columns:
        logger.debug("Coluna 'ID_Content' não encontrada. Nenhuma substituição aplicada.")
        return df

    df = df.copy()
    orig_series = df["ID_Content"].astype(str)
    norm_series = orig_series.str.strip().str.lower()
    
    logger.debug("apply_arbitrary_id_content_replacements: carregadas %d regras", len(mapping_excecoes))
    # Aplica o mapeamento
    df["ID_Content"] = norm_series.map(lambda x: mapping_excecoes.get(x, x))
    
    # Conta quantas efetivamente mudaram (comparing normalized originals)
    replaced = int((norm_series != df["ID_Content"].astype(str).str.strip().str.lower()).sum())
    logger.debug("Total de valores substituídos em 'ID_Content': %d", replaced)

    return df
