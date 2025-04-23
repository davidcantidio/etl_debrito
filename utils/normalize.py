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


def normalize_name(nome):
    """
    Normaliza nomes:

    - Se não for string, retorna string vazia.
    - Remove espaços externos, converte para lowercase.
    - Remove acentos e caracteres unicode combinantes.

    Parâmetros:
        nome: qualquer objeto; se for string, será normalizado.

    Retorna:
        str: nome normalizado sem acentos, em lowercase, ou '' se não for string.
    """
    logger = logging.getLogger(__name__)
    logger.debug("normalize_nome: recebendo %r", nome)

    if not isinstance(nome, str):
        logger.debug(
            "normalize_nome: valor não é string, retornando vazio",
        )
        return ""

    cleaned = nome.strip().lower()
    logger.debug(
        "normalize_nome: após strip/lower %r",
        cleaned,
    )

    normalized = unicodedata.normalize("NFKD", cleaned)
    no_accents = ''.join([c for c in normalized if not unicodedata.combining(c)])
    logger.debug(
        "normalize_nome: sem acentos %r",
        no_accents,
    )
    return no_accents





def normalize_columns(columns: pd.Index) -> pd.Index:
    """
    Normaliza nomes de colunas:

    - Converte cada coluna para string.
    - Remove espaços externos e quebras de linha.
    - Remove acentos e caracteres combinantes.
    - Converte para lowercase.

    Parâmetros:
        columns (pd.Index): índice de colunas a ser normalizado.

    Retorna:
        pd.Index: índice de colunas normalizado.
    """
    logger = logging.getLogger(__name__)
    logger.debug("normalize_columns: recebendo colunas %r", list(columns))

    # Passo a passo da normalização
    result = (
        columns.astype(str)
        .str.strip()
        .str.replace('\n', ' ', regex=False)
        .map(lambda x: unicodedata.normalize("NFKD", x))
        .map(lambda x: ''.join(c for c in x if not unicodedata.combining(c)))
        .str.lower()
    )

    logger.debug("normalize_columns: colunas normalizadas %r", list(result))
    return result



def normalize_parametrizacao_values(df: pd.DataFrame, cols: list[str] = None) -> pd.DataFrame:
    """
    Normaliza valores de parametrização em um DataFrame.

    - Se 'cols' for None, aplica em todas as colunas do DataFrame.
    - Para cada valor string em cada coluna especificada:
        * faz strip() e converte para lowercase
        * remove acentos e caracteres combinantes
    - Se o valor não for string, substitui por string vazia.

    Parâmetros:
        df (pd.DataFrame): DataFrame a ser normalizado.
        cols (list[str], opcional): lista de colunas para normalização.
                                    Se None, todas as colunas serão processadas.

    Retorna:
        pd.DataFrame: cópia do DataFrame com valores de parametrização normalizados.
    """
    logger = logging.getLogger(__name__)
    logger.debug("normalize_parametrizacao_values: iniciando com cols=%r", cols)

    df_norm = df.copy()
    target_cols = cols if cols is not None else df_norm.columns.tolist()

    for col in target_cols:
        if col in df_norm.columns:
            logger.debug("normalize_parametrizacao_values: processando coluna '%s'", col)
            def _normalize_val(val):
                if not isinstance(val, str):
                    return ""
                cleaned = val.strip().lower()
                normalized = unicodedata.normalize("NFKD", cleaned)
                return ''.join(c for c in normalized if not unicodedata.combining(c))
            df_norm[col] = df_norm[col].apply(_normalize_val)
        else:
            logger.debug("normalize_parametrizacao_values: coluna '%s' não encontrada", col)

    logger.debug("normalize_parametrizacao_values: conclusão da normalização")
    return df_norm


def normalize_age(valor) -> str:
    """
    Normaliza faixas etárias para uso em dashboards e relatórios.

    - Converte '55-64' e '65+' em '55+'.
    - Converte valores vazios ou desconhecidos em 'Não classificado'.
    - Mantém outros valores após strip e lowercase.

    Parâmetros:
        valor: entrada bruta (qualquer tipo); será convertido para str e processado.

    Retorna:
        str: faixa etária normalizada ou 'Não classificado' se valor inválido.
    """
    logger = logging.getLogger(__name__)
    logger.debug("normalizar_faixa_etaria: recebendo valor %r", valor)

    if not isinstance(valor, str):
        logger.debug("normalizar_faixa_etaria: valor não é string, retornando 'Não classificado'")
        return "Não classificado"

    valor_norm = valor.strip().lower()
    logger.debug("normalizar_faixa_etaria: após strip/lower %r", valor_norm)

    if valor_norm in {"", "none", "unknown", "others"}:
        logger.debug("normalizar_faixa_etaria: valor em branco ou desconhecido, retornando 'Não classificado'")
        return "Não classificado"
    if valor_norm in {"55-64", "65+"}:
        logger.debug("normalizar_faixa_etaria: valor '%s' convertido para '55+'", valor_norm)
        return "55+"

    logger.debug("normalizar_faixa_etaria: valor final %r", valor_norm)
    return valor_norm



def infer_vehicle_meta_by_placement (df: pd.DataFrame) -> pd.DataFrame:
    """
    Infere a coluna 'Veiculo' com base no conteúdo de 'Placement'.

    Regras:
        - Se contiver 'facebook' ou 'audience': atribui 'Facebook'.
        - Se contiver 'instagram': atribui 'Instagram'.
        - Caso contrário: atribui 'Meta'.

    Parâmetros:
        df (pd.DataFrame): DataFrame de entrada com coluna 'Placement'.

    Retorna:
        pd.DataFrame: Cópia do DataFrame com coluna 'Veiculo' atualizada.
    """
    logger = logging.getLogger(__name__)
    logger.debug("inferir_veiculo_meta_por_placement: iniciando inferência")

    def _extrair_veiculo(placement):
        if not isinstance(placement, str):
            logger.debug("_extrair_veiculo: não-string %r, retornando 'Meta'", placement)
            return "Meta"
        lower = placement.lower()
        if "facebook" in lower or "audience" in lower:
            return "Facebook"
        if "instagram" in lower:
            return "Instagram"
        logger.debug("_extrair_veiculo: padrão 'Meta' para %r", lower)
        return "Meta"

    df_out = df.copy()
    df_out['Veiculo'] = df_out.get('Placement', "").apply(_extrair_veiculo)
    logger.debug("inferir_veiculo_meta_por_placement: veículos únicos %r", df_out['Veiculo'].unique().tolist())
    return df_out


def assign_vehicle_by_creative(df: pd.DataFrame) -> pd.DataFrame:
    """
    Assigns the 'Veiculo' (media vehicle) column based on the 'Ad name' by
    looking up the BI_PARAMETRIZAÇÃO sheet.

    Steps:
    1. Load the BI_PARAMETRIZAÇÃO sheet (starting at row 2).
    2. Normalize header names to uppercase stripped strings.
    3. Build a mapping from 'CRIATIVO' to 'VEÍCULOS'.
    4. Apply the mapping to the 'Ad name' field to populate 'Veiculo'.

    Parameters:
        df (pd.DataFrame): Input DataFrame containing an 'Ad name' column.

    Returns:
        pd.DataFrame: Copy of the DataFrame with the 'Veiculo' column updated.
    """
    logger = logging.getLogger(__name__)
    logger.debug("assign_vehicle_by_creative: starting assignment for %d rows", len(df))

    from utils.google_sheets import carregar_aba_google_sheets, CREDS_PATH, SPREADSHEET_URL

    # Load the BI_PARAMETRIZAÇÃO sheet (headers on second row)
    try:
        df_param = carregar_aba_google_sheets(
            CREDS_PATH,
            SPREADSHEET_URL,
            "BI_PARAMETRIZAÇÃO",
            header_row_index=1
        )
        logger.debug(
            "assign_vehicle_by_creative: loaded BI_PARAM with columns %r",
            df_param.columns.tolist(),
        )
    except Exception as e:
        logger.error(
            "assign_vehicle_by_creative: failed to load BI_PARAM sheet: %s",
            e,
        )
        df = df.copy()
        df['Veiculo'] = ""
        return df

    # Normalize column names
    df_param.columns = [col.strip().upper() for col in df_param.columns]
    logger.debug(
        "assign_vehicle_by_creative: normalized BI_PARAM headers to %r",
        df_param.columns.tolist(),
    )

    # Ensure required columns exist
    if 'CRIATIVO' not in df_param.columns or 'VEÍCULOS' not in df_param.columns:
        logger.warning(
            "assign_vehicle_by_creative: 'CRIATIVO' or 'VEÍCULOS' not found in BI_PARAM columns"
        )
        df = df.copy()
        df['Veiculo'] = ""
        return df

    # Build mapping from creative to vehicle
    mapping = {
        str(cre).strip(): str(veh).strip()
        for cre, veh in zip(df_param['CRIATIVO'], df_param['VEÍCULOS'])
    }
    logger.debug(
        "assign_vehicle_by_creative: built mapping for %d creatives",
        len(mapping),
    )

    # Apply mapping to 'Ad name'
    df_out = df.copy()
    df_out['Veiculo'] = (
        df_out['Ad name']
        .astype(str)
        .str.strip()
        .map(mapping)
        .fillna("")
    )
    logger.debug(
        "assign_vehicle_by_creative: completed assignment, vehicles present %r",
        df_out['Veiculo'].unique().tolist(),
    )
    return df_out

def normalize_gender(value) -> str:
    """
    Normalize gender values:
    - 'female', 'feminino' → 'Woman'
    - 'male', 'masculino' → 'Man'
    - empty, 'unknown', 'others', 'none', '-' → 'Not classified'
    - other values → capitalized
    """
    logging.debug(">>> In normalize_gender; raw input: %r", value)
    if not isinstance(value, str):
        logging.debug("Value is not a string; returning 'Not classified'")
        return "Not classified"

    val = value.strip().lower()
    if val in {"female", "feminino"}:
        logging.debug("Matched female variants; returning 'Woman'")
        return "Woman"
    elif val in {"male", "masculino"}:
        logging.debug("Matched male variants; returning 'Man'")
        return "Man"
    elif val in {"", "unknown", "others", "none", "-"}:
        logging.debug("Matched null/unknown variants; returning 'Not classified'")
        return "Not classified"

    result = val.capitalize()
    logging.debug("Capitalized '%s' to '%s'", val, result)
    return result

def convert_numeric_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """
    Convert specified columns from Brazilian-style numeric strings
    ('.' thousands separator, ',' decimal separator) to numeric dtype,
    filling invalid or missing entries with zero.

    Logs each step for easier debugging.
    
    Parameters:
        df (pd.DataFrame): Input DataFrame.
        columns (list[str]): List of column names to convert.

    Returns:
        pd.DataFrame: The same DataFrame, with the specified columns converted.
    """
    logging.debug(">>> In converter_colunas_numericas; columns to convert: %s", columns)
    for col in columns:
        if col not in df.columns:
            logging.debug("Column '%s' not found in DataFrame; skipping.", col)
            continue

        logging.debug("Converting column '%s'", col)
        # Work on a string Series to strip and replace formatting
        s = df[col].astype(str)
        logging.debug("  Original head: %s", s.head(5).tolist())

        # 1) Remove non-breaking spaces
        s = s.str.replace("\u00a0", "", regex=False)
        # 2) Remove thousands separators (dots)
        s = s.str.replace(r"\.(?=\d{3}(?:\.|,))", "", regex=True)
        # 3) Convert comma decimals to dot
        s = s.str.replace(",", ".", regex=False)

        # 4) Convert to numeric, coercing errors to NaN, then fill with 0
        converted = pd.to_numeric(s, errors="coerce").fillna(0)
        logging.debug("  Converted head: %s", converted.head(5).tolist())

        df[col] = converted

    return df


def format_columns_to_comma_decimal(df: pd.DataFrame, cols: list[str], decimals: int = 2) -> pd.DataFrame:
    """
    Format specified float columns into Brazilian-style strings without thousands separators,
    using a comma as the decimal separator.

    Logs each conversion for debugging.

    Parameters:
        df (pd.DataFrame): Input DataFrame.
        cols (list[str]): List of column names to format.
        decimals (int): Number of decimal places to include (default is 2).

    Returns:
        pd.DataFrame: DataFrame with the specified columns reformatted as strings.
    """
    logging.debug(">>> In format_columns_to_comma_decimal; columns to format: %s with %d decimals", cols, decimals)
    for col in cols:
        if col not in df.columns:
            logging.debug("Column '%s' not found; skipping.", col)
            continue

        logging.debug("Formatting column '%s'", col)
        original_values = df[col].head(5).tolist()
        logging.debug("  Original head: %s", original_values)

        def fmt(x):
            try:
                s = f"{x:.{decimals}f}"
                return s.replace(".", ",")
            except Exception as e:
                logging.debug("    Failed to format value '%s': %s", x, e)
                return ""

        df[col] = df[col].apply(fmt)

        formatted_values = df[col].head(5).tolist()
        logging.debug("  Formatted head: %s", formatted_values)

    return df

import logging

def extract_meta_platform_from_placement(placement: str) -> str:
    """
    Determine the ad vehicle based on the placement string.

    Parameters:
        placement (str): The placement value from Meta Ads (e.g. "Facebook Feed", "Instagram Stories").

    Returns:
        str: 
            - "Facebook" if the placement mentions Facebook or Audience Network.
            - "Instagram" if the placement mentions Instagram.
            - Defaults to "Facebook" otherwise or if input isn’t a string.
    """
    logging.debug(">>> In extrair_veiculo; placement=%r", placement)
    if not isinstance(placement, str):
        logging.debug("Placement is not a string; defaulting to 'Facebook'")
        return "Facebook"

    text = placement.lower()
    if "facebook" in text or "audience" in text:
        logging.debug("Matched 'facebook' or 'audience' in placement -> 'Facebook'")
        return "Facebook"
    elif "instagram" in text:
        logging.debug("Matched 'instagram' in placement -> 'Instagram'")
        return "Instagram"

    logging.debug("No keywords matched; defaulting to 'Facebook'")
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
