import logging

import pandas as pd


def calcular_engajamento_total(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula a coluna 'Engajamento_Total' como a soma de:
    - post_reactions
    - post_shares
    - post_comments

    Caso os campos 'post_shares' ou 'post_comments' estejam ausentes ou vazios,
    assume valor 0.
    """
    for col in ["post_shares", "post_comments"]:
        if col not in df.columns:
            df[col] = 0
        else:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    if "post_reactions" not in df.columns:
        df["post_reactions"] = 0
    else:
        df["post_reactions"] = pd.to_numeric(
            df["post_reactions"], errors="coerce"
        ).fillna(0)

    df["Engajamento_Total"] = (
        df["post_reactions"] + df["post_shares"] + df["post_comments"]
    )

    return df


def inicializar_colunas_auxiliares(df: pd.DataFrame) -> pd.DataFrame:
    """
    Garante que as colunas auxiliares 'Numero' e 'ID' existam no DataFrame.
    """
    logging.debug(">>> In inicializar_colunas_auxiliares")
    df["Numero"] = df.get("Numero", pd.NA)
    df["ID"] = df.get("ID", pd.NA)
    return df


def remover_colunas_indesejadas(self):
    for col in ["placement", "campaign_id", "campaign_name", "utm_content"]:
        if col in self.df.columns:
            self.df.drop(columns=col, inplace=True)


def gerar_id(row: pd.Series) -> str:
    """
    ID = {data}-{Campanha}-{impressions}-{cost}-{link_clicks}
    Aceita tanto colunas snake_case (inglês) quanto as
    antigas em PT-BR com iniciais maiúsculas.
    """

    def pick(*candidatos: str) -> str:
        for c in candidatos:
            if c in row and str(row[c]).strip():
                return str(row[c]).strip()
        return ""

    parts = [
        pick("date", "Data"),
        pick("Campanha"),  # só existe em PT mesmo
        pick("impressions", "Impressoes"),
        pick("cost", "Investimento"),
        pick("link_clicks", "Cliques_no_Link"),
    ]
    return "-".join(parts)


def make_id_ponto_de_controle(row: pd.Series) -> str:
    """
    Concatena:
      Periodo|Campanha|Veiculo|Link conteúdos impulsionados|
      Agência|Editoria|Objetivo|Criativo
    Valores nulos (pd.NA/None) viram string vazia.
    """

    def safe(val) -> str:
        return "" if pd.isna(val) else str(val)

    # -- resolve 'criativo' sem ambiguidade -----------------------------
    criativo_val = row.get("Criativo", pd.NA)
    if pd.isna(criativo_val) or criativo_val == "":
        criativo_val = row.get("key_creative", "")
    criativo = safe(criativo_val)

    parts = [
        safe(row.get("Periodo")),
        safe(row.get("Campanha")),
        safe(row.get("Veiculo")),
        safe(row.get("Link conteúdos impulsionados")),
        safe(row.get("Agência")),
        safe(row.get("Editoria")),
        safe(row.get("Objetivo")),
        criativo,
    ]
    return "|".join(parts)


def add_key_creative(df: pd.DataFrame) -> pd.DataFrame:
    """
    Gera coluna 'key_creative' com prioridade de escolha:
      1. utm_content
      2. ad_name
      3. ad_group_name
      4. ID_Campanha

    Critério de aceite:
      - Todos os registros devem receber um valor não vazio.
      - Em caso de falta em todas as colunas-chave, será string vazia (asserta erro).
    """
    import numpy as _np

    # monta condições: cada coluna existe e não é string vazia
    conds = []
    for col in ("utm_content", "ad_name", "ad_group_name", "ID_Campanha"):
        if col in df.columns:
            conds.append(df[col].notna() & df[col].astype(str).str.strip().ne(""))
        else:
            conds.append(False)

    # escolhas correspondentes na mesma ordem
    choices = [
        df.get("utm_content", ""),
        df.get("ad_name", ""),
        df.get("ad_group_name", ""),
        df.get("ID_Campanha", ""),
    ]

    # cria a coluna
    df["key_creative"] = _np.select(conds, choices, default="")

    # debug: preview e assert
    print("Preview ‘key_creative’ (primeiras 5 linhas):")
    print(df["key_creative"].head(5).tolist())
    assert (
        df["key_creative"] != ""
    ).all(), "Há registros sem key_creative: verifique utm_content/ad_name/ad_group_name/ID_Campanha"

    return df


def dedupe_by_key_creative(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove linhas duplicadas com o mesmo `key_creative`, mantendo a primeira ocorrência.

    Critério de aceite:
      - O número de linhas resultante (`df_dedup`) deve ser menor ou igual ao original.
      - Não deve alterar a ordem relativa das linhas remanescentes.
    """
    # cópia para debug
    df_raw = df.copy()

    # deduplicação
    df_dedup = df_raw.drop_duplicates(subset="key_creative", keep="first")

    # debug: relatório de redução
    n_raw = len(df_raw)
    n_dedup = len(df_dedup)
    print(f"Deduplicação por key_creative: {n_raw} → {n_dedup} linhas")
    assert (
        n_dedup <= n_raw
    ), f"Após drop_duplicates, {n_dedup} linhas mas original tinha {n_raw}"

    # mostra as primeiras 5 chaves para conferência
    print("Preview das primeiras 5 key_creative únicas:")
    print(df_dedup["key_creative"].head(5).tolist())

    return df_dedup
