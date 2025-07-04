#!/usr/bin/env python
# coding: utf-8

# # Ponto de Controle
# Este notebook valida e escreve dados transformados no Google Sheets.

# In[ ]:


import os
import pandas as pd
import datetime as dt
from extract import read_df
from treat.utils.datas import normalize_date_to_str_DD_M_YYYY
from treat.utils.write_dataframe_to_sheet import write_dataframe_to_sheet
from treat.utils.normalize import normalize_vehicle
from treat.utils.datas import concat_period
from treat.utils.campos_calculados import make_id_ponto_de_controle
from treat.utils.campos_calculados import add_key_creative
from treat.utils.campos_calculados import dedupe_by_key_creative
from treat.settings import MIN_DATE
import numpy as np


# In[ ]:


# Flags de execução
"""
Célula  – Imports & parâmetros globais

Define:
- Módulos padrão e helpers do projeto
- Flags de execução e IDs de planilhas via env var
- Constantes de aba, cabeçalho e filtro de data
- Lista de colunas de destino
"""
DRY_RUN = True

# IDs das planilhas via variáveis de ambiente
os.environ["ORIGIN_SHEET_ID"] = "1DazUQxspLgT0utOFHcTINbFngXw7Fq0LOq6v4lRGixg"
ORIGIN_SHEET_ID = os.getenv("ORIGIN_SHEET_ID")
os.environ["DEST_SHEET_ID"] = "1DpH5tu4KJKqbA6ueFtf1s1FueBkR4-EtPf5xHyXx8zw"
DEST_SHEET_ID = os.getenv("DEST_SHEET_ID")

# Garantia de que foram definidas
assert (
    ORIGIN_SHEET_ID is not None and DEST_SHEET_ID is not None
), "Defina as variáveis de ambiente ORIGIN_SHEET_ID e DEST_SHEET_ID"


# In[ ]:


# Constantes de aba & cabeçalho: nomes centralizados em um só lugar
ORIGIN_TAB = "modeloGeral"
DEST_TAB = "IMPULSIONAMENTOS 2025"
HEAD_ROW_DEST = 4  # zero-based (header na linha 5)


# In[ ]:


# Filtro temporal & Data mínima – usado no filtro posterior
MIN_DATE = dt.date(2025, 6, 1)


# In[ ]:


DEST_COLUMNS = [
    "Data",                             # ← vem de "start"
    "Campanha",
    "Veiculo",
    "Link conteúdos impulsionados",
    "Periodo",                          # ← concat(start, end)
    "Agência",
    "Editoria",
    "Objetivo",
    "Meta",
    "Status",
    "Resultado"
]
assert (
    len(DEST_COLUMNS) == 11
), f"DEST_COLUMNS deve ter 11 colunas, mas tem {len(DEST_COLUMNS)}"
print("▶ DEST_COLUMNS definido:", DEST_COLUMNS)


# In[ ]:


# Leitura da aba de origem – deve executar sem exceção se ORIGIN_SHEET_ID estiver definido
"""
Célula 2 – Leitura + filtro temporal da aba modeloGeral
- Lê df_origin com read_df()
- Converte coluna date para date_dt
- Filtra linhas >= MIN_DATE
- Garante colunas críticas e prepara df_origin
"""
df_origin = read_df(
    sheet_id=ORIGIN_SHEET_ID,
    tab=ORIGIN_TAB,
    header_row=0,
)
from treat.utils.datas import filter_by_min_date

# ST-2: aplica filtro de data mínima no mesmo df_origin
df_origin = filter_by_min_date(df_origin)
print(f"▶ df_origin ≥ {MIN_DATE}: {len(df_origin)} linhas")

# ST-3: gera key_creative de forma padronizada
df_origin = add_key_creative(df_origin)
print(f"▶ key_creative gerado em {len(df_origin)} linhas")

# 5.2: deduplica mantendo primeira ocorrência
orig_len = len(df_origin)
df_origin = dedupe_by_key_creative(df_origin)
assert len(df_origin) <= orig_len, (
    f"5.2 Falha: pós-dedup {len(df_origin)} > original {orig_len}"
)
print(f"▶ 5.2 Deduplicados: {orig_len - len(df_origin)} linhas removidas")

# Sanitiza 'date' para str preservando zeros
df_origin["date"] = df_origin["date"].astype(str)

# (opcional) quick-check em DRY_RUN
if DRY_RUN:
    display(df_origin[["date", "date_dt"]].head())
    invalidados = df_origin["date_dt"].isna().sum()
    print(f"❌ {invalidados} date_dt inválidos ou não parseados")


# In[ ]:


# 🕵️‍♂️ Filtra linhas com key_creative vazia
faltando = df_origin[df_origin["key_creative"] == ""]

# 🧾 Exibe os índices (linhas da planilha) e os campos relevantes
print(f"Total de linhas com key_creative vazio: {len(faltando)}")

# Mostra os índices reais e os dados importantes
display(faltando.reset_index()[["index", "utm_content", "ad_name", "ad_group_name", "ID_Campanha"]])


# In[ ]:


# Quick-preview em DRY_RUN – exibe head e contagem somente em Dry Run
if DRY_RUN:
    display(df_origin.head())
    print(f"Total de linhas em df_origin: {len(df_origin)}")


# In[ ]:


# Assert de colunas críticas – garante que df_origin tenha todas as colunas necessárias
required_columns = ["date", "Campanha", "Veiculo", "URL_do_Anuncio", "objective", "start"]
missing_cols = [col for col in required_columns if col not in df_origin.columns]
if missing_cols:
    raise RuntimeError(f"Colunas críticas ausentes em df_origin: {missing_cols}")


# In[ ]:


# Clean-up da coluna auxiliar – remover 'date_dt' apenas após aplicar o filtro de data mínima (útil para debug)
if not DRY_RUN:
    df_origin.drop(columns=["date_dt"], inplace=True)


# In[ ]:


# Clonar DataFrame para não mutar a leitura crua
df = df_origin.copy()
df["Criativo"] = df_origin["key_creative"]
assert "Criativo" in df.columns and df["Criativo"].equals(
    df_origin["key_creative"]
), "5.3 Falha: coluna 'Criativo' não corresponde a key_creative"
print("▶ 5.3 Criativo mapeado com sucesso")


# In[ ]:


# Criar coluna 'Data'
df["Data"] = df["start"].apply(normalize_date_to_str_DD_M_YYYY)

# Criar coluna 'Periodo'
df["Periodo"] = df.apply(
    lambda r: concat_period(r["start"], r["end"]),
    axis=1
)

# Agora é seguro remover colunas auxiliares
df.drop(columns=["start", "end"], inplace=True)

# Verificações
assert df["Data"].ne("").all(), "❌ Há valores vazios em Data"
assert df["Periodo"].ne("").all(), "❌ Há valores vazios em Periodo"

# Definir colunas finais
DEST_COLUMNS = [
    "Data",
    "Campanha",
    "Veiculo",
    "Link conteúdos impulsionados",
    "Periodo",
    "Agência",
    "Editoria",
    "Objetivo",
    "Meta",
    "Status",
    "Resultado"
]
df_transf = df.reindex(columns=DEST_COLUMNS, fill_value="")
assert df_transf.columns.tolist() == DEST_COLUMNS


# In[ ]:


# Normaliza Veiculo diretamente com fallback embutido
df["Veiculo"] = df["Veiculo"].apply(normalize_vehicle)

# Opcional: verificar se ainda há valores em branco
assert df["Veiculo"].ne("").all(), "Ainda há valores vazios em Veiculo"


# In[ ]:


# Mapear colunas diretas
df["Campanha"] = df["Campanha"]
df["Link conteúdos impulsionados"] = df["URL_do_Anuncio"]
df["Objetivo"] = df["objective"]


# Debug prints para verificar mapeamento
print("Preview 'Campanha':", df["Campanha"].head().tolist())
print(
    "Preview 'Link conteúdos impulsionados':",
    df["Link conteúdos impulsionados"].head().tolist(),
)
print("Preview 'Objetivo", df["Objetivo"].head().tolist())

# Asserts para garantir a presença das colunas
assert "Campanha" in df.columns, "Coluna 'Campanha' não encontrada"
assert (
    "Link conteúdos impulsionados" in df.columns
), "Coluna 'Link conteúdos impulsionados' não encontrada"
assert "Objetivo" in df.columns, "Coluna 'Objetivo' não encontrada"


# In[ ]:


# Colunas constantes / vazias
df["Agência"] = "De Brito"
df["Editoria"] = df["Campanha"]
df["Meta"] = ""
df["Status"] = ""
df["Resultado"] = ""

# Debug prints para verificar preenchimento
print("Preview 'Agência':", df["Agência"].head().tolist())
print("Preview 'Editoria':", df["Editoria"].head().tolist())
print("Valores únicos em 'Agência':", df["Agência"].unique())
print("Contagem não vazia em 'Meta (número)':", df["Meta"].astype(bool).sum())
print("Contagem não vazia em 'Status':", df["Status"].astype(bool).sum())
print("Contagem não vazia em 'Resultado':", df["Resultado"].astype(bool).sum())

# Asserts para garantir colunas e conteúdo esperado
assert (
    "Agência" in df.columns and df["Agência"].eq("De Brito").all()
), "Erro em 'Agência': valores diferentes de 'De Brito' ou coluna ausente"
assert "Editoria" in df.columns, "Coluna 'Editoria' ausente"
assert all(df["Meta"] == ""), "'Meta"
assert all(df["Status"] == ""), "'Status' deve ser completamente vazio"
assert all(df["Resultado"] == ""), "'Resultado' deve ser completamente vazio"


# In[ ]:


# Debug prints e asserts para verificar ordem e conteúdo das colunas


# Reordenar / reindexar com DEST_COLUMNS e preencher vazios
df_transf = df.reindex(columns=DEST_COLUMNS, fill_value="")
print("Colunas em df_transf:", df_transf.columns.tolist())
assert set(DEST_COLUMNS).issubset(
    df_transf.columns
), f"Colunas fora de ordem ou faltando: {df_transf.columns.tolist()}"

# Mostrar as primeiras linhas para confirmação visual
display(df_transf.head())
print(f"Total de linhas em df_transf: {len(df_transf)}")


# In[ ]:


# ▶ Gerar __ID__ após garantir colunas necessárias
print("▶ Gerando __ID__ em df_transf...")
df_transf["__ID__"] = df_transf.apply(make_id_ponto_de_controle, axis=1)

# ▶ Validações + deduplicação amistosa
n_total   = len(df_transf)
n_unique  = df_transf["__ID__"].nunique()
n_duplics = n_total - n_unique

assert df_transf["__ID__"].isna().sum() == 0, (
    "❌ Há NaNs em __ID__ em df_transf — verifique campos obrigatórios vazios"
)

if n_duplics:
    print(f"⚠️  {n_duplics} IDs duplicados encontrados — mantendo apenas a 1ª ocorrência.")
    df_transf = (
        df_transf
        .drop_duplicates("__ID__", keep="first")
        .reset_index(drop=True)
    )

print(f"✅ __ID__ válido: {df_transf['__ID__'].nunique()} únicos em {len(df_transf)} linhas")
    


# In[ ]:


# 3.7.1 – Dropar colunas auxiliares 'start' e 'end' se ainda existirem
aux_cols = [c for c in ["start", "end"] if c in df_transf.columns]
if aux_cols:
    df_transf.drop(columns=aux_cols, inplace=True)

# Debug: confirmar que as colunas auxiliares foram removidas
print("Colunas após remoção de 'start' e 'end':", df_transf.columns.tolist())


# In[ ]:


# ------------------------------------------------------------------
# ▸ GERAR e DEDUPLICAR __ID__ EM df_origin ------------------------
# ------------------------------------------------------------------
df_origin["__ID__"] = df_origin.apply(make_id_ponto_de_controle, axis=1)
df_origin = df_origin.drop_duplicates("__ID__", keep="first").reset_index(drop=True)

# ------------------------------------------------------------------
# ▸ MAPEAR 'Criativo' PARA df_transf ALINHANDO PELO __ID__
# ------------------------------------------------------------------
# 0. Garante que não sobraram colunas 'Criativo' antigas em df_transf
if "Criativo" in df_transf.columns:
    df_transf.drop(columns="Criativo", inplace=True)

df_transf = (
    df_transf
        .merge(
            df_origin[["__ID__", "key_creative"]],
            on="__ID__", how="left",
            suffixes=("", "_DROP")          # evita _x / _y nas demais colunas
        )
        .rename(columns={"key_creative": "Criativo"})
)

# 🛈  Info apenas – não trava execução
n_missing = int(df_transf["Criativo"].isna().sum())   # força escalar
if n_missing > 0:
    print(f"⚠️  {n_missing} linhas sem 'Criativo' (OK, coluna é só para debug)")

# Se preferir remover 'Criativo' daqui pra frente:
# df_transf.drop(columns="Criativo", inplace=True)


# In[ ]:


# ---------------------------------------------------------------------------
# Consolidar aba de destino “IMPULSIONAMENTOS 2025” preservando dropdowns,
# check-boxes e demais valores FORMATADOS, e gerar __ID__ final.
# ---------------------------------------------------------------------------
import os, re, unicodedata
import pandas as pd
from treat.utils.get_google_client import get_google_client
from treat.utils.campos_calculados import make_id_ponto_de_controle

# ————— constantes já definidas no notebook ——————————————
# DEST_SHEET_ID, DEST_TAB, HEAD_ROW_DEST (5), DEST_COLUMNS (lista de 11 colunas, incluindo "Veiculo")

HEADER_ROW_SHEET = HEAD_ROW_DEST + 1

# 1 • Conectar ao worksheet via gspread
CREDS_PATH = os.getenv("GOOGLE_CREDS_PATH", "creds.json")
gclient = get_google_client(CREDS_PATH)
ws_dest = gclient.open_by_key(DEST_SHEET_ID).worksheet(DEST_TAB)

# 2 • Captura cabeçalho “bruto” (linha 5 → índice 5 no Sheets)
header_raw = ws_dest.row_values(HEADER_ROW_SHEET)
print("Header bruto:", header_raw)


# 3 • Limpa cada label de cabeçalho
def _clean(label: str) -> str:
    txt = unicodedata.normalize("NFKD", label or "")
    txt = "".join(c for c in txt if not unicodedata.combining(c))
    txt = txt.replace("\n", " ").strip()
    return re.sub(r"\s{2,}", " ", txt)


cleaned = [_clean(c) for c in header_raw]
print("Header limpo :", cleaned)

# 4 • Puxa linhas formatadas a partir da linha 6 (HEAD_ROW_DEST+1)
body = ws_dest.get_values(
    f"A{HEADER_ROW_SHEET+1}:K",
)
assert body, "❌ body veio vazio: não trouxe nenhuma linha de dados"

# 5 • Normaliza cada linha para ter exatamente 11 colunas
n_cols = len(DEST_COLUMNS)
normalized = [(row + [""] * n_cols)[:n_cols] for row in body]

# 6 • Monta DataFrame com o cabeçalho oficial
df_dest = pd.DataFrame(normalized, columns=DEST_COLUMNS)
if "Criativo" not in df_dest.columns:
    df_dest["Criativo"] = ""
assert set(DEST_COLUMNS).issubset(df_dest.columns)

print("Colunas após montagem:", df_dest.columns.tolist())


# 8 • Limpa linhas totalmente vazias (bordas/formatação)
df_dest = df_dest.replace("", pd.NA).dropna(how="all").reset_index(drop=True)
print(f"Linhas válidas em df_dest após dropna: {len(df_dest)}")

# 9 • Gera coluna __ID__ — somente aqui, com todos os campos (incluindo key_creative já embutido)
df_dest["__ID__"] = df_dest.apply(make_id_ponto_de_controle, axis=1)

# 9.1 • Deduplica mantendo o 1º registro de cada ID
before = len(df_dest)
df_dest = df_dest.drop_duplicates("__ID__", keep="first").reset_index(drop=True)
removed = before - len(df_dest)
print(f"🧹 Removidas {removed} linhas duplicadas de __ID__ (mantida a 1ª ocorrência)")

# 10 • Validações finais (já com df_dest deduplicado)
assert df_dest["__ID__"].notna().all(), "❌ Há NaNs em __ID__"
assert df_dest["__ID__"].nunique() == len(
    df_dest
), f"❌ __ID__ duplicados ainda presentes: {df_dest['__ID__'].nunique()} únicos vs {len(df_dest)} linhas"

print("▶ __ID__ gerado, deduplicado e validado com sucesso")


# 10 • Validações finais
assert df_dest["__ID__"].notna().all(), "❌ Há NaNs em __ID__"
assert df_dest["__ID__"].nunique() == len(
    df_dest
), f"❌ __ID__ duplicados: {df_dest['__ID__'].nunique()} únicos vs {len(df_dest)} linhas"
print("▶ __ID__ gerado e validado com sucesso")

# 11 • Pré-visualização
display(df_dest.head())
print(df_dest.apply(lambda c: (c != "") & (c.notna())).sum())


# In[ ]:


# ---------------------------------------------------------------------------
# ▶ Preparar df_dest: garantir coluna 'Criativo' e ordem exata de DEST_COLUMNS
# ---------------------------------------------------------------------------

# 1. Se não houver 'Criativo', criar vazia
if "Criativo" not in df_dest.columns:
    print("⚠️ Coluna 'Criativo' ausente em df_dest – criando vazia.")
    df_dest["Criativo"] = ""

# 2. Reindexar nas colunas oficiais (novo DEST_COLUMNS)
df_dest = df_dest.reindex(columns=DEST_COLUMNS, fill_value="")

# 3. Debug / validação
print("Colunas em df_dest após reindex:", df_dest.columns.tolist())
assert (
    df_dest.columns.tolist() == DEST_COLUMNS
), f"❌ Colunas inesperadas em df_dest: {df_dest.columns.tolist()}"
display(df_dest.head())


# In[ ]:


# ---------------------------------------------------------------------------
# ▶ 5.x: Gerar __ID__ em df_dest, deduplicar e identificar novas linhas
# ---------------------------------------------------------------------------

# 1 • Gera __ID__ final
df_dest["__ID__"] = df_dest.apply(make_id_ponto_de_controle, axis=1)

# 2 • Validações
assert df_dest["__ID__"].notna().all(), "❌ Há NaNs em __ID__ no destino"
n_uniques = df_dest["__ID__"].nunique()
n_total = len(df_dest)
if n_uniques < n_total:
    print(
        f"⚠️ {n_total-n_uniques} IDs duplicados em df_dest – vou manter só a primeira ocorrência."
    )
df_dest = df_dest.drop_duplicates("__ID__", keep="first").reset_index(drop=True)
print(f"▶ Após dedup: {len(df_dest)} linhas únicas de destino")

# 3 • Identifica quais linhas de df_transf ainda não estão em df_dest
novos = df_transf[~df_transf["__ID__"].isin(df_dest["__ID__"])].copy()
print(f"▶ Linhas novas a inserir: {len(novos)} de {len(df_transf)} totais")
display(novos.head())


# In[ ]:


# 5.8 • Gerar __ID__ em df_dest com a nova função e validar unicidade
# -------------------------------------------------------------------
# Aplica make_id_ponto_de_controle e checa se gerou algo
df_dest["__ID__"] = df_dest.apply(make_id_ponto_de_controle, axis=1)

# Debug / validação
print("Preview de __ID__ (5 primeiras linhas):")
print(df_dest["__ID__"].head().tolist())

# Verifica se algum ID ficou vazio ou nulo
n_empty = df_dest["__ID__"].isna().sum() + (df_dest["__ID__"] == "").sum()
assert n_empty == 0, f"❌ {n_empty} __ID__ vazios ou NaN em df_dest"

# Verifica unicidade
n_total = len(df_dest)
n_unique = df_dest["__ID__"].nunique()
print(f"Total linhas: {n_total}  —  IDs únicos: {n_unique}")
assert n_unique == n_total, f"❌ Há {n_total-n_unique} IDs duplicados em df_dest"

print("✅ __ID__ gerados e validados com sucesso em df_dest")


# In[ ]:


# 5.9 • Deduplicar novos registros
# -------------------------------------------------------------------
# 1 · Conta total de registros em df_transf
total_transf = len(df_transf)

# 2 · Filtra somente os que ainda não existem em df_dest
novos = df_transf[~df_transf["__ID__"].isin(df_dest["__ID__"])].copy()

# 3 · Debug / validação
print(f"Total em df_transf: {total_transf}")
print(f"Novos identificados: {len(novos)}")
assert (
    len(novos) <= total_transf
), f"❌ Falha de deduplicação: len(novos)={len(novos)} > len(df_transf)={total_transf}"

# 4 · Mostra uma prévia dos novos registros
display(novos.head())

# 5 · Está pronto para escrever apenas 'novos' no destino


# In[ ]:


# 5.10 • Validação de “Periodo” em df_transf
# ----------------------------------------------------------------------------
# Garante que não haja strings vazias na coluna “Periodo”
assert (
    not df_transf["Periodo"].eq("").any()
), f"❌ Há {df_transf['Periodo'].eq('').sum()} registro(s) com 'Periodo' vazio em df_transf"
print(f"✅ Todos os {len(df_transf)} registros em df_transf têm 'Periodo' preenchido")


# In[ ]:


# ---------------------------------------------------------------------------
# Gerar coluna de identificador único (__ID__) em df_transf
# Mesmo make_id_ponto_de_controle usado para df_dest
# ---------------------------------------------------------------------------

# 1 · Cria/atualiza a coluna __ID__ em df_transf
df_transf["__ID__"] = df_transf.apply(make_id_ponto_de_controle, axis=1)

# 2 · Validação rápida
print(
    f"__ID__ gerados em df_transf: {df_transf['__ID__'].nunique()} (únicos) / {len(df_transf)} (linhas)"
)
assert (
    df_transf["__ID__"].isna().sum() == 0
), "Há valores NaN em __ID__ em df_transf — verifique campos vazios"

# 3 · Pré-visualização
display(df_transf.head()[["Periodo", "Campanha", "Veiculo", "__ID__"]])


# In[ ]:


# ---------------------------------------------------------------------------
# DEBUG – conferindo como o __ID__ está sendo montado em df_transf
# ---------------------------------------------------------------------------
cols_check = ["Periodo", "Veiculo", "Criativo", "__ID__"]

# Se alguma coluna estiver faltando, avisa (evita KeyError no display)
missing = [c for c in cols_check if c not in df_transf.columns]
assert not missing, f"⚠️ Colunas ausentes em df_transf: {missing}"

# Mostra as 5 primeiras linhas com os campos-chave
display(df_transf[cols_check].head())


# In[ ]:


# ---------------------------------------------------------------------------
# Deduplicação: filtra apenas as linhas novas em df_transf
# ---------------------------------------------------------------------------

# 1 • Cria o DataFrame somente com os registros cujo __ID__ não está em df_dest
novos = df_transf[~df_transf["__ID__"].isin(df_dest["__ID__"])].copy()

# 2 • Validação rápida: nunca adicionar mais registros do que existem na origem
assert len(novos) <= len(
    df_transf
), f"Erro de deduplicação: len(novos)={len(novos)} maior que len(df_transf)={len(df_transf)}"

# 3 • Exibe quantas linhas novas foram identificadas
print(f"Linhas novas após deduplicação: {len(novos)}")

# 4 • Pré-visualização das primeiras entradas novas
display(novos.head(10))


# In[ ]:


df_final = novos.reindex(columns=DEST_COLUMNS, fill_value="").copy()
df_final.drop(columns="__ID__", inplace=True, errors="ignore")


def _exit_if_no_rows(df: pd.DataFrame, dry_run: bool) -> bool:
    """
    Early-exit safeguard before writing to Google Sheets.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame pronto para escrita.
    dry_run : bool
        Se True, estamos em modo simulação (nunca escreve).

    Returns
    -------
    bool
        True  -> há linhas para escrever OU estamos em DRY_RUN.  
        False -> DRY_RUN==False e df está vazio (nada a fazer).
    """
    rows = len(df)
    print(f"▶ Integrity check – rows in df_final: {rows}, DRY_RUN={dry_run}")

    # Caso real de saída: não é dry-run e não há linhas
    if not dry_run and rows == 0:
        print("Nenhuma linha nova para escrever.")
        return False

    # Assert extra de coerência (ajuda em debug)
    assert dry_run or rows > 0, (
        "Guard clause failed: DRY_RUN is False but DataFrame is empty."
    )
    return True


# --------------------------------------------------------------------------- #
# ↳ Uso: chame antes de qualquer escrita                                      #
# --------------------------------------------------------------------------- #
proceed_with_write = _exit_if_no_rows(df_final, DRY_RUN)

if not proceed_with_write:
    # Encerramos a célula de forma limpa; código de escrita virá em outra célula.
    # Adicione apenas um 'pass' para que o notebook não gere erro.
    pass




# In[ ]:


from IPython.display import display

# Mostrar TODO o DataFrame -- desativa truncamento de linhas/colunas
pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)

print(f"▶ Preview completo de df_final ({len(df_final)} linhas):")
display(df_final)


# In[ ]:


# ⬇️ Célula ÚNICA de escrita
if proceed_with_write:              # vari‧ável gerada pelo guard-clause
    if DRY_RUN:
        print("⭐ DRY_RUN ativo – nada será escrito no Sheets.")
    else:
        # 1. Quantas linhas já existem na aba-destino?
        used_rows = len(df_dest)    # df_dest tem só dados, sem cabeçalho

        # 2. Linha inicial de gravação (1-based no Sheets)
        start_row = HEAD_ROW_DEST + 1 + used_rows + 1
        #                 header  ▲   dados já escritos ▲  linha vazia ▲

        print(f"▶ Gravando {len(df_final)} novas linhas a partir da linha {start_row}")

        # 3. Enviar somente valores – sem cabeçalho
        # ✅ 1) passando o ID como 1º parâmetro posicional
        # 1. totalmente posicional
        import json, pathlib

        creds_path = pathlib.Path("creds.json")        # ou seu path real
        with creds_path.open() as f:
            creds_json = f.read()

        write_dataframe_to_sheet(
            spreadsheet_id = DEST_SHEET_ID,
            sheet_name     = DEST_TAB,
            df             = df_final,
            start_row      = start_row,
            include_header = False,
            google_credentials_json = creds_json,      # ✅ aqui!
        )



        print("✅ Escrita concluída com sucesso!")


# In[ ]:


import inspect, textwrap
from treat.utils.write_dataframe_to_sheet import write_dataframe_to_sheet
print(textwrap.dedent(inspect.getsource(write_dataframe_to_sheet)))

