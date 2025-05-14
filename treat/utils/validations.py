import logging
import pandas as pd
from typing import List, Sequence
from treat.bi_param_utils import BIParamLookup  # ajuste conforme seu projeto
import math


log = logging.getLogger(__name__)

def check_required_columns(
    df: pd.DataFrame,
    *,
    optional_cols: List[str] = None,
    zero_valid_cols: List[str] = None,
) -> None:
    """
    Verifica células vazias em colunas obrigatórias e registra no log.

    Args
    ----
    df : DataFrame já tratado (df_ok).
    optional_cols : colunas que PODEM ficar vazias (ex: URL_do_Anuncio).
    zero_valid_cols : colunas numéricas cujo valor 0 é considerado “preenchido”.
    """
    optional_cols   = optional_cols or []
    zero_valid_cols = zero_valid_cols or []

    # 1) detecta linhas completamente em branco (exceto optional_cols)
    emptiness = df.isna() | df.astype(str).apply(lambda s: s.str.strip().isin(["", "nan"]))
    cols_for_blank = [c for c in df.columns if c not in optional_cols]
    fully_blank_rows = emptiness[cols_for_blank].all(axis=1)

    # 2) para cada coluna obrigatória, marca vazios — ignorando as linhas totalmente em branco
    for col in df.columns:
        if col in optional_cols:
            continue

        serie = df[col]
        if col in zero_valid_cols:
            mask_empty = serie.isna() | (serie.astype(str).str.strip() == "")
        else:
            mask_empty = (
                serie.isna() |
                serie.astype(str).str.strip().isin(["", "nan"])
            )

        mask_empty &= ~fully_blank_rows

        if mask_empty.any():
            linhas = mask_empty[mask_empty].index.tolist()
            preview = ", ".join(str(i) for i in linhas[:10])
            if len(linhas) > 10:
                preview += ", …"
            log.warning(
                "[Validação] Coluna '%s' vazia em %d linha(s): %s",
                col, len(linhas), preview
            )

def validate_utm_content_in_bi(
    df_raw: pd.DataFrame,
    df_bi: pd.DataFrame,
    *,
    bi_utm_cols: Sequence[str] = ("utm_content_raw", "utm_content")
) -> None:
    """
    Garante que todo utm_content de df_raw exista em df_bi.
    Se faltar algum, levanta RuntimeError listando os utms e as linhas.
    """
    if "utm_content" not in df_raw.columns:
        return

    raw_utms = df_raw["utm_content"].astype(str).str.strip()
    cols = {c.strip().lower(): c for c in df_bi.columns}
    bi_col = next((cols[k] for k in bi_utm_cols if k in cols), None)
    if bi_col is None:
        raise RuntimeError("Não encontrei coluna de utm_content em BI_PARAMETRIZAÇÃO")

    allowed = set(df_bi[bi_col].astype(str).str.strip())
    mask_missing = raw_utms.astype(bool) & ~raw_utms.isin(allowed)
    if mask_missing.any():
        missing_utms = raw_utms[mask_missing].unique().tolist()
        linhas = mask_missing[mask_missing].index.tolist()
        preview = linhas[:10] + (["…"] if len(linhas) > 10 else [])
        raise RuntimeError(
            f"UTM_CONTENT(s) não mapeado(s) em BI_PARAMETRIZAÇÃO: {missing_utms} "
            f"(linhas: {preview})"
        )


def validate_utm_content_with_lookup(
    df_raw: pd.DataFrame,
    creds_path: str,
    spreadsheet_id: str,
) -> None:
    """
    Verifica se todos os utm_content em df_raw existem na aba BI_PARAMETRIZAÇÃO
    carregada via BIParamLookup; imprime linhas faltantes.
    """
    if "utm_content" not in df_raw.columns:
        print("[VALIDATION] coluna 'utm_content' ausente nos dados brutos.")
        return

    raw_utms = df_raw["utm_content"].astype(str).str.strip()
    lookup = BIParamLookup(creds_path, spreadsheet_id)
    lookup._ensure_df()
    df_param = lookup._df

    cols = {c.strip().lower(): c for c in df_param.columns}
    bi_col = cols.get("utm_content_raw") or cols.get("utm_content")
    if not bi_col:
        print("[VALIDATION] não encontrei coluna 'utm_content[_raw]' em BI_PARAMETRIZAÇÃO.")
        return

    bi_utms = set(df_param[bi_col].astype(str).str.strip())
    mask_missing = raw_utms.astype(bool) & ~raw_utms.isin(bi_utms)
    missing = raw_utms[mask_missing]

    if missing.empty:
        print("[VALIDATION] OK — todos os utm_content existem em BI_PARAMETRIZAÇÃO.")
    else:
        for idx, utm in missing.items():
            print(f"[VALIDATION] utm_content '{utm}' não encontrado (linha {idx})")
        print(f"[VALIDATION] Total de {len(missing)} utm_content sem correspondência.")


def validate_aggregates(
    df_raw: pd.DataFrame,
    df_ok: pd.DataFrame,
    impressions_col: str = "impressions",
    cost_col: str = "cost",
    *,
    tol: float = 1e-6,
) -> None:
    """
    Verifica se os somatórios de impressões e custo em df_ok
    correspondem aos valores de df_raw. Se houver divergência,
    lança RuntimeError com detalhes.

    Parameters
    ----------
    df_raw : DataFrame original antes do tratamento
    df_ok  : DataFrame já tratado (antes da renomeação de colunas)
    impressions_col : nome da coluna de impressões
    cost_col        : nome da coluna de custo
    tol             : tolerância para comparação de floats
    """
    # checa existência das colunas
    for col in (impressions_col, cost_col):
        if col not in df_raw.columns or col not in df_ok.columns:
            log.warning(
                "[Validação] Coluna '%s' ausente em df_raw ou df_ok; pulando aggregate check", 
                col
            )
            return

    # converte para numérico (substui vírgula decimal)
    def to_num(s: pd.Series) -> pd.Series:
        return pd.to_numeric(
            s.astype(str).str.replace(",", ".", regex=False),
            errors="coerce"
        ).fillna(0)

    raw_imp = to_num(df_raw[impressions_col]).sum()
    ok_imp  = to_num(df_ok[impressions_col]).sum()
    raw_cost = to_num(df_raw[cost_col]).sum()
    ok_cost  = to_num(df_ok[cost_col]).sum()

    errs: list[str] = []
    if not math.isclose(raw_imp, ok_imp, rel_tol=tol, abs_tol=tol):
        errs.append(
            f"impressions: raw={raw_imp:.2f} vs ok={ok_imp:.2f}"
        )
    if not math.isclose(raw_cost, ok_cost, rel_tol=tol, abs_tol=tol):
        errs.append(
            f"cost      : raw={raw_cost:.2f} vs ok={ok_cost:.2f}"
        )

    if errs:
        msg = "Divergência nos totais:\n  " + "\n  ".join(errs)
        raise RuntimeError(msg)
    else:
        log.info("[Validação] Totais de impressions e cost conferem: %d imp, %.2f cost",
                 raw_imp, raw_cost)