import logging
import pandas as pd
from typing import List, Sequence, Any, Dict
from treat.bi_param_utils import BIParamLookup  # ajuste conforme seu projeto
import math
import numpy as np
from collections import defaultdict

log = logging.getLogger(__name__)



def _json_ready(obj):
    if obj is None:
        return None
    if isinstance(obj, bool):
        return obj
    if isinstance(obj, (int, str)):
        return obj
    if isinstance(obj, float):
        return None if math.isnan(obj) or math.isinf(obj) else obj
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        return None if math.isnan(obj) else float(obj)
    if isinstance(obj, dict):
        return {k: _json_ready(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return type(obj)(_json_ready(v) for v in obj)
    return str(obj)


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


def validate_taxonomy_consistency(
    df_ok: pd.DataFrame,
    df_bi: pd.DataFrame,
    cols_to_check: List[str] | None = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Verifica se valores de campaign_name, ad_group_name, ad_name e utm_content
    em df_ok existem nas colunas correspondentes da BI_PARAMETRIZAÇÃO.
    """

    # 1) Mapear nome do campo de origem → nome da coluna na BI
    bi_col_map = {
        "campaign_name":       "taxonomy_campaign_name",
        "ad_group_name":       "taxonomy_ad_group_name",  # ajuste conforme existir
        "ad_name":             "taxonomy_ad_name",
        "utm_content":         "utm_content",
    }

    report: Dict[str, Dict[str, Any]] = {}

    # 2) Preparar os sets da BI, normalizando só para lookup
    bi_sets: Dict[str, set] = {}
    for orig_col, bi_col in bi_col_map.items():
        if bi_col in df_bi.columns:
            bi_sets[orig_col] = set(
                df_bi[bi_col].astype(str).str.strip().str.lower()
            )

    # 3) Para cada coluna de origem, faça a checagem
    for orig_col, bi_col in bi_col_map.items():
        col_report = {"missing_column": False, "empty_count": 0, "unknown_values": []}

        # 3.1) Checa existência
        if orig_col not in df_ok.columns:
            col_report["missing_column"] = True
            log.warning("[Validação] Coluna '%s' inexistente em df_ok", orig_col)

        else:
            # 3.2) Série bruta e lookup key
            series_raw = df_ok[orig_col].astype(str)
            lookup = series_raw.str.strip().str.lower()

            # 3.3) Contagem de vazios
            empty = (series_raw.str.strip() == "").sum()
            col_report["empty_count"] = empty
            if empty:
                log.warning("[Validação] Coluna '%s' vazia em %d linha(s)", orig_col, empty)

            # 3.4) Comparação com o set correto da BI
            bi_set = bi_sets.get(orig_col, set())
            mask_unknown = (lookup != "") & (~lookup.isin(bi_set))
            unknown = series_raw[mask_unknown].unique().tolist()
            if unknown:
                preview = unknown[:20]
                log.warning(
                    "[Validação] %d valor(es) de '%s' fora da BI_PARAMETRIZAÇÃO (%s): %s%s",
                    len(unknown), orig_col, bi_col, preview, " …" if len(unknown) > 20 else ""
                )
            col_report["unknown_values"] = unknown

        report[orig_col] = col_report

    return _json_ready(report)


def validate_no_blank_cells(
    df: pd.DataFrame,
    *,
    allow_blank_cols: Sequence[str] = ("URL_do_Anuncio",),
    context: str | None = None,
) -> None:
    """
    Verifica se há células vazias em *todas* as colunas, exceto as listadas em
    ``allow_blank_cols``.  
    - Logs `WARNING` por coluna com células vazias.  
    - Se quiser tornar “fatal”, troque `log.warning`→`log.error` ou levante
      `ValueError`.
    """
    allow_blank_cols = {c.lower() for c in allow_blank_cols}
    # normaliza nomes → lower para comparação
    cols_to_check = [c for c in df.columns if c.lower() not in allow_blank_cols]

    for col in cols_to_check:
        # string vazia, NaN, None contam como “blank”
        blank_mask = df[col].astype(str).str.strip().isin(("", "nan", "None"))
        n_blank = int(blank_mask.sum())
        if n_blank:
            where = f" ({context})" if context else ""
            log.warning(
                "[Validação]%s Coluna '%s' vazia em %d linha(s)",
                where, col, n_blank
            )

def _norm_date(val: str | pd.Timestamp) -> str | None:
    """YYYY-MM-DD normalizado ou None se vazio/indefinido."""
    if pd.isna(val):                       # NaN / NaT
        return None
    s = str(val).strip()
    if not s or s.lower() in ("", "nan", "nat"):
        return None
    try:
        return pd.to_datetime(s).date().isoformat()
    except Exception:
        return s           # assume string já está ok (conta como valor)

def validate_consistent_dates_across_models(
    dest_dfs: Dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """
    Para cada par (Campanha, Veiculo), verifica se há mais de um valor distinto
    de start ou end em cada aba e também entre abas.

    Retorna um DataFrame com as inconsistências encontradas, com colunas:
      nível      ('aba' ou 'entre-abas')
      aba        nome da(s) aba(s) onde foi detectado o problema
      Campanha   nome da campanha
      Veiculo    nome do veículo
      start      valores distintos de início (set)
      end        valores distintos de fim (set)
    """
    # Acumula tuplas de inconsistência: (nível, aba, Campanha, Veiculo, start_set, end_set)
    prob: list[tuple[str, str, str, str, set, set]] = []

    # estruturas temporárias para coletar todos os valores
    starts = defaultdict(lambda: defaultdict(set))
    ends   = defaultdict(lambda: defaultdict(set))

    # 1) coleta todos os valores de every row
    for aba, df in dest_dfs.items():
        if not {"Campanha", "Veiculo", "start", "end"}.issubset(df.columns):
            continue
        for _, row in df.iterrows():
            key = (row["Campanha"], row["Veiculo"])
            starts[key][aba].add(row["start"])
            ends[key][aba].add(row["end"])

    # 2) valida dentro de cada aba (intra-aba)
    for (camp, veic), per_aba in starts.items():
        for aba, vals in per_aba.items():
            # desconsidera valores vazios/NaN
            valid = {v for v in vals if pd.notna(v) and str(v).strip()}
            if len(valid) > 1:
                log.warning(
                    "[Consistência Datas] Aba %s: campanha=%r, veículo=%r tem múltiplos start: %s",
                    aba, camp, veic, valid
                )
                prob.append(("aba", aba, camp, veic, valid, {""}))
    for (camp, veic), per_aba in ends.items():
        for aba, vals in per_aba.items():
            valid = {v for v in vals if pd.notna(v) and str(v).strip()}
            if len(valid) > 1:
                log.warning(
                    "[Consistência Datas] Aba %s: campanha=%r, veículo=%r tem múltiplos end: %s",
                    aba, camp, veic, valid
                )
                prob.append(("aba", aba, camp, veic, set(), valid))

    # 3) valida entre abas
    for key in set(starts) | set(ends):
        camp, veic = key
        all_starts = {v for per in starts[key].values() for v in per if pd.notna(v) and str(v).strip()}
        all_ends   = {v for per in ends[key].values()   for v in per if pd.notna(v) and str(v).strip()}
        if len(all_starts) > 1 or len(all_ends) > 1:
            aba_str = ",".join(sorted(starts[key].keys() | ends[key].keys()))
            log.warning(
                "[Consistência Datas] Entre abas: campanha=%r, veículo=%r tem inconsistência start/end em %s",
                camp, veic, aba_str
            )
            prob.append(("entre-abas", aba_str, camp, veic, all_starts, all_ends))

    # monta DataFrame de problemas
    check = pd.DataFrame(
        prob,
        columns=["nível", "aba", "Campanha", "Veiculo", "start", "end"]
    )

    if check.empty:
        log.info("✅ Nenhuma divergência de start/end entre modelos.")
    return check
