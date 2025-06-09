# load/origin_writer.py

import logging
from typing import Optional

import pandas as pd
from gspread import Worksheet

from treat.utils.sheets_cache import get_worksheet
from treat.utils.write_back import write_back_df  # faz o batch_update

log = logging.getLogger(__name__)


def write_back_origin(
    df_raw: pd.DataFrame,
    df_ok: pd.DataFrame,
    creds_path: str,
    spreadsheet_id: str,
    *,
    write_back: bool,
    dry_run: bool = False,
    a1_range: str = "A1",
    value_input_option: str = "RAW",
    worksheet: Optional[Worksheet] = None,
    sheet_name: Optional[str] = None,
) -> pd.DataFrame:
    """
    Grava correções de `df_ok` de volta na aba de origem, ajustando o tamanho da planilha
    e escrevendo apenas uma vez via batch_update. Usa `worksheet` fornecido, se houver,
    para evitar reabrir a conexão. Caso contrário, abre via `sheet_name`.

    Parâmetros
    ----------
    df_raw : pd.DataFrame
        DataFrame original lido da aba.
    df_ok : pd.DataFrame
        DataFrame com valores corrigidos (apenas colunas originais de df_raw).
    creds_path : str
        Caminho para o JSON de credenciais.
    spreadsheet_id : str
        ID da planilha Google.
    write_back : bool
        Se False, a função retorna imediatamente sem gravar nada.
    dry_run : bool, default=False
        Se True, simula o write-back (faz cálculos mas não chama batch_update).
    a1_range : str, default="A1"
        Intervalo inicial onde começa a escrita (por padrão, "A1").
    value_input_option : str, default="RAW"
        Opção de input para a API ao escrever (ex.: "RAW" ou "USER_ENTERED").
    worksheet : Optional[gspread.Worksheet], default=None
        Se fornecido, usa este objeto Worksheet para resize e batch_update, sem nova
        chamada à API. Se None, `sheet_name` precisa estar definido e será usado para abrir.
    sheet_name : Optional[str], default=None
        Nome da aba de destino (somente usado se `worksheet` for None).

    Retorna
    -------
    pd.DataFrame
        O DataFrame efetivamente gravado (ou df_ok, no caso de dry_run ou write_back=False).
    """

    # 1) Garante que todas as colunas originais estão presentes em df_ok
    extras = set(df_ok.columns) - set(df_raw.columns)
    missing = set(df_raw.columns) - set(df_ok.columns)
    if extras:
         log.warning(
            "[write_back_origin] Ignorando colunas extras: %s",
            sorted(extras),
        )
    if missing:
        raise ValueError(
            f"[write_back_origin] Colunas faltando: {sorted(missing)}"
        )

    # 2) Se write_back estiver desligado, não grava nada
    if not write_back:
        log.info("🔸 Write-back de origem desativado")
        return df_ok

    # 3) Prepara DataFrame para gravação (apenas colunas originais)
    df_wb = df_ok[df_raw.columns].copy()
    n_linhas, n_colunas = df_wb.shape

    # 4) Calcula dimensões desejadas (incluindo cabeçalho)
    desired_rows = n_linhas + 1  # +1 para a linha de cabeçalho
    desired_cols = n_colunas

    # 5) Obtém o Worksheet (se não foi fornecido)
    if worksheet is None:
        if sheet_name is None:
            raise ValueError("Quando `worksheet=None`, `sheet_name` deve ser fornecido.")
        ws = get_worksheet(creds_path, spreadsheet_id, sheet_name)
        actual_sheet_name = sheet_name
    else:
        ws = worksheet
        actual_sheet_name = worksheet.title

    # 6) Redimensionamento seguro:
    frozen = ws._properties.get("gridProperties", {}).get("frozenRowCount", 0)
    min_rows = max(frozen + 1, 2)
    desired_rows = max(desired_rows, min_rows)

    if ws.row_count != desired_rows or ws.col_count != desired_cols:
        ws.resize(rows=desired_rows, cols=desired_cols)

    # 7) Logging de instrumentação
    total_cells = desired_rows * desired_cols
    log.info(
        "ℹ️  Preparando write-back origin para '%s': %d linhas × %d colunas (com cabeçalho) = %s células",
        actual_sheet_name,
        n_linhas,
        n_colunas,
        f"{total_cells:,}",
    )

    # 8) Se dry_run, não grava de fato
    if dry_run:
        log.info("🔸 Dry-run ativo: não gravando '%s'", actual_sheet_name)
        return df_wb

    # 9) Grava em batch usando write_back_df
    #    `write_back_df` ainda precisa receber o sheet_name (a string)
    write_back_df(
        df=df_wb,
        creds_path=creds_path,
        spreadsheet_id=spreadsheet_id,
        sheet_name=actual_sheet_name,
        a1_range=a1_range,
        value_input_option=value_input_option,
    )
    log.info(
        "✅ Write-back concluído para '%s' (%d linhas × %d colunas)",
        actual_sheet_name,
        n_linhas,
        n_colunas,
    )

    return df_wb
