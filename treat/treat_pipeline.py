# treat/treat_pipeline.py
"""
Pipeline de tratamento por aba de origem.

Características principais
──────────────────────────
• **Uma única autenticação** – `SheetsFetcher` é criado no `__init__`
  (ou reaproveitado de `builtins.fetcher`) e serve para todas as leituras.

• **Lookups em memória** – SOURCE → `SourceLookup`, BI → `BIParamLookup`
  (ambos com cache + TTL de 10 min).

• **Nenhuma leitura extra** – todas as funções que antes abriam o Sheets
  diretamente foram refatoradas para receber o `fetcher` ou o `worksheet`
  já existente.

• **Um único write-back** por aba (além do Pinterest-dim que tem fluxo
  próprio), sempre reaproveitando `self.worksheet_origem`.
"""

from __future__ import annotations

import builtins
import logging
from typing import Callable, Dict, List, Optional

import pandas as pd
from gspread import Worksheet

from extract.sheets_fetcher import SheetsFetcher
from load.origin_writer import write_back_origin
from treat.utils.sheets_cache import get_worksheet
from treat.utils.datas import (
    converter_data,
    fill_missing_start_end_from_params,
)
from treat.utils.merges.pinterest.pinterest_dimension_pin_id_merge import (
    merge_pinterest_dimension as pin_merge,
)
from treat.platforms import dispatch
from treat.utils.preprocess_utils import (
    preprocess_origin,
    assign_vehicle_and_id,
    get_sibling_sheet,
)
from treat.utils.atribuicoes_via_lookup import SourceLookup
from treat.bi_param_utils import (
    BIParamLookup,
    enrich_with_bi_parametrizacao,
    fill_objective_from_bi,
)
from treat.utils.renomeacoes import (
    renomear_colunas_origem_para_modelo,
    aplicar_substituicoes_objetivo,
)
from treat.utils.campos_calculados import gerar_id, calcular_engajamento_total
from treat.utils.validations import (
    check_required_columns,
    validate_aggregates,
    validate_columns,
    validate_utm_content_in_bi,
    validate_taxonomy_consistency,
)

log = logging.getLogger(__name__)


class TreatPipeline:
    """Pipeline genérico que normaliza, enriquece, valida e grava uma aba."""

    def __init__(
        self,
        creds_path: str,
        spreadsheet_id: str,
        sheet_name: str,
        mapping_renomeacao: Dict[str, str],
        *,
        write_back: bool = True,
        subs_objetivo_fn: Callable[[pd.DataFrame], pd.DataFrame] = aplicar_substituicoes_objetivo,
    ) -> None:
        self.creds_path = creds_path
        self.spreadsheet_id = spreadsheet_id
        self.sheet_name = sheet_name
        self.write_back = write_back
        self.mapping = mapping_renomeacao
        self.subs_obj_fn = subs_objetivo_fn

        # BI lookup (cache + TTL)
        self.bi_lookup = BIParamLookup(creds_path, spreadsheet_id)

        # Fetcher único (reaproveita se já existir em builtins)
        self.fetcher: SheetsFetcher = getattr(builtins, "fetcher", None)  # type: ignore
        if self.fetcher is None:
            self.fetcher = SheetsFetcher(spreadsheet_id, creds_path)
            builtins.fetcher = self.fetcher

        # Worksheet de origem (apenas se vamos fazer write-back)
        self.worksheet_origem: Optional[Worksheet] = (
            get_worksheet(creds_path, spreadsheet_id, sheet_name) if write_back else None
        )

        # Cache interno para “abas irmãs” (ex.: pinterestGeral)
        self._sibling_cache: dict[str, pd.DataFrame] = {}

    def _preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        """Sanitiza dados brutos antes de qualquer tratamento específico."""
        df = preprocess_origin(
            df,
            worksheet=self.worksheet_origem,
            write_back=self.write_back,
        )

        # Remove linhas onde date/account_name/campaign_name estão vazias
        mandatory = ["date", "account_name", "campaign_name"]
        cols = [c for c in mandatory if c in df.columns]
        if cols:
            empty = df[cols].apply(
                lambda s: s.astype(str).str.strip().str.lower().isin(["", "nan"])
            )
            df = df.loc[~empty.all(axis=1)].reset_index(drop=True)

        return df

    def run(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """
        Executa o fluxo completo e devolve DataFrame já no layout destino.
        """
        lower = self.sheet_name.lower()

        # 1) Snapshot das colunas originais
        orig_cols: List[str] = df_raw.columns.tolist()
        df = df_raw.copy()

        # 2) Pré-processamento genérico (substituições + normalize_region)
        df = self._preprocess(df)

        # 3) Tratamento exclusivo de Pinterest dimensão demográfica
        if lower in {"pinterestgenero", "pinterestidade", "pinterestregiao"}:
            df_orig_to_write = df[orig_cols]
            validate_columns(df_orig_to_write, orig_cols, stage=f"Origem {self.sheet_name}")

            # Usa write_back_origin passando o Worksheet já obtido
            write_back_origin(
                df_raw=df_raw,
                df_ok=df_orig_to_write,
                creds_path=self.creds_path,
                spreadsheet_id=self.spreadsheet_id,
                write_back=self.write_back,
                dry_run=False,
                worksheet=self.worksheet_origem,
            )

            df_geral = get_sibling_sheet("pinterestGeral", self.fetcher, self._sibling_cache)
            return pin_merge(df_general=df_geral, df_dimension=df)

        # 4) Validações iniciais contra BI
        df_bi = self.bi_lookup.df()
        validate_utm_content_in_bi(df, df_bi)
        self._last_taxo_report = validate_taxonomy_consistency(df, df_bi)

        # 5) Enriquecimento BI + 'objective'
        df = enrich_with_bi_parametrizacao(
            df, self.creds_path, self.spreadsheet_id, sheet_name=self.sheet_name
        )
        df = fill_objective_from_bi(df, self.bi_lookup)

        # 6) Conversão de colunas datetime → date/start/end
        for col in ("date", "start", "end"):
            if col in df.columns:
                df = converter_data(df, col)

        # 7) Preenche start/end em memória (sem write-back aqui)
        df = fill_missing_start_end_from_params(df, lookup=self.bi_lookup, inplace=False)

        # 8) Atribuição de 'Veiculo' + 'ID_Veiculo'
        df = assign_vehicle_and_id(
            df,
            sheet_name=self.sheet_name,
            fetcher=self.fetcher,
            bi_lookup=self.bi_lookup,
        )

        # 9) Transformações específicas de cada plataforma
        df = dispatch(self.sheet_name)(df, lookup=self.bi_lookup)

        # 10) Substituições de objetivo customizadas
        df = self.subs_obj_fn(df)

        # 11) Validação de agregados
        validate_aggregates(df_raw, df)

        # 12) Checagem de colunas obrigatórias (zeros válidos)
        ZERO_OK = [
            "impressions", "cost", "link_clicks", "video_play",
            "video_watches_25", "video_watches_50", "video_watches_75",
            "video_watched_100", "post_reactions", "post_shares", "post_comments",
        ]
        check_required_columns(
            df, optional_cols=["URL_do_Anúncio"], zero_valid_cols=ZERO_OK
        )

        # 13) Write-back das correções na aba de origem (não-Pinterest dim)
        df_orig_to_write = df[orig_cols]
        validate_columns(df_orig_to_write, orig_cols, stage=f"Origem {self.sheet_name}")

        write_back_origin(
            df_raw=df_raw,
            df_ok=df_orig_to_write,
            creds_path=self.creds_path,
            spreadsheet_id=self.spreadsheet_id,
            sheet_name=self.sheet_name,
            write_back=self.write_back,
            dry_run=False,
        )

        # 14) Renomeia colunas para o modelo destino + cálculos finais
        df = renomear_colunas_origem_para_modelo(df, self.mapping)
        df = calcular_engajamento_total(df)
        df["ID"] = df.apply(gerar_id, axis=1)

        # 15) Logs finais de verificação
        if not df[df["Veiculo"].fillna("") == ""].empty:
            log.warning(
                "[%s] linhas sem Veiculo: %s",
                self.sheet_name,
                df.index[df["Veiculo"].fillna("") == ""].tolist(),
            )
        if not df[df["objective"].fillna("") == ""].empty:
            log.warning(
                "[%s] linhas sem objective: %s",
                self.sheet_name,
                df.index[df["objective"].fillna("") == ""].tolist(),
            )

        return df
