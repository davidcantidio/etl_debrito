from __future__ import annotations

import re
import logging
from typing import Dict, Callable

import pandas as pd
from treat.utils.merges.pinterest.pinterest_dimension_pin_id_merge import (
    merge_pinterest_dimension as pin_merge,
)

from treat.platforms import dispatch
from treat.utils.datas import converter_data     # <— ajustado para o módulo correto
from load.origin_writer import write_back_origin

from treat.utils.geo_normalize import obter_estado_de_regiao, carregar_caches_padrao
from treat.utils.atribuicoes_via_lookup import (
    atribuir_veiculo_e_id_meta,
    atribuir_veiculo_por_prefixo,
    PLATFORM_TO_VEICULO,
)
from treat.utils.substitute_origin_values import apply_all_origin_substitutions
from treat.utils.preprocess_utils import preprocess_origin
from .bi_param_utils import (
    BIParamLookup,
    enrich_with_bi_parametrizacao,
    fill_objective_from_bi,
)
from treat.utils.renomeacoes import (
    renomear_colunas_origem_para_modelo,
    aplicar_substituicoes_objetivo,
)
from treat.utils.campos_calculados import gerar_id, calcular_engajamento_total
from treat.utils.normalize import normalize_age, normalize_gender
from treat.utils.validations import (
    check_required_columns,
    validate_utm_content_in_bi,
    validate_aggregates,
    validate_taxonomy_consistency,
)

log = logging.getLogger(__name__)
CACHE_ESTADOS, CACHE_MUNICIPIOS = carregar_caches_padrao()


class TreatPipeline:
    """Pipeline genérico para tratar e carregar dados de uma aba de origem."""

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
        self._bi_lookup = BIParamLookup(creds_path, spreadsheet_id)

    def _preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        df = preprocess_origin(df)
        mandatory = ["date", "account_name", "campaign_name"]
        cols = [c for c in mandatory if c in df.columns]
        if cols:
            empty = df[cols] \
                .apply(lambda s: s.astype(str)
                                  .str.strip()
                                  .str.lower()
                                  .isin(["", "nan"]))
            df = df.loc[~empty.all(axis=1)].reset_index(drop=True)
        return df

    def _assign_vehicle_and_id(self, df: pd.DataFrame) -> pd.DataFrame:
        lower = self.sheet_name.lower()
        if lower.startswith("meta"):
            if "placement" in df.columns:
                return atribuir_veiculo_e_id_meta(df)
            return atribuir_veiculo_por_prefixo(df, "meta")
        prefix = next((k for k in PLATFORM_TO_VEICULO if lower.startswith(k)), None)
        prefix = prefix or re.match(r"[a-z]+", lower).group(0)
        return atribuir_veiculo_por_prefixo(df, prefix)

    def _fill_start_end(self, df: pd.DataFrame) -> pd.DataFrame:
        return self._bi_lookup.fill_missing_start_end_from_utm(
            df, sheet_name=self.sheet_name, write_back=self.write_back
        )

    def _enrich_bi(self, df: pd.DataFrame) -> pd.DataFrame:
        return enrich_with_bi_parametrizacao(
            df, self.creds_path, self.spreadsheet_id, sheet_name=self.sheet_name
        )
    # ------------------------------------------------------------------
    def _fetch_sibling_sheet(self, name: str) -> pd.DataFrame:
        """
        Busca no fetcher global já existente no notebook (ou faz 1 leitura
        extra caso ainda não esteja em cache).  Mantém independência do
        pipeline para uso fora do notebook.
        """
        try:
            from extract.sheets_fetcher import SheetsFetcher   # evite ciclo
            if hasattr(self, "_sibling_cache") and name in self._sibling_cache:
                return self._sibling_cache[name]

            # 1) tenta reaproveitar o fetcher global (notebook)
            import builtins
            fetcher = getattr(builtins, "fetcher", None)
            if isinstance(fetcher, SheetsFetcher):
                df = fetcher.get([name])[name]
            else:
                # 2) fallback: cria um fetcher pontual
                _tmp = SheetsFetcher(self.spreadsheet_id, self.creds_path)
                df = _tmp.get([name])[name]

            # cache interno
            self._sibling_cache = getattr(self, "_sibling_cache", {})
            self._sibling_cache[name] = df
            return df

        except Exception as e:
            raise RuntimeError(f"Falhou ao ler aba '{name}': {e}") from e

    def run(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        lower = self.sheet_name.lower()

        # 1) Pré-processo comum (substituições + normalize_region)
        df = self._preprocess(df_raw)

        # 2) Tratamento especial: Pinterest Idade/Gênero/Região
        if lower in {"pinterestgenero", "pinterestidade", "pinterestregiao"}:
            # 2a) grava o DF limpo (normalizado) na aba de origem
            write_back_origin(
                df_raw         = df_raw,
                df_ok          = df,
                creds_path     = self.creds_path,
                spreadsheet_id = self.spreadsheet_id,
                sheet_name     = self.sheet_name,
                write_back     = self.write_back,
                dry_run        = not self.write_back,
            )
            # 2b) executa só o merge demográfico e devolve o DataFrame de destino
            return pin_merge(
                df_general   = self._fetch_sibling_sheet("pinterestGeral"),
                df_dimension = df,
            )

        # 3) Validações iniciais contra BI
        df_bi = self._bi_lookup.df()
        validate_utm_content_in_bi(df, df_bi)
        self._last_taxo_report = validate_taxonomy_consistency(df, df_bi)

        # 4) Enriquecimento BI (campanha, start/end)
        df = self._enrich_bi(df)

        # 5) Conversão datetime → date/start/end
        for col in ("date", "start", "end"):
            if col in df.columns:
                df = converter_data(df, col)

        # 6) Atribuição de veículo + ID interno
        df = self._assign_vehicle_and_id(df)

        # 7) Transformações específicas de plataforma
        df = dispatch(self.sheet_name)(df, lookup=self._bi_lookup)

        # 8) Substituições de objetivo
        df = self.subs_obj_fn(df)

        # 9) Validação de agregados
        validate_aggregates(df_raw, df)

        # 10) Checagem de colunas obrigatórias (zero OK em algumas métricas)
        ZERO_OK = [
            "impressions", "cost", "link_clicks", "video_play",
            "video_watches_25", "video_watches_50", "video_watches_75",
            "video_watched_100", "post_reactions", "post_shares",
            "post_comments",
        ]
        check_required_columns(
            df,
            optional_cols=["URL_do_Anuncio"],
            zero_valid_cols=ZERO_OK,
        )

        # 11) Write-back de correções na aba de origem (somente para não-Pinterest dim)
        write_back_origin(
            df_raw        = df_raw,
            df_ok         = df,
            creds_path    = self.creds_path,
            spreadsheet_id= self.spreadsheet_id,
            sheet_name    = self.sheet_name,
            write_back    = self.write_back,
            dry_run       = False,
        )

        # 12) Renomeação para modelo destino
        df = renomear_colunas_origem_para_modelo(df, self.mapping)

        # 13) Cálculo de engajamento + geração de ID sintético
        df = calcular_engajamento_total(df)
        df["ID"] = df.apply(gerar_id, axis=1)

        return df