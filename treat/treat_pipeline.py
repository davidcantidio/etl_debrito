from __future__ import annotations


import re
import logging
from typing import Dict, Callable

import pandas as pd

from utils import converter_data
from load.origin_writer import write_back_origin

from treat.utils.geo_normalize import obter_estado_de_regiao, carregar_caches_padrao
from treat.utils.atribuicoes_via_lookup import (
    atribuir_veiculo_e_id_meta,
    atribuir_veiculo_por_prefixo,
    PLATFORM_TO_VEICULO
)
from treat.utils.substitute_origin_values import apply_all_origin_substitutions
from treat.utils.preprocess_utils import preprocess_origin
from .bi_param_utils import (
    BIParamLookup,
    enrich_with_bi_parametrizacao,
    fill_objective_from_bi
)
from treat.utils.renomeacoes import renomear_colunas_origem_para_modelo, aplicar_substituicoes_objetivo
from treat.utils.campos_calculados import gerar_id, calcular_engajamento_total
from treat.utils.normalize import normalize_age, normalize_gender
from treat.utils.validations import (
    check_required_columns,
    validate_utm_content_in_bi,
    validate_aggregates,
    validate_taxonomy_consistency
)

from treat.utils.preview_links import (
    determine_meta_ad_preview_link,
    generate_tiktok_ad_preview_link,
    build_pinterest_preview_link,
    generate_linkedin_ad_preview_link_from_lookup
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
            empty = df[cols].apply(lambda s: s.astype(str).str.strip().str.lower().isin(["", "nan"]))
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

    def run(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        # 1) Pré-process
        df = self._preprocess(df_raw)
        mandatory = ["date", "account_name", "campaign_name"]
        subset = [c for c in mandatory if c in df.columns]
        if subset:
            df = df.dropna(how="all", subset=subset).reset_index(drop=True)

        # 2) Substituições de origem
        df = apply_all_origin_substitutions(
            df,
            sheet_name=self.sheet_name,
            write_back=False,
            inplace=True
        )

        # 3) Validação UTM_CONTENT
        df_param = self._bi_lookup.df()
        validate_utm_content_in_bi(df, df_param)

        # Validação de consistência de taxonomia (camp/ad/adgroup/utm)
        self._last_taxo_report = validate_taxonomy_consistency(
        df_ok=df,
        df_bi=self._bi_lookup.df()
    )



        # 4) Preencher start/end via utm_content
        df = self._fill_start_end(df)

        # ── Converte colunas datetime → date puro ─────────────────────────── 
        for coluna in ("date", "start", "end"):
            if coluna in df.columns:
                df = converter_data(df, coluna)

        # 5) Enriquecer com BI_PARAMETRIZAÇÃO
        df = self._enrich_bi(df)

        # 5.1) Fallback quando utm_content está vazio (ex.: LinkedIn)
        camp_map, id_map = self._bi_lookup.get_campaign_maps(
            prefer_cols=("utm_content", "taxonomy_campaign_name")
        )
        # chave normalizeda de campaign_name
        camp_key = df["campaign_name"].astype(str).str.strip().str.lower()

        # preencher Campanha / ID_Campanha onde ainda vazio
        mask_missing = df["Campanha"].astype(str).str.strip() == ""
        if mask_missing.any():
            df.loc[mask_missing, "Campanha"] = (
                camp_key[mask_missing]
                .map(camp_map)
                .fillna("")
            )
            df.loc[mask_missing, "ID_Campanha"] = (
                camp_key[mask_missing]
                .map(id_map)
                .fillna("")
            )

        # 5.2) Fallback de start/end via campaign_name
        df_param = self._bi_lookup.df()
        df_param["key"] = (
            df_param["taxonomy_campaign_name"]
            .astype(str).str.strip().str.lower()
        )
        start_map = df_param.set_index("key")["start"].to_dict()
        end_map   = df_param.set_index("key")["end"].to_dict()

        if "start" in df.columns:
            miss = df["start"].astype(str).str.strip() == ""
            df.loc[miss, "start"] = camp_key[miss].map(start_map)

        if "end" in df.columns:
            miss = df["end"].astype(str).str.strip() == ""
            df.loc[miss, "end"] = camp_key[miss].map(end_map)

        # 6) preencher objective vazio a partir da BI
        df = fill_objective_from_bi(
            df,
            self._bi_lookup,
            key_col="utm_content",
            objective_col="objective"
        )

        # 7) Normalizações extras
        if "age" in df.columns:
            df["age"] = df["age"].apply(normalize_age)
        if "region" in df.columns:
            df["region"] = df["region"].apply(
                lambda v: obter_estado_de_regiao(v, CACHE_MUNICIPIOS, CACHE_ESTADOS)
            )
        if "gender" in df.columns:
            df["gender"] = df["gender"].apply(normalize_gender)

        # 8) Atribuir Veiculo e ID_Veiculo
        df = self._assign_vehicle_and_id(df)

        # 9) Gerar preview links e ad_name
        lower = self.sheet_name.lower()
        if lower.startswith("meta") and any(
            c.strip().lower() in ("preview_link_ig", "preview_link_fb")
            for c in df.columns
        ):
            df = determine_meta_ad_preview_link(df)
        elif lower.startswith("tiktok"):
            df = generate_tiktok_ad_preview_link(df)
        elif lower.startswith("pinterest") and "pin_id" in df.columns:
            df["URL_do_Anuncio"] = df.get("URL_do_Anuncio", "")
            df["URL_do_Anuncio"] = df["URL_do_Anuncio"].where(
                df["URL_do_Anuncio"].str.strip() != "",
                df["pin_id"].apply(build_pinterest_preview_link),
            )
        elif lower.startswith("linkedin"):
            df["URL_do_Anuncio"] = df.get("URL_do_Anuncio", "")
            preview_map = generate_linkedin_ad_preview_link_from_lookup(
                self._bi_lookup.df()
            )
            if preview_map and "utm_content" in df.columns:
                df["URL_do_Anuncio"] = df["URL_do_Anuncio"].where(
                    df["URL_do_Anuncio"].str.strip() != "",
                    df["utm_content"].astype(str).map(preview_map).fillna(""),
                )
                # ---- ad_name via lookup (sempre sobrescreve) ------------------------
            ad_name_map = self._bi_lookup.get_linkedin_ad_name_map()
            # se existir o mapa e utm_content, então sobrescreve ad_name
            if ad_name_map and "utm_content" in df.columns:
                df["ad_name"] = (
                    df["utm_content"]
                    .astype(str)
                    .str.strip()
                    .map(ad_name_map)
                    .fillna(df.get("ad_name", ""))
                )
        # ── 9.2) Para LinkedIn, garantir que ad_group_name seja sempre igual a ad_name
            # (insira logo após o bloco de ad_name_map)
            df["ad_group_name"] = df["ad_name"]

            


        # 10) substituir valores de objective
        df = self.subs_obj_fn(df)

        # 11) validação de somatórios de impressões e custo
        validate_aggregates(df_raw, df)

        # 12) validação final de colunas obrigatórias
        ZERO_OK_METRICS = [
            "impressions", "cost", "link_clicks", "video_play",
            "video_watches_25", "video_watches_50", "video_watches_75",
            "video_watches_100", "post_reactions", "post_shares",
            "post_comments"
        ]
        check_required_columns(
            df,
            optional_cols=["URL_do_Anuncio"],
            zero_valid_cols=ZERO_OK_METRICS
        )

        # 13) Write-back na aba de origem
        write_back_origin(
            df_raw=df_raw,
            df_ok=df,
            creds_path=self.creds_path,
            spreadsheet_id=self.spreadsheet_id,
            sheet_name=self.sheet_name,
            write_back=self.write_back,
            dry_run=False
        )

        # 14) Renomear colunas para modelo
        df = renomear_colunas_origem_para_modelo(df, self.mapping)

        # 15) Calcular Engajamento_Total e ID final
        df = calcular_engajamento_total(df)
        df["ID"] = df.apply(gerar_id, axis=1)

        return df
