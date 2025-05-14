from __future__ import annotations
import re
import pandas as pd
from typing import Dict, Callable
import logging

from treat.utils.geo_normalize import (
    obter_estado_de_regiao,
    carregar_caches_padrao,
)
from .bi_param_utils import (
    BIParamLookup,
    enrich_with_bi_parametrizacao,
    fill_objective_from_bi
)
from .preprocess_utils import preprocess_origin
from treat.utils.write_back import write_back_df
from treat.utils.renomeacoes import (
    renomear_colunas_origem_para_modelo,
    aplicar_substituicoes_objetivo,
)
from treat.utils.campos_calculados import gerar_id, calcular_engajamento_total          
from treat.utils.normalize import normalize_age, normalize_gender
from treat.utils.validations import (check_required_columns,
                                     validate_utm_content_in_bi,
                                     validate_aggregates
)
from treat.utils.substitute_origin_values import apply_all_origin_substitutions

from treat.utils.atribuicoes_via_lookup import (
    atribuir_veiculo_e_id_meta,
    atribuir_veiculo_por_prefixo,
    PLATFORM_TO_VEICULO
    
)
from treat.utils.preview_links import (
    determine_meta_ad_preview_link,
    generate_tiktok_ad_preview_link,
    build_pinterest_preview_link,
    generate_linkedin_ad_preview_link_from_lookup,
)

log = logging.getLogger(__name__)

CACHE_ESTADOS, CACHE_MUNICIPIOS = carregar_caches_padrao()

class TreatPipeline:
    """Pipeline genérico que aplica **todas** as etapas de tratamento a uma aba
    de origem (Meta, TikTok, Pinterest, etc.).

    Parâmetros
    ----------
    creds_path      : caminho do JSON de credenciais do serviço
    spreadsheet_id  : ID da planilha (Google Sheets)
    sheet_name      : nome da aba de origem a ser tratada
    mapping_renomeacao : dict col_origem → col_modelo (específico da aba)
    write_back      : se True, grava as correções de volta na aba
    subs_objetivo_fn: função que aplica substituições de "objective"
                      (permite customizar por plataforma se necessário)
    """

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

        # lookup BI compartilhado durante toda a execução
        self._bi_lookup = BIParamLookup(creds_path, spreadsheet_id)

    # ───────────────────────────── helpers internos ────────────────────────── #
    def _preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        1) executa o pipeline padrão de origem;
        2) elimina linhas onde TODAS as colunas chave estão vazias
        (NaN, "", "nan", ou apenas espaços).
        """
        df = preprocess_origin(df)

        # --- remove linhas totalmente vazias --- #
        mandatory = ["date", "account_name", "campaign_name"]
        cols = [c for c in mandatory if c in df.columns]
        if cols:
            # Cria máscara TRUE se a célula é vazia/nula/"nan" (case-insensitive)
            empty = df[cols].apply(
                lambda s: s.astype(str).str.strip().str.lower().isin(["", "nan"])
            )
            # Mantém a linha se pelo menos UMA coluna mandatória não for vazia
            keep = ~empty.all(axis=1)
            df = df.loc[keep].reset_index(drop=True)

        return df


    def _assign_vehicle_and_id(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Atribui 'Veiculo' e 'ID_Veiculo' conforme a plataforma indicada por sheet_name:
        - Qualquer aba começando com 'meta': 
            • se existir 'placement' → inferência Meta (Facebook/Instagram etc.)
            • caso contrário → Veiculo = 'Meta'
        - Demais: prefixo da aba → atribuir_veiculo_por_prefixo (TikTok, LinkedIn...)
        """
        lower = self.sheet_name.lower()

        # 1) todas as abas meta*
        if lower.startswith("meta"):
            # tenta usar placement para detalhar Facebook/Instagram…
            if "placement" in df.columns:
                return atribuir_veiculo_e_id_meta(df)
            # sem placement, atribui plataforma genérica 'Meta' + ID_Veiculo
            return atribuir_veiculo_por_prefixo(df, "meta")

        # 2) outras plataformas: procura prefixo em PLATFORM_TO_VEICULO
        prefix = next(
            (k for k in PLATFORM_TO_VEICULO if lower.startswith(k)),
            None
        )
        if prefix is None:
            # fallback genérico
            prefix = re.match(r"[a-z]+", lower).group(0)
        return atribuir_veiculo_por_prefixo(df, prefix)


    def _fill_start_end(self, df: pd.DataFrame) -> pd.DataFrame:
        """Preenche start/end vazios via utm_content usando BI_PARAMETRIZAÇÃO."""
        return self._bi_lookup.fill_missing_start_end_from_utm(
            df, sheet_name=self.sheet_name, write_back=self.write_back
        )

    def _enrich_bi(self, df):
        return enrich_with_bi_parametrizacao(
            df, self.creds_path, self.spreadsheet_id,
            sheet_name=self.sheet_name
        )


    # ──────────────────────────── método principal ─────────────────────────── #

    def run(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """Executa o pipeline completo e devolve DataFrame padronizado."""
        # 1) pré-processamento + descartar linhas totalmente vazias
        df = self._preprocess(df_raw)
        mandatory = ["date", "account_name", "campaign_name"]
        subset   = [c for c in mandatory if c in df.columns]
        if subset:
            df = df.dropna(how="all", subset=subset).reset_index(drop=True)

        # 1.1) substituir valores de origem (aplica mappings, sem write-back)
        df = apply_all_origin_substitutions(
            df,
            sheet_name=self.sheet_name,
            write_back=False,
            inplace=True
        )

        # 1.2) validar utm_content contra BI_PARAMETRIZAÇÃO (pára se houver missings)
        df_param = self._bi_lookup._load_df()
        validate_utm_content_in_bi(df, df_param)

        # 2) completar start/end
        df = self._fill_start_end(df)

        # 3) enriquecimento BI (preenche utm_content, Campanha, ID_Campanha, start/end)
        df = enrich_with_bi_parametrizacao(
                                df, self.creds_path,
                                self.spreadsheet_id,
                                sheet_name=self.sheet_name
)



        # 3.1) preencher objective vazio a partir de BI_PARAMETRIZAÇÃO
        df = fill_objective_from_bi(df, self._bi_lookup, key_col="utm_content", objective_col="objective")


        # 4) normalizações opcionais
        if "age" in df.columns:
            df["age"] = df["age"].apply(normalize_age)
        if "region" in df.columns:
            df["region"] = df["region"].apply(
                lambda v: obter_estado_de_regiao(v, CACHE_MUNICIPIOS, CACHE_ESTADOS)
            )
        if "gender" in df.columns:
            df["gender"] = df["gender"].apply(normalize_gender)

        # 5) inferir Veiculo / ID_Veiculo
        df = self._assign_vehicle_and_id(df)

        # 6) gerar URL_do_Anuncio e, no LinkedIn, preencher ad_name
        lower = self.sheet_name.lower()
        if lower.startswith("meta"):
            if any(c.strip().lower() in ("preview_link_ig", "preview_link_fb") for c in df.columns):
                df = determine_meta_ad_preview_link(df)

        elif lower.startswith("tiktok"):
            df = generate_tiktok_ad_preview_link(df)

        elif lower.startswith("pinterest") and "pin_id" in df.columns:
            if "URL_do_Anuncio" not in df.columns:
                df["URL_do_Anuncio"] = ""
            df["URL_do_Anuncio"] = df["URL_do_Anuncio"].where(
                df["URL_do_Anuncio"].str.strip() != "",
                df["pin_id"].apply(build_pinterest_preview_link),
            )


        elif lower.startswith("linkedin"):
            # 1) Preview Link
            if "URL_do_Anuncio" not in df.columns:
                df["URL_do_Anuncio"] = ""
            preview_map = generate_linkedin_ad_preview_link_from_lookup(self._bi_lookup._load_df())
            if preview_map and "utm_content" in df.columns:
                df["URL_do_Anuncio"] = df["URL_do_Anuncio"].where(
                    df["URL_do_Anuncio"].str.strip() != "",
                    df["utm_content"].astype(str).map(preview_map).fillna(""),
                )

            # 2) Ad Name (LinkedIn não traz; preenche via utm_content)
            ad_name_map = self._bi_lookup.get_linkedin_ad_name_map()
            if ad_name_map and "utm_content" in df.columns:
                # garante que ad_name exista como Series
                if "ad_name" not in df.columns:
                    df["ad_name"] = ""
                # faz o where sobre a Series, não sobre uma string
                mask = df["ad_name"].astype(str).str.strip() != ""
                df["ad_name"] = df["ad_name"].where(
                    mask,
                    df["utm_content"].astype(str).map(ad_name_map).fillna(""),
                )

        # 6.3) validação de somatórios de impressões e custo
        validate_aggregates(df_raw, df)


        # 7) write-back (opcional)
        if self.write_back:
            write_back_df(df, self.creds_path, self.spreadsheet_id, self.sheet_name)

        # 8) renomear colunas
        df = renomear_colunas_origem_para_modelo(df, self.mapping)

        # 9) substituir valores de objective
        df = self.subs_obj_fn(df)

        # 10) gerar Engajamento_Total e ID final
        df = calcular_engajamento_total(df)
        df["ID"] = df.apply(gerar_id, axis=1)

        # 11) validação final de colunas obrigatórias
        ZERO_OK_METRICS = [
            "imrpessions", "cost", "link_clicks", "video_play",
            "video_watches_25", "video_watches_50", "video_watches_75",
            "video_watches_100", "post_reactions", "post_shares",
            "post_comments"
        ]
        check_required_columns(
            df,
            optional_cols=["URL_do_Anuncio"],
            zero_valid_cols=ZERO_OK_METRICS
        )

        return df
