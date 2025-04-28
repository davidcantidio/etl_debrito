import logging
from utils.fields_lists import GENERAL_MODEL_COLUMN_ORDER, NUMERIC_COLUMNS
from utils.substitutions_lists import ID_CONTENT_REPLACEMENTS
from utils.utm_lookup import load_utm_mapping, fill_missing_start_end_from_utm
from utils.setup_logging import setup_logging

setup_logging(level=logging.DEBUG, log_file='etl_geral.log')

from utils import (
    aplicar_substituicoes_objetivo,
    renomear_colunas_origem_para_modelo,
)
from utils.numeracao import gerar_numeracao
from utils import converter_data
from utils import determine_meta_ad_preview_link
from utils.common_linkedin import preencher_nomes_anuncio_linkedin
from utils.atribuicoes_via_lookup import (
    atribuir_veiculo_e_id_meta,
    atribuir_id_veiculo_generico,
    aplicar_parametrizacao_campanha,
)
from utils.campos_calculados import (
    calcular_engajamento_total,
    inicializar_colunas_auxiliares,
    gerar_id,
)
from utils.normalize import convert_numeric_columns
from utils.organizar_dataframe import (
    remover_colunas_indesejadas,
    reordenar_colunas_para_modelo,
)
from utils.datas import generate_pinterest_dates
from utils.preview_links import generate_pinterest_ad_preview_link


class BaseGeralETL:
    """
    Base pipeline for general ETL, now applying arbitrary ID_Content replacements
    for ALL platforms (not just LinkedIn), plus UTM-based Start/End lookup.
    """
    def __init__(self, df, id_veiculo, veiculo, mapping_campanha=None, mapping_sigla=None):
        logging.debug(">>> Initializing BaseGeralETL")
        self.df = df.copy()
        self.id_veiculo = id_veiculo
        self.veiculo = veiculo
        self.mapping_campanha = mapping_campanha or {}
        self.mapping_sigla = mapping_sigla or {}
        self.utm_mapping = load_utm_mapping()

    def renomear_colunas_origem_para_modelo(self):
        self.df = renomear_colunas_origem_para_modelo(self.df)

    def ajustar_tipos_e_calculos(self):
        logging.debug("Filling missing Start/End from UTM mapping")
        self.df = fill_missing_start_end_from_utm(self.df, self.utm_mapping)
        logging.debug("Converting date fields")
        self.df = converter_data(self.df, 'Data')
        self.df = converter_data(self.df, 'Inicio_da_Campanha')
        self.df = converter_data(self.df, 'Fim_da_Campanha')
        logging.debug(f"Converting numeric columns: {NUMERIC_COLUMNS}")
        self.df = convert_numeric_columns(self.df, NUMERIC_COLUMNS)
        logging.debug("Calculating engagement and initializing auxiliaries")
        self.df = calcular_engajamento_total(self.df)
        self.df = inicializar_colunas_auxiliares(self.df)

    def aplicar_substituicoes_objetivo(self):
        self.df = aplicar_substituicoes_objetivo(self.df)

    def aplicar_parametrizacao_campanha_externa(self):
        self.df = aplicar_parametrizacao_campanha(
            self.df, self.mapping_campanha, self.mapping_sigla
        )

    def criar_veiculo(self):
        logging.debug("Assigning generic vehicle")
        self.df['Veiculo'] = self.veiculo
        self.df = atribuir_id_veiculo_generico(self.df)

    def remover_colunas_indesejadas(self):
        self.df = remover_colunas_indesejadas(self.df)

    def reordenar_colunas_para_modelo(self):
        self.df = reordenar_colunas_para_modelo(self.df, GENERAL_MODEL_COLUMN_ORDER)

    def gerar_id(self):
        self.df = gerar_id(self.df)

    def processar(self, df_destino=None):
        logging.debug(">>> Starting BaseGeralETL.processar()")

        # 1) Renomeia colunas de origem para o modelo
        self.renomear_colunas_origem_para_modelo()

        # 1.1) Elimina eventuais colunas duplicadas (criada por pré-fill)
        self.df = self.df.loc[:, ~self.df.columns.duplicated()]

        # 2) Ajusta tipos, preenche datas faltantes, converte numéricos, etc.
        self.ajustar_tipos_e_calculos()

        # 3) Continua pipeline padrão
        self.aplicar_substituicoes_objetivo()
        self.aplicar_parametrizacao_campanha_externa()
        self.determine_ad_preview_link()
        self.criar_veiculo()
        self.remover_colunas_indesejadas()
        self.reordenar_colunas_para_modelo()
        self.gerar_id()

        # 4) Numera e retorna
        self.df = gerar_numeracao(self.df, df_destino, linha_insercao=2, coluna='Numero')
        return self.df

    def determine_ad_preview_link(self):
        pass


class MetaGeralETL(BaseGeralETL):
    def determine_ad_preview_link(self):
        self.df = determine_meta_ad_preview_link(self.df)

    def criar_veiculo(self):
        logging.debug(">>> In MetaGeralETL.criar_veiculo")
        self.df = atribuir_veiculo_e_id_meta(self.df)


class LinkedinGeralETL(BaseGeralETL):
    def __init__(self, *args, mapping_preview=None, mapping_criativo=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.mapping_preview = mapping_preview or {}
        self.mapping_criativo = mapping_criativo or {}

    def ajustar_tipos_e_calculos(self):
        super().ajustar_tipos_e_calculos()
        # Duplica Content (utm) para ID_Content e utm_content
        if "Content (utm)" in self.df.columns:
            self.df["ID_Content"] = self.df["Content (utm)"]
            self.df["utm_content"] = self.df["Content (utm)"]
            logging.debug("[LinkedIn] Campo 'Content (utm)' duplicado em 'ID_Content' e 'utm_content'.")
        else:
            logging.warning("[LinkedIn] Coluna 'Content (utm)' ausente; não será possível gerar 'ID_Content' nem 'utm_content'.")
        # Preenche nomes de anúncio via mapeamento
        self.df = preencher_nomes_anuncio_linkedin(self.df, self.mapping_criativo)
        return self.df

    def determine_ad_preview_link(self):
        logging.debug(">>> In LinkedinGeralETL.determine_ad_preview_link (via ID_Content)")
        if "ID_Content" not in self.df.columns:
            logging.warning("[LinkedIn] Coluna 'ID_Content' ausente; não será possível gerar 'URL_do_Anuncio'.")
            return
        self.df["URL_do_Anuncio"] = self.df["ID_Content"].map(self.mapping_preview)
        nao_mapeados = self.df[self.df["URL_do_Anuncio"].isna()]["ID_Content"].dropna().unique()
        if len(nao_mapeados) > 0:
            logging.warning(f"[LinkedIn] ID_Content sem preview (até 10): {nao_mapeados[:10]}")


class PinterestGeralETL(BaseGeralETL):
    def ajustar_tipos_e_calculos(self):
        super().ajustar_tipos_e_calculos()
        self.df = generate_pinterest_dates(self.df)
        self.df = generate_pinterest_ad_preview_link(self.df)


class TiktokGeralETL(BaseGeralETL):
    def ajustes_preview(self):
        logging.debug(">>> In TiktokGeralETL.ajustes_preview")
        logging.debug(
            f"Inicio_da_Campanha: {self.df.get('Inicio_da_Campanha', '').head(3)}"
        )
        logging.debug(
            f"Fim_da_Campanha: {self.df.get('Fim_da_Campanha', '').head(3)}"
        )
