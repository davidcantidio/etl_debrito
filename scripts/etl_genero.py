import logging
from scripts.etl_geral import BaseGeralETL
from utils.fields_lists import GENDER_MODEL_COLUMN_ORDER
from utils.organizar_dataframe import reordenar_colunas_para_modelo
from utils.normalize import normalizar_genero
from utils.atribuicoes_via_lookup import atribuir_veiculo_e_id_meta
from utils.common_pinterest import preencher_campos_com_campanha
from utils.datas import generate_pinterest_dates
from utils.numeracao import gerar_numeracao

# Importa as funções de transformação de dados do módulo common_meta
from utils.common_meta import (
    load_and_prepare_meta_gender_data,
    load_and_prepare_meta_placement_data,
    pivot_meta_gender_data,
    pivot_meta_placement_data,
    merge_placement_and_gender_data,
    distribute_gender_metrics
)
# Importa a função que preserva a coluna "Placement"
from utils.common_meta import preserve_placement_column

class BaseGeneroETL(BaseGeralETL):
    """
    Classe base para ETLs de Gênero.
    """
    def ajustar_tipos_e_calculos(self):
        logging.debug(">>> In BaseGeneroETL.ajustar_tipos_e_calculos (Antes do super)")
        super().ajustar_tipos_e_calculos()
        logging.debug(f"[DEBUG] Colunas após super().ajustar_tipos_e_calculos: {list(self.df.columns)}")
        
        # Renomeia 'Gender' para 'Genero', se existir
        if 'Gender' in self.df.columns:
            logging.debug("[DEBUG] Renomeando 'Gender' -> 'Genero'")
            self.df.rename(columns={'Gender': 'Genero'}, inplace=True)
        
        # Aplica normalização no campo 'Genero'
        if 'Genero' in self.df.columns:
            logging.debug("[DEBUG] Normalizando campo 'Genero'")
            self.df['Genero'] = self.df['Genero'].apply(normalizar_genero)
        
        return self.df

    def criar_veiculo(self):
        logging.debug(">>> In BaseGeneroETL.criar_veiculo (Chamando super para atribuir Veiculo e ID)")
        super().criar_veiculo()
        return self.df

    def reordenar_colunas_para_modelo(self):
        logging.debug(">>> In BaseGeneroETL.reordenar_colunas_para_modelo")
        self.df = reordenar_colunas_para_modelo(self.df, GENDER_MODEL_COLUMN_ORDER)
        return self.df

    def processar(self, df_destino=None):
        logging.debug(">>> In processar (BaseGeneroETL)")
        
        # Etapas padrão do ETL
        self.renomear_colunas_origem_para_modelo()
        logging.debug(f"[DEBUG] Colunas após renomear_colunas_origem_para_modelo: {list(self.df.columns)}")
        
        self.ajustar_tipos_e_calculos()
        logging.debug(f"[DEBUG] Colunas após ajustar_tipos_e_calculos: {list(self.df.columns)}")
        
        self.aplicar_substituicoes_objetivo()
        logging.debug(f"[DEBUG] Colunas após aplicar_substituicoes_objetivo: {list(self.df.columns)}")
        
        self.aplicar_parametrizacao_campanha_externa()
        logging.debug(f"[DEBUG] Colunas após aplicar_parametrizacao_campanha_externa: {list(self.df.columns)}")
        
        self.determine_ad_preview_link()
        logging.debug(f"[DEBUG] Colunas após determine_ad_preview_link: {list(self.df.columns)}")
        
        self.criar_veiculo()
        logging.debug(f"[DEBUG] Colunas após criar_veiculo: {list(self.df.columns)}")
        
        self.remover_colunas_indesejadas()
        logging.debug(f"[DEBUG] Colunas após remover_colunas_indesejadas: {list(self.df.columns)}")
        
        self.reordenar_colunas_para_modelo()
        logging.debug(f"[DEBUG] Colunas após reordenar_colunas_para_modelo: {list(self.df.columns)}")
        
        self.gerar_id()
        logging.debug(f"[DEBUG] Colunas após gerar_id: {list(self.df.columns)}")
        
        self.df = gerar_numeracao(self.df, df_destino, linha_insercao=2, coluna='Numero')
        logging.debug(">>> Final do processar (BaseGeneroETL)")
        
        return self.df


class MetaGeneroETL(BaseGeneroETL):
    def processar(self, df_destino=None):
        logging.info("Starting MetaGeneroETL processing for Meta...")

        # Carrega os dados brutos das duas abas utilizando as funções de common_meta
        df_gender = load_and_prepare_meta_gender_data()
        df_placement = load_and_prepare_meta_placement_data()
        logging.debug(f"metaGenero loaded with shape: {df_gender.shape}")
        logging.debug(f"metaGeral loaded with shape: {df_placement.shape}")

        # Aplica os pivôs para cada conjunto de dados
        df_gender_pivot = pivot_meta_gender_data(df_gender)
        df_placement_pivot = pivot_meta_placement_data(df_placement)
        logging.debug(f"Pivot metaGenero shape: {df_gender_pivot.shape}")
        logging.debug(f"Pivot metaGeral (placement) shape: {df_placement_pivot.shape}")

        # Realiza o merge dos dois DataFrames pivotados utilizando "Ad ID" e "Date"
        merged_df = merge_placement_and_gender_data(df_placement_pivot, df_gender_pivot)
        logging.debug(f"Merged data shape: {merged_df.shape}")

        # Distribui os valores de gênero para cada plataforma (resultando em uma linha por plataforma)
        distributed_df = distribute_gender_metrics(merged_df)
        logging.debug(f"Distributed data shape: {distributed_df.shape}")

        # Preserve a coluna "Placement" copiando os valores de "_Plataforma" para "Placement"
        distributed_df = preserve_placement_column(distributed_df)
        logging.debug(f"Data shape after preserving Placement: {distributed_df.shape}")

        # Atualiza o DataFrame interno com o resultado final
        self.df = distributed_df.copy()

        # Executa as demais etapas do pipeline comum do ETL
        self.renomear_colunas_origem_para_modelo()
        logging.debug(f"[DEBUG] Colunas após renomear_colunas_origem_para_modelo: {list(self.df.columns)}")
        
        self.ajustar_tipos_e_calculos()
        logging.debug(f"[DEBUG] Colunas após ajustar_tipos_e_calculos: {list(self.df.columns)}")
        
        self.aplicar_substituicoes_objetivo()
        logging.debug(f"[DEBUG] Colunas após aplicar_substituicoes_objetivo: {list(self.df.columns)}")
        
        self.aplicar_parametrizacao_campanha_externa()
        logging.debug(f"[DEBUG] Colunas após aplicar_parametrizacao_campanha_externa: {list(self.df.columns)}")
        
        self.determine_ad_preview_link()
        logging.debug(f"[DEBUG] Colunas após determine_ad_preview_link: {list(self.df.columns)}")
        
        self.criar_veiculo()
        logging.debug(f"[DEBUG] Colunas após criar_veiculo: {list(self.df.columns)}")
        
        self.remover_colunas_indesejadas()
        logging.debug(f"[DEBUG] Colunas após remover_colunas_indesejadas: {list(self.df.columns)}")
        
        self.reordenar_colunas_para_modelo()
        logging.debug(f"[DEBUG] Colunas após reordenar_colunas_para_modelo: {list(self.df.columns)}")
        
        self.gerar_id()
        logging.debug(f"[DEBUG] Colunas após gerar_id: {list(self.df.columns)}")
        
        self.df = gerar_numeracao(self.df, df_destino, linha_insercao=2, coluna='Numero')
        logging.info("MetaGeneroETL processing finished.")

        return self.df

    def criar_veiculo(self):
        logging.debug(">>> In MetaGeneroETL.criar_veiculo (Chamando atribuir_veiculo_e_id_meta)")
        self.df = atribuir_veiculo_e_id_meta(self.df)
        return self.df


class TikTokGeneroETL(BaseGeneroETL):
    pass


class LinkedinGeneroETL(BaseGeneroETL):
    pass


class PinterestGeneroETL(BaseGeneroETL):
    """
    Subclasse para tratar peculiaridades do Pinterest na dimensão Gênero.
    A principal diferença é que precisamos gerar datas específicas do Pinterest
    e preencher os campos Nome_do_Conjunto_de_Anuncio / Nome_do_Anuncio utilizando 'Campaign name'
    (mantendo 'Campanha' e 'ID_Campanha' vindos do lookup).
    """
    def ajustar_tipos_e_calculos(self):
        logging.debug(">>> In PinterestGeneroETL.ajustar_tipos_e_calculos (Antes de super)")
        super().ajustar_tipos_e_calculos()
        logging.debug("[DEBUG] Gerando datas do Pinterest")
        self.df = generate_pinterest_dates(self.df)
        logging.debug(f"[DEBUG] Colunas após generate_pinterest_dates: {list(self.df.columns)}")
        logging.debug("[DEBUG] Aplicando preencher_campos_com_campanha para Nome_do_Conjunto_de_Anuncio e Nome_do_Anuncio")
        self.df = preencher_campos_com_campanha(self.df)
        logging.debug(f"[DEBUG] Colunas após preencher_campos_com_campanha: {list(self.df.columns)}")
        return self.df
