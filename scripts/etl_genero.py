import logging
from scripts.etl_geral import BaseGeralETL
from utils.fields_lists import GENDER_MODEL_COLUMN_ORDER
from utils.organizar_dataframe import reordenar_colunas_para_modelo
from utils.normalize import normalizar_genero
from utils.atribuicoes_via_lookup import (
    atribuir_veiculo_e_id_meta,
    aplicar_parametrizacao_campanha
)
from utils.common_pinterest import preencher_campos_com_campanha
from utils.datas import generate_pinterest_dates
from utils.numeracao import gerar_numeracao
from utils.common_meta import (
    load_and_prepare_meta_gender_data,
    load_and_prepare_meta_placement_data,
    pivot_meta_gender_data,
    pivot_meta_placement_data,
    merge_placement_and_gender_data,
    preserve_placement_column,
)
import pandas as pd

# Distribui métricas de gênero por plataforma
def distribute_gender_metrics(df_merged):
    """
    Gera linhas separadas para cada plataforma e distribui as métricas (Impressions, Cost, etc.)
    proporcionalmente às colunas pivotadas por plataforma.
    """
    logging.debug("Iniciando distribute_gender_metrics()")
    metrics = ["Impressions", "Link clicks", "Cost", "Video watches at 100%"]

    # Identificar todas as 'plataformas' a partir das colunas do pivot (padrão: "<plataforma>_Impressions")
    placement_cols = [col for col in df_merged.columns if col.endswith("_Impressions")]
    placements = list({col.rsplit("_", 1)[0] for col in placement_cols})
    logging.debug(f"Plataformas identificadas: {placements}")

    output_rows = []

    def base_columns(row):
        # Devolve as colunas que não sejam métricas pivotadas (ex.: sem suffix "_Cost", etc.)
        return [col for col in row.index if not any(col.endswith(f"_{m}") for m in metrics)]

    for idx, row in df_merged.iterrows():
        row_dict = row[base_columns(row)].to_dict()

        # Soma total de cada métrica em TODAS as plataformas
        total_by_metric = {}
        for metric in metrics:
            total_by_metric[metric] = sum(row.get(f"{pl}_{metric}", 0) for pl in placements)

        # Para cada plataforma, calcula valor proporcional e gera nova linha
        for platform in placements:
            new_row = row_dict.copy()
            new_row["_Plataforma"] = platform

            for metric in metrics:
                col_name = f"{platform}_{metric}"
                total_global = total_by_metric[metric]
                value = row.get(col_name, 0)
                # Se não há total ou total_global == 0, atribuir 0. Caso contrário, faz proporcional.
                new_row[metric] = round(row[metric] * value / total_global) if total_global > 0 else 0

            output_rows.append(new_row)

    output_df = pd.DataFrame(output_rows)
    logging.debug(f"Distribuição concluída. Formato final: {output_df.shape}")
    return output_df


def custom_reordenar_colunas_para_modelo(df, column_order):
    """
    Reordena colunas de acordo com column_order,
    preservando no final quaisquer colunas adicionais que não
    estejam explicitamente na lista.
    """
    logging.debug("Iniciando custom_reordenar_colunas_para_modelo")
    colunas_existentes = [col for col in column_order if col in df.columns]
    colunas_adicionais = [col for col in df.columns if col not in colunas_existentes]
    nova_ordem = colunas_existentes + colunas_adicionais
    logging.debug("Nova ordem de colunas: %s", nova_ordem)
    return df[nova_ordem]


def preencher_dimensoes_meta_genero(df):
    """
    Copia colunas de dimensionais do Facebook/Instagram
    para nomes padronizados do modelo.
    """
    logging.debug("Mapeando colunas dimensionais para o modelo (Meta).")
    if 'Account name' in df.columns:
        df['Nome_da_Conta'] = df['Account name']
        logging.debug("Mapeado: 'Account name' -> 'Nome_da_Conta'")
    if 'Ad group name' in df.columns:
        df['Nome_do_Conjunto_de_Anuncio'] = df['Ad group name']
        logging.debug("Mapeado: 'Ad group name' -> 'Nome_do_Conjunto_de_Anuncio'")
    if 'Ad name' in df.columns:
        df['Nome_do_Anuncio'] = df['Ad name']
        logging.debug("Mapeado: 'Ad name' -> 'Nome_do_Anuncio'")
    if 'Campaign objective type' in df.columns:
        df['Objetivo'] = df['Campaign objective type']
        logging.debug("Mapeado: 'Campaign objective type' -> 'Objetivo'")
    if 'Campaign name' in df.columns:
        df['Campanha_Lookup'] = df['Campaign name']
        df['Campanha'] = df['Campaign name']
        logging.debug("Mapeado: 'Campaign name' -> 'Campanha' e 'Campanha_Lookup'")

    return df


class BaseGeneroETL(BaseGeralETL):
    """
    Classe base para ETLs de Gênero, herdando de BaseGeralETL.
    """

    def ajustar_tipos_e_calculos(self):
        logging.debug(">>> In BaseGeneroETL.ajustar_tipos_e_calculos (Antes do super)")
        super().ajustar_tipos_e_calculos()

        # Se existe a coluna 'Gender', renomeia para 'Genero'
        if 'Gender' in self.df.columns:
            logging.debug("Renomeando 'Gender' para 'Genero'")
            self.df.rename(columns={'Gender': 'Genero'}, inplace=True)

        # Normaliza valores ('male', 'female', etc.)
        if 'Genero' in self.df.columns:
            logging.debug("Normalizando 'Genero'")
            self.df['Genero'] = self.df['Genero'].apply(normalizar_genero)

        return self.df

    def criar_veiculo(self):
        logging.debug(">>> In BaseGeneroETL.criar_veiculo - Chamando super() para atribuir Veiculo e ID")
        super().criar_veiculo()
        return self.df

    def reordenar_colunas_para_modelo(self):
        logging.debug("Reordenando colunas conforme GENDER_MODEL_COLUMN_ORDER")
        self.df = custom_reordenar_colunas_para_modelo(self.df, GENDER_MODEL_COLUMN_ORDER)
        return self.df


class MetaGeneroETL(BaseGeneroETL):
    """
    ETL de Gênero para dados do Meta (Facebook/Instagram),
    que carrega dados de duas abas (metaGenero + metaGeral) e faz merge/pivôs.
    """

    def processar(self, df_destino=None):
        logging.info("Iniciando processamento do MetaGeneroETL para Meta...")

        # 1) Carrega dados brutos das abas metaGenero e metaGeral
        logging.debug("Starting load_and_prepare_meta_gender_data()")
        df_gender = load_and_prepare_meta_gender_data()
        logging.debug("Starting load_and_prepare_meta_placement_data()")
        df_placement = load_and_prepare_meta_placement_data()
        logging.debug(f"Dados metaGenero carregados, shape: {df_gender.shape}")
        logging.debug(f"Dados metaGeral carregados, shape: {df_placement.shape}")

        # 2) Pivot de cada DataFrame
        logging.debug("Starting pivot_meta_gender_data()")
        df_gender_pivot = pivot_meta_gender_data(df_gender)
        logging.debug("Starting pivot_meta_placement_data()")
        df_placement_pivot = pivot_meta_placement_data(df_placement)
        logging.debug(f"Pivot da aba metaGenero, shape: {df_gender_pivot.shape}")
        logging.debug(f"Pivot da aba metaGeral, shape: {df_placement_pivot.shape}")

        # 3) Merge de placements e gender
        logging.debug("Starting merge_placement_and_gender_data()")
        merged_df = merge_placement_and_gender_data(df_placement_pivot, df_gender_pivot)
        logging.debug(f"Merge final (placement + gender), shape: {merged_df.shape}")

        # 4) Enriquecer o DataFrame com dimensões extras
        dims = ["Ad ID", "Account name", "Ad group name", "Ad name", "Campaign name", "Campaign objective type"]
        df_dims = df_placement[dims].drop_duplicates(subset=["Ad ID"])
        merged_df = merged_df.merge(df_dims, on="Ad ID", how="left")
        logging.debug("Merge com dados dimensionais concluído.")

        # 5) Mapeia colunas dimensionais para o nosso modelo
        merged_df = preencher_dimensoes_meta_genero(merged_df)

        # 6) Distribui métricas de gênero por plataforma
        distributed_df = distribute_gender_metrics(merged_df)

        # 7) Preserva a coluna 'Placement'
        distributed_df = preserve_placement_column(distributed_df)
        logging.debug(f"Distribuição de métricas concluída, shape: {distributed_df.shape}")

        # Define no self.df para as etapas subsequentes
        self.df = distributed_df.copy()

        # 8) Renomear colunas para ficar no padrão do modelo
        self.df.rename(columns={
            "Impressions": "Impressoes",
            "Link clicks": "Cliques_no_Link",
            "Cost": "Investimento",
            "Video watches at 100%": "Visualizacoes_ate_100",
        }, inplace=True)

        if "Date" in self.df.columns:
            self.df.rename(columns={"Date": "Data"}, inplace=True)

        # 9) Ajuste de tipos e normalização de 'Genero'
        self.ajustar_tipos_e_calculos()

        # 10) Substituir valores de objetivo, se houver
        self.aplicar_substituicoes_objetivo()

        # 11) Aplicar mapeamento de campanha (gera 'ID_Campanha' e pode sobrescrever 'Campanha')
        logging.debug("Aplicando parametrização de campanha...")
        self.df = aplicar_parametrizacao_campanha(
            df=self.df,
            mapping_campanha=self.mapping_campanha,
            mapping_sigla=self.mapping_sigla
        )
        logging.debug("Parametrização aplicada com sucesso.")

        # Exemplo de debug para verificar se ID_Campanha foi preenchido
        if "ID_Campanha" in self.df.columns:
            logging.debug("Exemplo de ID_Campanha: %s", self.df["ID_Campanha"].dropna().head(5).tolist())

        # 12) Link de preview, se aplicável
        self.determine_ad_preview_link()

        # 13) Atribuir o veículo e seu ID (gera 'Veiculo' e 'ID_Veiculo')
        self.criar_veiculo()

        # 14) Reordena colunas para o modelo (GENDER_MODEL_COLUMN_ORDER)
        self.reordenar_colunas_para_modelo()

        # 15) Gera ID único da linha
        self.gerar_id()

        # 16) Gera numeração nas linhas (caso seja append only)
        self.df = gerar_numeracao(self.df, df_destino, linha_insercao=2, coluna='Numero')

        logging.info("Processamento do MetaGeneroETL finalizado. Formato final: %s", self.df.shape)
        return self.df

    def criar_veiculo(self):
        """
        Sobrescreve para chamar atribuir_veiculo_e_id_meta, que
        gera as colunas 'Veiculo' e 'ID_Veiculo' de acordo com
        as regras do Meta.
        """
        logging.debug(">>> In MetaGeneroETL.criar_veiculo -> atribuir_veiculo_e_id_meta")
        self.df = atribuir_veiculo_e_id_meta(self.df)
        return self.df


class TikTokGeneroETL(BaseGeneroETL):
    pass


class LinkedinGeneroETL(BaseGeneroETL):
    pass


class PinterestGeneroETL(BaseGeneroETL):
    """
    ETL de Gênero para dados do Pinterest, gera datas personalizadas e
    preenche Nome_do_Conjunto_de_Anuncio / Nome_do_Anuncio a partir de 'Campaign name'.
    """
    def ajustar_tipos_e_calculos(self):
        logging.debug(">>> In PinterestGeneroETL.ajustar_tipos_e_calculos (Antes do super)")
        super().ajustar_tipos_e_calculos()
        logging.debug("Gerando datas específicas do Pinterest")
        self.df = generate_pinterest_dates(self.df)
        logging.debug("Preenchendo campos de campanha para Pinterest (Nome_do_Anuncio, etc.)")
        self.df = preencher_campos_com_campanha(self.df)
        return self.df
