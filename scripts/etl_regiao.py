import logging
import pandas as pd
from scripts.etl_geral import BaseGeralETL
from utils.campos_calculados import inicializar_colunas_auxiliares
from utils.fields_lists import REGION_MODEL_COLUMN_ORDER
from utils.organizar_dataframe import reordenar_colunas_para_modelo
from utils.numeracao import gerar_numeracao
from utils.renomeacoes import aplicar_substituicoes_objetivo
from utils.common_meta import (
    load_and_prepare_meta_region_data,        # Adaptado de load_and_prepare_meta_gender_data
    load_and_prepare_meta_placement_data,
    pivot_meta_region_data,                     # Pivot para dados de região (agrupa por 'Ad ID', 'Date' e 'Province name')
    pivot_meta_placement_data,
    merge_placement_and_region_data,            # Merge entre dados de region e placement
    preserve_placement_column,
)
from utils.atribuicoes_via_lookup import atribuir_veiculo_e_id_meta, atribuir_id_veiculo_generico
from utils.common_pinterest import preencher_campos_com_campanha
from utils.datas import generate_pinterest_dates
from utils.geo_normalize import limpeza_basica, obter_estado_de_regiao, carregar_caches_padrao

# ====================================================
#                    BASE REGIÃO
# ====================================================

class BaseRegiaoETL(BaseGeralETL):
    def ajustar_tipos_e_calculos(self):
        logging.debug(">>> In BaseRegiaoETL.ajustar_tipos_e_calculos (Antes do super)")
        super().ajustar_tipos_e_calculos()
        
        # Em vez de tratar gênero, geramos o campo 'Estado' a partir de 'Province name'
        if "Province name" in self.df.columns:
            logging.debug("Gerando coluna 'Estado' a partir de 'Province name'")
            cache_estados, cache_municipios = carregar_caches_padrao()
            self.df["Estado"] = self.df["Province name"].apply(limpeza_basica).apply(
                lambda r: obter_estado_de_regiao(r, cache_municipios, cache_estados)
            )
        else:
            self.df["Estado"] = "Não classificado"

        # Tratamento padrão de Veiculo: se não estiver definido, utiliza self.veiculo ou fallback para 'Facebook'
        if "Veiculo" not in self.df.columns or self.df["Veiculo"].isnull().all():
            if self.veiculo:
                logging.debug(f"Veiculo não atribuído, usando self.veiculo: {self.veiculo}")
                self.df["Veiculo"] = self.veiculo
            else:
                logging.debug("Veiculo não atribuído e self.veiculo ausente, aplicando fallback padrão: Facebook")
                self.df["Veiculo"] = "Facebook"
            self.df = atribuir_id_veiculo_generico(self.df)
        return self.df

    def aplicar_substituicoes_objetivo(self):
        logging.debug(">>> Aplicando substituições de objetivo")
        self.df = aplicar_substituicoes_objetivo(self.df)

    def remover_colunas_indesejadas(self):
        logging.debug(">>> Removendo colunas indesejadas, exceto 'Ad ID' e 'Data'")
        cols_a_preservar = {'Ad ID', 'Data'}
        self.df = self.df[[col for col in self.df.columns if not col.startswith('_') or col in cols_a_preservar]]

    def get_dataframe_base(self, df_destino=None):
        logging.debug(">>> Executando get_dataframe_base() da BaseRegiaoETL")
        self.renomear_colunas_origem_para_modelo()
        self.ajustar_tipos_e_calculos()
        self.aplicar_substituicoes_objetivo()
        self.aplicar_parametrizacao_campanha_externa()
        self.df = inicializar_colunas_auxiliares(self.df)
        return self.df.copy()

    def processar(self, df_destino=None):
        logging.debug(">>> In processar() padrão da BaseRegiaoETL")
        self.df = self.get_dataframe_base(df_destino)
        self.df = reordenar_colunas_para_modelo(self.df, REGION_MODEL_COLUMN_ORDER)
        self.gerar_id()
        self.df = gerar_numeracao(self.df, df_destino, linha_insercao=2, coluna="Numero")
        return self.df

# ====================================================
#                  META REGIÃO
# ====================================================

class MetaRegiaoETL(BaseRegiaoETL):
    def gerar_dados_regiao_meta(self):
        """
        Gera os dados específicos de região a partir das abas metaRegiao e metaGeral.
        Aplica pivot, merge e distribuição proporcional das métricas,
        usando "Province name" como chave.
        """
        logging.debug(">>> Iniciando gerar_dados_regiao_meta")
        df_region = load_and_prepare_meta_region_data()
        df_placement = load_and_prepare_meta_placement_data()
        logging.debug(f"metaRegiao shape: {df_region.shape}, metaGeral shape: {df_placement.shape}")

        df_region_pivot = pivot_meta_region_data(df_region)
        df_placement_pivot = pivot_meta_placement_data(df_placement)
        logging.debug(f"Pivot metaRegiao shape: {df_region_pivot.shape}, pivot metaGeral shape: {df_placement_pivot.shape}")

        df_merged = merge_placement_and_region_data(df_placement_pivot, df_region_pivot)
        logging.debug(f"Merged region + placement shape: {df_merged.shape}")

        metrics = ["Impressions", "Link clicks", "Cost", "Video watches at 100%"]
        placement_cols = [col for col in df_merged.columns if col.endswith("_Impressions")]
        placements = list({col.rsplit("_", 1)[0] for col in placement_cols})
        logging.debug(f"Plataformas detectadas: {placements}")

        output_rows = []
        for _, row in df_merged.iterrows():
            base_data = {
                "Ad ID": row.get("Ad ID"),
                "Date": row.get("Date")
            }
            total_by_metric = {
                metric: sum(row.get(f"{pl}_{metric}", 0) for pl in placements)
                for metric in metrics
            }
            for platform in placements:
                row_data = base_data.copy()
                row_data["_Plataforma"] = platform
                for metric in metrics:
                    metric_col = f"{platform}_{metric}"
                    total = total_by_metric[metric]
                    row_data[metric] = round(row[metric] * (row.get(metric_col, 0) / total)) if total > 0 else 0
                output_rows.append(row_data)
        df_result = pd.DataFrame(output_rows)
        logging.debug(f"Resultado pós-distribuição, shape: {df_result.shape}")
        df_result.rename(columns={
            "Date": "Data",
            "Impressions": "Impressoes",
            "Link clicks": "Cliques_no_Link",
            "Cost": "Investimento",
            "Video watches at 100%": "Visualizacoes_ate_100"
        }, inplace=True)
        return df_result

    def atribuir_veiculo_meta(self):
        logging.debug(">>> Atribuindo Veiculo e ID_Veiculo para Meta")
        self.df = atribuir_veiculo_e_id_meta(self.df)

    def processar(self, df_destino=None):
        logging.info(">>> Iniciando processar() da MetaRegiaoETL")
        # 1. Extrai DataFrame base com dimensões, Ad ID e Data
        df_base = self.get_dataframe_base(df_destino)
        logging.debug(f"Base extraída da classe BaseRegiaoETL, shape: {df_base.shape}")

        # 2. Processa dados específicos de região (pivot, merge, distribuição)
        df_regiao = self.gerar_dados_regiao_meta()
        logging.debug(f"Dados de região específicos da Meta, shape: {df_regiao.shape}")

        # 3. Merge: Une a base com as métricas regionais por Ad ID e Data
        self.df = df_base.merge(df_regiao, on=["Ad ID", "Data"], how="left")
        logging.debug(f"Resultado após merge por Ad ID + Data, shape: {self.df.shape}")

        # 4. Se existir '_Plataforma', gera 'Placement' temporário (para eventual inferência de veículo)
        if '_Plataforma' in self.df.columns:
            self.df['Placement'] = self.df['_Plataforma']
            logging.debug("Coluna 'Placement' criada a partir de '_Plataforma' para inferência de Veiculo.")

        # 5. Atribui Veiculo e ID_Veiculo (Meta)
        self.atribuir_veiculo_meta()

        # 6. Aplica mapeamento de Campanha e ID_Campanha via lookup
        self.aplicar_parametrizacao_campanha_externa()

        logging.debug(f"Colunas disponíveis antes do gerar_id: {list(self.df.columns)}")
        # Resolver conflitos de nomes vindos do merge
        self.df["Impressoes"] = self.df["Impressoes_y"]
        self.df["Investimento"] = self.df["Investimento_y"]
        self.df["Cliques_no_Link"] = self.df["Cliques_no_Link_y"]
        self.df["Visualizacoes_ate_100"] = self.df["Visualizacoes_ate_100_y"]

        # 7. Gera ID único
        self.gerar_id()

        # 8. Reordena colunas conforme o modelo de Região
        self.df = reordenar_colunas_para_modelo(self.df, REGION_MODEL_COLUMN_ORDER)

        # 9. Gera numeração para escrita
        self.df = gerar_numeracao(self.df, df_destino, linha_insercao=2, coluna="Numero")

        # 10. Limpeza final: mantém apenas as colunas definidas no modelo
        colunas_modelo = set(REGION_MODEL_COLUMN_ORDER)
        colunas_validas = colunas_modelo.union({"ID"})
        self.df = self.df[[col for col in self.df.columns if col in colunas_validas]]

        logging.debug(f"Exemplo de Campanha: {self.df['Campanha'].dropna().unique()[:5]}")
        logging.debug(f"Exemplo de ID_Campanha: {self.df['ID_Campanha'].dropna().unique()[:5]}")
        logging.debug(f"Exemplo de Veiculo: {self.df['Veiculo'].dropna().unique()[:5]}")
        logging.debug(f"Exemplo de ID_Veiculo: {self.df['ID_Veiculo'].dropna().unique()[:5]}")
        logging.debug(f"Exemplo de Estado: {self.df['Estado'].dropna().unique()[:5]}")
        logging.info(f"Final do processamento MetaRegiaoETL. Linhas: {self.df.shape[0]}")
        return self.df

# ====================================================
#              TIKTOK, LINKEDIN, PINTEREST
# ====================================================

class TikTokRegiaoETL(BaseRegiaoETL):
    pass


class LinkedinRegiaoETL(BaseRegiaoETL):
    pass


class PinterestRegiaoETL(BaseRegiaoETL):
    def ajustar_tipos_e_calculos(self):
        logging.debug(">>> In PinterestRegiaoETL.ajustar_tipos_e_calculos (Antes do super)")
        super().ajustar_tipos_e_calculos()
        logging.debug("Aplicando transformações específicas para Pinterest")
        self.df = generate_pinterest_dates(self.df)
        self.df = preencher_campos_com_campanha(self.df)
        return self.df
