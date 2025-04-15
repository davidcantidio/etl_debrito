import logging
import pandas as pd
from scripts.etl_geral import BaseGeralETL
from utils.normalize import normalizar_genero
from utils.campos_calculados import inicializar_colunas_auxiliares
from utils.fields_lists import GENDER_MODEL_COLUMN_ORDER
from utils.organizar_dataframe import reordenar_colunas_para_modelo
from utils.numeracao import gerar_numeracao
from utils.renomeacoes import aplicar_substituicoes_objetivo
from utils.common_meta import (
    load_and_prepare_meta_gender_data,
    load_and_prepare_meta_placement_data,
    pivot_meta_gender_data,
    pivot_meta_placement_data,
    merge_placement_and_gender_data,
    preserve_placement_column,
)
from utils.atribuicoes_via_lookup import atribuir_veiculo_e_id_meta


# ---------- BASE GENERO ----------

class BaseGeneroETL(BaseGeralETL):
    def ajustar_tipos_e_calculos(self):
        logging.debug(">>> In BaseGeneroETL.ajustar_tipos_e_calculos (Antes do super)")
        super().ajustar_tipos_e_calculos()

        if 'Gender' in self.df.columns:
            logging.debug("Renomeando 'Gender' para 'Genero'")
            self.df.rename(columns={'Gender': 'Genero'}, inplace=True)

        if 'Genero' in self.df.columns:
            logging.debug("Normalizando valores da coluna 'Genero'")
            self.df['Genero'] = self.df['Genero'].apply(normalizar_genero)

        return self.df

    def aplicar_substituicoes_objetivo(self):
        logging.debug(">>> Aplicando substituições de objetivo")
        self.df = aplicar_substituicoes_objetivo(self.df)

    def remover_colunas_indesejadas(self):
        logging.debug(">>> Removendo colunas indesejadas, exceto 'Ad ID' e 'Data'")
        cols_a_preservar = {'Ad ID', 'Data'}
        self.df = self.df[[col for col in self.df.columns if not col.startswith('_') or col in cols_a_preservar]]

    def get_dataframe_base(self, df_destino=None):
        logging.debug(">>> Executando get_dataframe_base() da BaseGeneroETL")
        self.renomear_colunas_origem_para_modelo()
        self.ajustar_tipos_e_calculos()
        self.aplicar_substituicoes_objetivo()
        self.aplicar_parametrizacao_campanha_externa()
        self.df = inicializar_colunas_auxiliares(self.df)
        return self.df.copy()

    def processar(self, df_destino=None):
        logging.debug(">>> In processar() padrão da BaseGeneroETL")
        self.df = self.get_dataframe_base(df_destino)
        self.df = reordenar_colunas_para_modelo(self.df, GENDER_MODEL_COLUMN_ORDER)
        self.gerar_id()
        self.df = gerar_numeracao(self.df, df_destino, linha_insercao=2, coluna="Numero")
        return self.df


# ---------- META GENERO ----------

class MetaGeneroETL(BaseGeneroETL):

    def gerar_dados_genero_meta(self):
        logging.debug(">>> Iniciando gerar_dados_genero_meta")
        df_gender = load_and_prepare_meta_gender_data()
        df_placement = load_and_prepare_meta_placement_data()
        logging.debug(f"metaGenero shape: {df_gender.shape}, metaGeral shape: {df_placement.shape}")

        df_gender_pivot = pivot_meta_gender_data(df_gender)
        df_placement_pivot = pivot_meta_placement_data(df_placement)
        logging.debug(f"Pivot metaGenero shape: {df_gender_pivot.shape}, pivot metaGeral shape: {df_placement_pivot.shape}")

        df_merged = merge_placement_and_gender_data(df_placement_pivot, df_gender_pivot)
        logging.debug(f"Merged gender + placement shape: {df_merged.shape}")

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
                    value = row.get(metric_col, 0)
                    row_data[metric] = round(row[metric] * value / total) if total > 0 else 0
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
        logging.info(">>> Iniciando processar() da MetaGeneroETL")

        # 1. Extrai DataFrame base com dimensões, Ad ID e Data
        df_base = self.get_dataframe_base(df_destino)
        logging.debug(f"Base extraída da classe BaseGeneroETL, shape: {df_base.shape}")

        # 2. Processa dados específicos de gênero (pivot, merge, distribuição)
        df_genero = self.gerar_dados_genero_meta()
        logging.debug(f"Dados de gênero específicos da Meta, shape: {df_genero.shape}")

        # 3. Merge: Une a base com as métricas específicas por Ad ID + Data
        self.df = df_base.merge(df_genero, on=["Ad ID", "Data"], how="left")
        logging.debug(f"Resultado após merge por Ad ID + Data, shape: {self.df.shape}")

        # 4. Usar '_Plataforma' para gerar 'Placement' temporário
        if '_Plataforma' in self.df.columns:
            self.df['Placement'] = self.df['_Plataforma']
            logging.debug("Coluna 'Placement' criada a partir de '_Plataforma' para inferência de Veiculo.")

        # 5. Atribui Veiculo e ID_Veiculo (Meta)
        self.atribuir_veiculo_meta()

        # 6. Aplicar mapeamento de Campanha e ID_Campanha via Campanha_Lookup
        self.aplicar_parametrizacao_campanha_externa()


        logging.debug(f"Colunas disponíveis antes do gerar_id: {list(self.df.columns)}")
        # Resolver conflitos de nomes vindos do merge
        self.df["Impressoes"] = self.df["Impressoes_y"]
        self.df["Investimento"] = self.df["Investimento_y"]
        self.df["Cliques_no_Link"] = self.df["Cliques_no_Link_y"]


        # 7. Gera ID único
        self.gerar_id()

        # 8. Ordena colunas conforme o modelo
        self.df = reordenar_colunas_para_modelo(self.df, GENDER_MODEL_COLUMN_ORDER)

        # 9. Gera numeração para escrita
        self.df = gerar_numeracao(self.df, df_destino, linha_insercao=2, coluna="Numero")

        # 10. Limpeza final: remove colunas auxiliares
        colunas_modelo = set(GENDER_MODEL_COLUMN_ORDER)
        colunas_validas = colunas_modelo.union({"ID"})
        self.df = self.df[[col for col in self.df.columns if col in colunas_validas]]

        # Logs de verificação
        logging.debug(f"Exemplo de Campanha: {self.df['Campanha'].dropna().unique()[:5]}")
        logging.debug(f"Exemplo de ID_Campanha: {self.df['ID_Campanha'].dropna().unique()[:5]}")
        logging.debug(f"Exemplo de Veiculo: {self.df['Veiculo'].dropna().unique()[:5]}")
        logging.debug(f"Exemplo de ID_Veiculo: {self.df['ID_Veiculo'].dropna().unique()[:5]}")
        logging.debug(f"Exemplo de Genero: {self.df['Genero'].dropna().unique()[:5] if 'Genero' in self.df.columns else 'coluna ausente'}")

        logging.info(f"Final do processamento MetaGeneroETL. Linhas: {self.df.shape[0]}")
        return self.df


# ---------- TIKTOK GENERO ----------

class TikTokGeneroETL(BaseGeneroETL):
    pass


# ---------- LINKEDIN GENERO ----------

class LinkedinGeneroETL(BaseGeneroETL):
    pass


# ---------- PINTEREST GENERO ----------

class PinterestGeneroETL(BaseGeneroETL):
    def ajustar_tipos_e_calculos(self):
        logging.debug(">>> In PinterestGeneroETL.ajustar_tipos_e_calculos (Antes do super)")
        super().ajustar_tipos_e_calculos()
        logging.debug("Aplicando transformações específicas para Pinterest")
        self.df = generate_pinterest_dates(self.df)
        self.df = preencher_campos_com_campanha(self.df)
        return self.df