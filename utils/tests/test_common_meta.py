import os
import sys
# Ajusta o sys.path para que o pacote "utils" seja encontrado
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import unittest
import pandas as pd
import logging

# Ativa o logging em DEBUG para os testes
logging.basicConfig(level=logging.DEBUG)

# Importa as funções do módulo common_meta
from utils.common_meta import (
    load_and_prepare_meta_placement_data,
    load_and_prepare_meta_gender_data,
    pivot_meta_placement_data,
    pivot_meta_gender_data,
    merge_placement_and_gender_data,
    distribute_gender_metrics
)

# Mocks para carregar dados (para testes unitários de load_and_prepare)
def fake_carregar_aba_google_sheets_placement(*args, **kwargs):
    # Retorna um DataFrame de exemplo para metaGeral
    data = {
        "Campaign name": ["Campanha A", "Campanha B"],
        "Ad name": ["Anúncio 1", "Anúncio 2"],
        "Placement": ["Facebook", "Instagram"],
        "Impressions": ["1000", "2000"],
        "Link clicks": ["100", "150"],
        "Cost": ["50", "75"],
        "Video watches at 100%": ["300", "400"],
        "Date": ["2023-08-01", "2023-08-01"]
    }
    return pd.DataFrame(data)


def fake_carregar_aba_google_sheets_gender(*args, **kwargs):
    # Retorna um DataFrame de exemplo para metaGenero
    data = {
        "Campaign name": ["Campanha A", "Campanha B"],
        "Ad name": ["Anúncio 1", "Anúncio 2"],
        "Gender": ["Female", "Male"],
        "Impressions": ["1500", "3000"],
        "Link clicks": ["150", "300"],
        "Cost": ["15", "30"],
        "Video watches at 100%": ["150", "300"],
        "Date": ["2023-08-01", "2023-08-01"]
    }
    return pd.DataFrame(data)


class TestLoadAndPrepareFunctions(unittest.TestCase):
    def setUp(self):
        # Salva as referências originais, se necessário, para restaurá-las depois
        from utils.google_sheets import carregar_aba_google_sheets
        self._orig_carregar = carregar_aba_google_sheets

    def test_load_and_prepare_meta_placement_data(self):
        # Testa usando o mock para metaGeral
        from utils.google_sheets import carregar_aba_google_sheets
        carregar_aba_google_sheets_orig = carregar_aba_google_sheets
        try:
            # Substitui a função de carregamento pelo mock
            import utils.google_sheets as gs
            gs.carregar_aba_google_sheets = fake_carregar_aba_google_sheets_placement

            df = load_and_prepare_meta_placement_data()
            self.assertIn("Ad ID", df.columns, "A coluna 'Ad ID' não foi criada.")
            self.assertIn("Placement", df.columns, "A coluna 'Placement' deve existir antes do pivot.")
            self.assertIn("Impressions", df.columns, "A coluna 'Impressions' não foi convertida.")
            self.assertTrue(pd.api.types.is_numeric_dtype(df["Impressions"]),
                            "A coluna 'Impressions' não foi convertida para numérico.")
            self.assertIn("Date", df.columns, "A coluna 'Date' não foi encontrada.")
        finally:
            gs.carregar_aba_google_sheets = carregar_aba_google_sheets_orig

    def test_load_and_prepare_meta_gender_data(self):
        from utils.google_sheets import carregar_aba_google_sheets
        carregar_aba_google_sheets_orig = carregar_aba_google_sheets
        try:
            import utils.google_sheets as gs
            gs.carregar_aba_google_sheets = fake_carregar_aba_google_sheets_gender

            df = load_and_prepare_meta_gender_data()
            self.assertIn("Ad ID", df.columns, "A coluna 'Ad ID' não foi criada em metaGenero.")
            self.assertIn("Gender", df.columns, "A coluna 'Gender' não está presente em metaGenero.")
            self.assertIn("Impressions", df.columns, "A coluna 'Impressions' não foi convertida em metaGenero.")
            self.assertTrue(pd.api.types.is_numeric_dtype(df["Impressions"]),
                            "A coluna 'Impressions' não foi convertida para numérico em metaGenero.")
            self.assertIn("Date", df.columns, "A coluna 'Date' não foi encontrada em metaGenero.")
        finally:
            gs.carregar_aba_google_sheets = carregar_aba_google_sheets_orig


class TestPivotFunctions(unittest.TestCase):
    def test_pivot_meta_placement_data(self):
        # Cria um DataFrame de exemplo para metaGeral
        df = pd.DataFrame({
            "Ad ID": ["ad1", "ad1", "ad2", "ad2"],
            "Date": ["2023-08-01", "2023-08-01", "2023-08-01", "2023-08-01"],
            "Placement": ["Facebook", "Instagram", "Facebook", "Instagram"],
            "Impressions": [1000, 500, 2000, 1000],
            "Link clicks": [100, 50, 200, 100],
            "Cost": [10, 5, 20, 10],
            "Video watches at 100%": [100, 50, 200, 100]
        })
        pivot_df = pivot_meta_placement_data(df)
        self.assertIn("Ad ID", pivot_df.columns)
        self.assertIn("Date", pivot_df.columns)
        for expected in ["Facebook_Impressions", "Instagram_Impressions",
                         "Facebook_Link clicks", "Instagram_Link clicks",
                         "Facebook_Cost", "Instagram_Cost",
                         "Facebook_Video watches at 100%", "Instagram_Video watches at 100%"]:
            self.assertIn(expected, pivot_df.columns,
                          f"A coluna '{expected}' não foi encontrada no DataFrame pivotado.")

    def test_pivot_meta_gender_data(self):
        # Cria um DataFrame de exemplo para metaGenero
        df = pd.DataFrame({
            "Ad ID": ["ad1", "ad1", "ad2", "ad2"],
            "Date": ["2023-08-01", "2023-08-01", "2023-08-01", "2023-08-01"],
            "Gender": ["Female", "Female", "Male", "Male"],
            "Impressions": [1000, 500, 2000, 1000],
            "Link clicks": [100, 50, 200, 100],
            "Cost": [10, 5, 20, 10],
            "Video watches at 100%": [100, 50, 200, 100]
        })
        pivot_df = pivot_meta_gender_data(df)
        unique_groups = df.groupby(["Ad ID", "Date", "Gender"]).size().reset_index().iloc[:, :3]
        self.assertEqual(len(pivot_df), len(unique_groups),
                         "Número de grupos agregados incorreto.")
        # Verifica agregação para um grupo
        mask = (pivot_df["Ad ID"] == "ad1") & (pivot_df["Date"] == "2023-08-01") & (pivot_df["Gender"] == "Female")
        row = pivot_df.loc[mask]
        self.assertEqual(row["Impressions"].iloc[0], 1500)

        
class TestMergeAndDistributeFunctions(unittest.TestCase):
    def setUp(self):
        # Cria um DataFrame simulado de merge (resultado de merge_placement_and_gender_data)
        # Este DataFrame combina colunas de metaGenero pivotado e metaGeral pivotado.
        data = {
            "Ad ID": ["ad1", "ad2"],
            "Date": ["2023-08-01", "2023-08-01"],
            "Gender": ["Female", "Male"],
            "Impressions": [1500, 3000],
            "Link clicks": [150, 300],
            "Cost": [15, 30],
            "Video watches at 100%": [150, 300],
            "Facebook_Impressions": [1000, 2000],
            "Instagram_Impressions": [500, 1000],
            "Facebook_Link clicks": [100, 200],
            "Instagram_Link clicks": [50, 100],
            "Facebook_Cost": [10, 20],
            "Instagram_Cost": [5, 10],
            "Facebook_Video watches at 100%": [100, 200],
            "Instagram_Video watches at 100%": [50, 100],
        }
        self.df_merged = pd.DataFrame(data)

    def test_merge_placement_and_gender_data(self):
        # Testa o merge utilizando as chaves "Ad ID" e "Date"
        # Cria DataFrames de exemplo simulados para os pivôs
        df_placement = pd.DataFrame({
            "Ad ID": ["ad1", "ad2", "ad3"],
            "Date": ["2023-08-01", "2023-08-01", "2023-08-01"],
            "Facebook_Impressions": [1000, 2000, 1500],
            "Instagram_Impressions": [500, 1000, 0],
            "Facebook_Link clicks": [100, 200, 150],
            "Instagram_Link clicks": [50, 100, 0]
        })
        df_gender = pd.DataFrame({
            "Ad ID": ["ad1", "ad2", "ad4"],
            "Date": ["2023-08-01", "2023-08-01", "2023-08-01"],
            "Gender": ["Female", "Male", "Female"],
            "Impressions": [1500, 3000, 500],
            "Link clicks": [150, 300, 40],
            "Cost": [15, 30, 5],
            "Video watches at 100%": [150, 300, 20]
        })
        merged_df = merge_placement_and_gender_data(df_placement, df_gender)
        self.assertEqual(len(merged_df), 2, "Merge deve retornar apenas registros com chaves comuns.")

    def test_distribute_gender_metrics(self):
        df_distributed = distribute_gender_metrics(self.df_merged)
        # Neste exemplo, para cada registro do merge (2 registros), cada plataforma gera uma linha; total esperado = 4
        expected_rows = len(self.df_merged) * 2
        self.assertEqual(len(df_distributed), expected_rows,
                         "Número de linhas distribuídas incorreto.")
        # Testa a distribuição para 'ad1':
        # Para ad1: Facebook_Impressions = 1000, Instagram_Impressions = 500 → total pivot = 1500;
        # Métrica agregada "Impressions" do df_merged para ad1 = 1500.
        # Espera: para Facebook: round(1500*(1000/1500)) = 1000; para Instagram: round(1500*(500/1500)) = 500.
        for platform, expected in [("Facebook", 1000), ("Instagram", 500)]:
            row = df_distributed[(df_distributed["Ad ID"] == "ad1") & (df_distributed["_Plataforma"] == platform)]
            self.assertEqual(row["Impressions"].iloc[0], expected,
                             f"Distribuição de 'Impressions' incorreta para {platform} em ad1.")

    def test_preservation_of_key_columns_in_distribution(self):
        df_distributed = distribute_gender_metrics(self.df_merged)
        for col in ["Ad ID", "Date", "Gender", "_Plataforma"]:
            self.assertIn(col, df_distributed.columns,
                          f"A coluna '{col}' não foi preservada na distribuição.")

if __name__ == "__main__":
    unittest.main()
