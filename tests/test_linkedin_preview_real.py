import os
import sys
import unittest
import pandas as pd

# Adiciona o diretório base ao sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.get_google_client import get_google_client
from utils.google_sheets import CREDS_PATH, SPREADSHEET_ID
from utils.read_sheet_as_dataframe import read_sheet_as_dataframe_range
from utils.common_linkedin import carregar_mapeamentos_linkedin
from utils.normalize import normalize_columns, normalize_parametrizacao_values

class TestLinkedinPreviewMappingReal(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.client = get_google_client(CREDS_PATH)
        df_raw = read_sheet_as_dataframe_range(
            cls.client,
            SPREADSHEET_ID,
            sheet_name="BI_PARAMETRIZAÇÃO",
            range_str="A1:ZZ",
            header_row_index=1
        )
        cls.df_parametrizacao = normalize_parametrizacao_values(df_raw)
        cls.df_parametrizacao.columns = normalize_columns(cls.df_parametrizacao.columns)

    def test_colunas_existem(self):
        """Verifica se colunas críticas existem no DataFrame."""
        cols = self.df_parametrizacao.columns
        self.assertIn("utm_content", cols, "Coluna 'utm_content' não encontrada na BI_PARAMETRIZAÇÃO.")
        self.assertIn("preview", cols, "Coluna 'preview' não encontrada na BI_PARAMETRIZAÇÃO.")

    def test_carregar_mapeamentos_linkedin(self):
        """Testa se o mapeamento {utm_content: preview} é corretamente gerado e tem links válidos."""
        mapping_preview, _ = carregar_mapeamentos_linkedin()

        self.assertIsInstance(mapping_preview, dict, "Mapping preview não é um dicionário.")
        self.assertGreater(len(mapping_preview), 0, "Mapping preview está vazio.")

        for i, (k, v) in enumerate(mapping_preview.items()):
            self.assertIsInstance(k, str, f"Chave {k} não é string.")
            self.assertIsInstance(v, str, f"Valor {v} não é string.")
            self.assertTrue(v.startswith("http"), f"Valor de preview inválido: {v}")
            if i >= 5:
                break


if __name__ == "__main__":
    unittest.main()
