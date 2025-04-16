# tests/test_preview_mapping_linkedin.py

import os
import sys
import unittest
import logging

# Ajusta o sys.path para incluir o diretório raiz do projeto.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.common_linkedin import carregar_mapeamentos_linkedin

class TestPreviewMappingLinkedin(unittest.TestCase):
    def test_carregar_mapeamentos(self):
        """
        Testa se a função carregar_mapeamentos_linkedin() retorna dicionários não vazios
        e se o dicionário de preview contém chaves e valores adequados:
          - As chaves devem ser strings (correspondentes a ID_Content)
          - Os valores devem ser strings que comecem com "http", representando o link de preview (a partir da coluna PREVIEW)
        """
        mapping_preview, mapping_criativo = carregar_mapeamentos_linkedin()
        
        # Verifica se mapping_preview é um dicionário não vazio.
        self.assertIsInstance(mapping_preview, dict, "Mapping preview deve ser um dicionário.")
        self.assertGreater(len(mapping_preview), 0, "Mapping preview está vazio.")
        
        # Valida que cada link de preview é uma string que começa com "http"
        for key, value in mapping_preview.items():
            self.assertIsInstance(key, str, "Chave do mapping preview deve ser uma string.")
            self.assertIsInstance(value, str, "Valor do mapping preview deve ser uma string.")
            self.assertTrue(value.startswith("http"),
                            f"Link de preview '{value}' para a chave '{key}' não começa com 'http'.")
        
        # Opcional: Verifica também o mapping de criativo, se necessário.
        self.assertIsInstance(mapping_criativo, dict, "Mapping criativo deve ser um dicionário.")
        self.assertGreater(len(mapping_criativo), 0, "Mapping criativo está vazio.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
