# tests/test_preencher_nomes_anuncio_linkedin.py

import os
import sys
import unittest
import pandas as pd
import logging

# Ajusta o sys.path para que o pacote "utils" seja encontrado
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.common_linkedin import preencher_nomes_anuncio_linkedin

class TestPreencherNomesAnuncioLinkedin(unittest.TestCase):
    def test_preencher_nomes(self):
        # Cria um DataFrame de exemplo simulando os dados de entrada
        data = {
            "Content (utm)": ["content1", "content2", "content3"],
        }
        df = pd.DataFrame(data)
        
        # Cria um mapping que define um preview link para content1 e content2; content3 não será mapeado
        mapping_criativo = {
            "content1": "https://www.example.com/preview1",
            "content2": "https://www.example.com/preview2"
        }
        
        # Chama a função
        df_result = preencher_nomes_anuncio_linkedin(df, mapping_criativo)
        
        # O campo Nome_do_Anuncio deve ser exatamente o valor original de "Content (utm)"
        pd.testing.assert_series_equal(df_result["Nome_do_Anuncio"], df["Content (utm)"],
                                       check_names=False,
                                       err_msg="Nome_do_Anuncio não corresponde ao esperado (deve ser igual a Content (utm)).")
        
        # O campo Nome_do_Conjunto_de_Anuncio deve ser mapeado conforme o dictionary;
        # Para content1 e content2 os valores devem ser preenchidos; content3 deve resultar em NaN.
        self.assertEqual(df_result.loc[0, "Nome_do_Conjunto_de_Anuncio"], "https://www.example.com/preview1",
                         "Erro no mapeamento para content1.")
        self.assertEqual(df_result.loc[1, "Nome_do_Conjunto_de_Anuncio"], "https://www.example.com/preview2",
                         "Erro no mapeamento para content2.")
        self.assertTrue(pd.isna(df_result.loc[2, "Nome_do_Conjunto_de_Anuncio"]),
                        "content3 deveria não ter mapeamento e resultar em NaN.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
