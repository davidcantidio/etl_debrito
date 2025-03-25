import pandas as pd
import numpy as np
import logging
from utils.campanha_mapper import buscar_mapping

class etl_alcance_meta:
    def __init__(self, df, mapping_campanha=None, mapping_sigla=None):
        """
        Inicializa o ETL para os dados de META – Alcance.
        :param df: DataFrame lido da aba de origem (Meta – Alcance)
        :param mapping_campanha: dicionário que mapeia o valor da coluna "Campaign name" (normalizado)
                                 para o valor desejado para a coluna Campanha (destino), vindo da BI_PARAMETRIZAÇÃO
                                 (coluna "CAMPANHA OBRIGATÓRIO")
        :param mapping_sigla: dicionário que mapeia o valor da coluna "Campaign name" (normalizado)
                              para o valor desejado para a coluna ID_Campanha (destino), vindo da BI_PARAMETRIZAÇÃO
                              (coluna "SIGLA")
        """
        self.df = df.copy()
        # Normaliza os nomes das colunas removendo espaços extras
        self.df.columns = [col.strip() for col in self.df.columns]
        self.mapping_campanha = mapping_campanha or {}
        self.mapping_sigla = mapping_sigla or {}

    def ajustar_tipos(self):
        # Cria um dicionário de nomes normalizados (lowercase) para localizar as colunas
        columns_norm = {col.strip().lower(): col for col in self.df.columns}
        reach_col = columns_norm.get("reach", None)
        imp_col = columns_norm.get("impressions", None)
        
        if reach_col:
            self.df[reach_col] = pd.to_numeric(self.df[reach_col], errors='coerce').fillna(0).astype(int)
        else:
            logging.error("Coluna 'Reach' não encontrada na origem.")
        if imp_col:
            self.df[imp_col] = pd.to_numeric(self.df[imp_col], errors='coerce').fillna(0).astype(int)
        else:
            logging.error("Coluna 'Impressions' não encontrada na origem.")
        
        # Converter demais colunas para string e limpar espaços
        for col in ['Veiculo', 'Campaign name', 'Placement', 'Campaign ID']:
            if col in self.df.columns:
                self.df[col] = self.df[col].astype(str).str.strip()
            else:
                logging.warning(f"Coluna '{col}' não encontrada na origem.")
        
        # Criar as colunas de destino "Alcance" e "Impressões" a partir das colunas originais
        if reach_col:
            self.df["Alcance"] = self.df[reach_col]
        else:
            self.df["Alcance"] = 0
        if imp_col:
            self.df["Impressões"] = self.df[imp_col]
        else:
            self.df["Impressões"] = 0

    def aplicar_parametrizacao_campanha(self):
        """
        Preenche as colunas 'Campanha' e 'ID_Campanha' a partir dos mapeamentos externos.
        Usa o valor da coluna "Campaign name" normalizado para buscar nos dicionários.
        Se não houver correspondência, mantém o valor original (para Campanha) e preenche com vazio para ID_Campanha.
        """
        self.df["Campanha"] = self.df["Campaign name"].apply(
            lambda x: buscar_mapping(self.mapping_campanha, x) or x
        )
        self.df["ID_Campanha"] = self.df["Campaign name"].apply(
            lambda x: buscar_mapping(self.mapping_sigla, x)
        )

    def renomear_colunas(self):
        """
        Renomeia a coluna "Placement" para "Posicionamento".
        """
        if "Placement" in self.df.columns:
            self.df.rename(columns={"Placement": "Posicionamento"}, inplace=True)
        else:
            logging.warning("Coluna 'Placement' não encontrada para renomear.")

    def reordenar_colunas(self):
        """
        Reordena as colunas para o modelo de destino:
        Veiculo, Campanha, ID_Campanha, Posicionamento, Alcance, Impressões, ID
        """
        ordem = ["Veiculo", "Campanha", "ID_Campanha", "Posicionamento", "Alcance", "Impressões", "ID"]
        self.df = self.df[[col for col in ordem if col in self.df.columns]]

    def gerar_id(self):
        """
        Gera uma coluna 'ID' única combinando os campos Campanha, Posicionamento, Alcance e Impressões.
        """
        required_cols = ["Campanha", "Posicionamento", "Alcance", "Impressões"]
        for col in required_cols:
            if col not in self.df.columns:
                logging.error(f"Coluna necessária para gerar ID não encontrada: {col}")
        self.df["ID"] = self.df.apply(
            lambda row: f"{row['Campanha']}-{row['Posicionamento']}-{row['Alcance']}-{row['Impressões']}",
            axis=1
        )

    def processar(self):
        self.ajustar_tipos()
        self.aplicar_parametrizacao_campanha()
        self.renomear_colunas()
        self.reordenar_colunas()
        self.gerar_id()
        return self.df
