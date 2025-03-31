import pandas as pd
import numpy as np
import logging
from utils.campanha_mapper import buscar_mapping

class etl_alcance_meta:
    def __init__(self, df, mapping_campanha=None, mapping_sigla=None):
        """
        Inicializa o ETL para os dados de META – Alcance.
        
        Parâmetros:
            df (pandas.DataFrame): DataFrame lido da aba de origem (Meta – Alcance)
            mapping_campanha (dict): dicionário que mapeia "Campaign name" (normalizado)
                                     → "Campanha" (coluna do destino), obtido da BI_PARAMETRIZAÇÃO
            mapping_sigla (dict): dicionário que mapeia "Campaign name" (normalizado)
                                  → "ID_Campanha" (coluna do destino), obtido da BI_PARAMETRIZAÇÃO
        """
        self.df = df.copy()
        # Remove espaços extras dos nomes das colunas
        self.df.columns = [col.strip() for col in self.df.columns]
        self.mapping_campanha = mapping_campanha or {}
        self.mapping_sigla = mapping_sigla or {}

    def ajustar_tipos(self):
        """
        - Busca as colunas 'Reach' e 'Impressions' (case-insensitive) e as converte para int.
        - Converte 'Campaign name', 'Placement', 'Campaign ID' para string (se existirem).
        - Cria as colunas "Alcance" e "Impressões" a partir das originais.
        - Se existir 'Date', converte para datetime e armazena em 'Data'.
        - Cria 'Numero' (0) e 'Veiculo' = 'Meta'.
        """
        # Mapeia colunas para minúsculo
        columns_norm = {col.lower(): col for col in self.df.columns}

        # 1) Alcance e Impressões
        reach_key = columns_norm.get("reach", None)
        imp_key = columns_norm.get("impressions", None)
        
        if reach_key:
            self.df[reach_key] = pd.to_numeric(self.df[reach_key], errors='coerce').fillna(0).astype(int)
        else:
            logging.error("Coluna 'Reach' não encontrada na origem.")
        if imp_key:
            self.df[imp_key] = pd.to_numeric(self.df[imp_key], errors='coerce').fillna(0).astype(int)
        else:
            logging.error("Coluna 'Impressions' não encontrada na origem.")
        
        # 2) Converte colunas textuais se existirem
        for col in ['Veiculo', 'Campaign name', 'Placement', 'Campaign ID']:
            if col in self.df.columns:
                self.df[col] = self.df[col].astype(str).str.strip()
            else:
                logging.warning(f"Coluna '{col}' não encontrada na origem.")
        
        # 3) Cria colunas de destino "Alcance" e "Impressões" a partir das originais
        self.df["Alcance"] = self.df[reach_key] if reach_key else 0
        self.df["Impressões"] = self.df[imp_key] if imp_key else 0

        # 4) Se existir 'Date', converte para datetime e guarda em 'Data'
        if "Date" in self.df.columns:
            self.df["Date"] = pd.to_datetime(self.df["Date"], errors='coerce')
            self.df["Data"] = self.df["Date"]
        else:
            logging.warning("Coluna 'Date' não encontrada; 'Data' ficará ausente.")

        # 5) Cria 'Numero' (inicializa com zero)
        self.df["Numero"] = 0

        # 6) Define 'Veiculo' = 'Meta'
        self.df["Veiculo"] = "Meta"

    def aplicar_parametrizacao_campanha(self):
        """
        Preenche 'Campanha' e 'ID_Campanha' via lookup dos dicionários,
        usando o valor em 'Campaign name'.
        Se não achar correspondência, mantém o valor original em 'Campanha'
        e None em 'ID_Campanha'.
        """
        if "Campaign name" not in self.df.columns:
            logging.warning("'Campaign name' não existe no DataFrame para lookup.")
            return
        
        self.df["Campanha"] = self.df["Campaign name"].apply(
            lambda x: buscar_mapping(self.mapping_campanha, x) or x
        )
        self.df["ID_Campanha"] = self.df["Campaign name"].apply(
            lambda x: buscar_mapping(self.mapping_sigla, x)
        )

    def renomear_colunas(self):
        """
        Renomeia 'Placement' para 'Posicionamento', se existir.
        """
        if "Placement" in self.df.columns:
            self.df.rename(columns={"Placement": "Posicionamento"}, inplace=True)
        else:
            logging.warning("Coluna 'Placement' não encontrada para renomear.")

    def reordenar_colunas(self):
        """
        Define a ordem final para:
          Numero, Data, Veiculo, Campanha, ID_Campanha, Posicionamento, Alcance, Impressões, ID
        """
        ordem = [
            "Numero",
            "Data",
            "Veiculo",
            "Campanha",
            "ID_Campanha",
            "Posicionamento",
            "Alcance",
            "Impressões",
            "ID"
        ]
        self.df = self.df[[col for col in ordem if col in self.df.columns]]

    def gerar_id(self):
        """
        Gera 'ID' combinando Campanha, Posicionamento, Alcance, Impressões.
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
        """
        Executa o pipeline do ETL Alcance Meta:
          1) ajustar_tipos (converte colunas, cria 'Data', 'Numero', 'Veiculo')
          2) aplicar_parametrizacao_campanha (lookup para criar Campanha/ID_Campanha)
          3) renomear_colunas (Placement -> Posicionamento)
          4) reordenar_colunas
          5) gerar_id
        """
        self.ajustar_tipos()
        self.aplicar_parametrizacao_campanha()
        self.renomear_colunas()
        self.reordenar_colunas()
        self.gerar_id()
        return self.df
