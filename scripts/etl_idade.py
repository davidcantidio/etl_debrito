from scripts.etl_geral import BaseGeralETL
from utils.fields_lists import AGE_MODEL_COLUMN_ORDER
from utils.organizar_dataframe import reordenar_colunas_para_modelo
from utils.normalize import normalizar_faixa_etaria

class BaseIdadeETL(BaseGeralETL):
    def ajustar_tipos_e_calculos(self):
        # Aplica as transformações gerais da classe base
        super().ajustar_tipos_e_calculos()
        # Renomeia a coluna que veio como 'Faixa_Etaria' para 'Idade'
        if 'Faixa_Etaria' in self.df.columns:
            self.df.rename(columns={'Faixa_Etaria': 'Idade'}, inplace=True)
        # Aplica a normalização no campo 'Idade'
        if 'Idade' in self.df.columns:
            self.df['Idade'] = self.df['Idade'].apply(normalizar_faixa_etaria)
    
    def criar_veiculo(self):
        # Utiliza a implementação padrão para preencher 'Veiculo' e 'ID_Veiculo'
        super().criar_veiculo()
    
    def reordenar_colunas_para_modelo(self):
        # Reordena as colunas conforme o modelo específico para Idade
        self.df = reordenar_colunas_para_modelo(self.df, AGE_MODEL_COLUMN_ORDER)


# Classes específicas para cada plataforma no contexto do ETL de Idade
class MetaIdadeETL(BaseIdadeETL):
    # Se necessário, sobrescreva métodos para tratar peculiaridades da Meta para Idade
    pass

class TikTokIdadeETL(BaseIdadeETL):
    # Se necessário, sobrescreva métodos para o TikTok
    pass

class LinkedinIdadeETL(BaseIdadeETL):
    # Se necessário, sobrescreva métodos para o Linkedin
    pass

class PinterestIdadeETL(BaseIdadeETL):
    # Para Pinterest, pode ser necessário tratamentos adicionais – por exemplo, para ajustar datas
    def ajustar_tipos_e_calculos(self):
        super().ajustar_tipos_e_calculos()
        from utils.datas import generate_pinterest_dates
        self.df = generate_pinterest_dates(self.df)
