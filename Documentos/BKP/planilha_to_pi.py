import pandas as pd
from io import StringIO

# Simulando o texto que você colou (apenas os dados, sem cabeçalho).
dados = """21/3/2025	14/4/2025	Alright	programatica	Inova Pantanal	Ação	Alcance	Nacional	cpm		as 25+ interesse em negócios sustentáveis: MT e MS	David Cantidio	18/03/2025	2025_3_INOVA PANTANAL_ALC_COMERCIALIZAÇÃO_CPM	2025_3_BR_ALC_CPM_AS 25+ INTERESSE EM NEGÓCIOS SUSTENTÁVEIS: MT E MS	2025_3_BR_300X250_PANTANAL_300X250_ACAO_DBT_SBRAE_2025_PAN0179	https://programas.sebraestartups.com.br/in/pantanaltr25?utm_source=alright&utm_medium=programatica&utm_campaign=dbt_sbrae_2025_pan&editoria=Comercialização&utm_term=300x250-dbt&utm_content=dbt_sbrae_2025_pan0179	Impulsionar	Comercialização	pantanal_300x250	300x250		saiba mais	pantanal_300x250		https://programas.sebraestartups.com.br/in/pantanaltr25	
21/3/2025	14/4/2025	Alright	programatica	Inova Pantanal	Ação	Alcance	Nacional	cpm		as 25+ interesse em negócios sustentáveis: MT e MS	David Cantidio	18/03/2025	2025_3_INOVA PANTANAL_ALC_COMERCIALIZAÇÃO_CPM	2025_3_BR_ALC_CPM_AS 25+ INTERESSE EM NEGÓCIOS SUSTENTÁVEIS: MT E MS	2025_3_BR_300X600_PANTANAL_300X600_ACAO_DBT_SBRAE_2025_PAN0180	https://programas.sebraestartups.com.br/in/pantanaltr25?utm_source=alright&utm_medium=programatica&utm_campaign=dbt_sbrae_2025_pan&editoria=Comercialização&utm_term=300x600-dbt&utm_content=dbt_sbrae_2025_pan0180	Impulsionar	Comercialização	pantanal_300x600	300x600		saiba mais	pantanal_300x600		https://programas.sebraestartups.com.br/in/pantanaltr25	
21/3/2025	14/4/2025	Alright	programatica	Inova Pantanal	Ação	Alcance	Nacional	cpm		as 25+ interesse em negócios sustentáveis: MT e MS	David Cantidio	18/03/2025	2025_3_INOVA PANTANAL_ALC_COMERCIALIZAÇÃO_CPM	2025_3_BR_ALC_CPM_AS 25+ INTERESSE EM NEGÓCIOS SUSTENTÁVEIS: MT E MS	2025_3_BR_320X480_PANTANAL_320X480_ACAO_DBT_SBRAE_2025_PAN0181	https://programas.sebraestartups.com.br/in/pantanaltr25?utm_source=alright&utm_medium=programatica&utm_campaign=dbt_sbrae_2025_pan&editoria=Comercialização&utm_term=320x480-dbt&utm_content=dbt_sbrae_2025_pan0181	Impulsionar	Comercialização	pantanal_320x480	320x480		saiba mais	pantanal_320x480		https://programas.sebraestartups.com.br/in/pantanaltr25	
21/3/2025	14/4/2025	Alright	programatica	Inova Pantanal	Ação	Alcance	Nacional	cpm		as 25+ interesse em negócios sustentáveis: MT e MS	David Cantidio	18/03/2025	2025_3_INOVA PANTANAL_ALC_COMERCIALIZAÇÃO_CPM	2025_3_BR_ALC_CPM_AS 25+ INTERESSE EM NEGÓCIOS SUSTENTÁVEIS: MT E MS	2025_3_BR_728X90_PANTANAL_728X90_ACAO_DBT_SBRAE_2025_PAN0182	https://programas.sebraestartups.com.br/in/pantanaltr25?utm_source=alright&utm_medium=programatica&utm_campaign=dbt_sbrae_2025_pan&editoria=Comercialização&utm_term=728x90-dbt&utm_content=dbt_sbrae_2025_pan0182	Impulsionar	Comercialização	pantanal_728x90	728x90		saiba mais	pantanal_728x90		https://programas.sebraestartups.com.br/in/pantanaltr25	
21/3/2025	4/14/2025	Alright	programatica	Inova Pantanal	Ação	Alcance	Nacional	cpm		as 25+ interesse em negócios sustentáveis: MT e MS	David Cantidio	18/03/2025	2025_3_INOVA PANTANAL_ALC_COMERCIALIZAÇÃO_CPM	2025_3_BR_ALC_CPM_AS 25+ INTERESSE EM NEGÓCIOS SUSTENTÁVEIS: MT e MS	2025_3_BR_970X250_PANTANAL_970X250_ACAO_DBT_SBRAE_2025_PAN0183	https://programas.sebraestartups.com.br/in/pantanaltr25?utm_source=alright&utm_medium=programatica&utm_campaign=dbt_sbrae_2025_pan&editoria=Comercialização&utm_term=970x250-dbt&utm_content=dbt_sbrae_2025_pan0183	Impulsionar	Comercialização	pantanal_970x250	970x250		saiba mais	pantanal_970x250		https://programas.sebraestartups.com.br/in/pantanaltr25	
21/3/2025	14/4/2025	Serasa Experian	programatica	Inova Pantanal	Ação	Alcance	Nacional	cpm		as 25+ interesse em negócios sustentáveis: MT e MS	David Cantidio	18/03/2025	2025_3_INOVA PANTANAL_ALC_COMERCIALIZAÇÃO_CPM	2025_3_BR_ALC_CPM_AS 25+ INTERESSE EM NEGÓCIOS SUSTENTÁVEIS: MT e MS	2025_3_BR_160X600_PANTANAL_160X600_ACAO_DBT_SBRAE_2025_PAN0184	https://programas.sebraestartups.com.br/in/pantanaltr25?utm_source=serasa_experian&utm_medium=programatica&utm_campaign=dbt_sbrae_2025_pan&editoria=Comercialização&utm_term=160x600-dbt&utm_content=dbt_sbrae_2025_pan0184	Impulsionar	Comercialização	pantanal_160x600	160x600		saiba mais	pantanal_300x250		https://programas.sebraestartups.com.br/in/pantanaltr25	
21/3/2025	14/4/2025	Serasa Experian	programatica	Inova Pantanal	Ação	Alcance	Nacional	cpm		as 25+ interesse em negócios sustentáveis: MT e MS	David Cantidio	18/03/2025	2025_3_INOVA PANTANAL_ALC_COMERCIALIZAÇÃO_CPM	2025_3_BR_ALC_CPM_AS 25+ INTERESSE EM NEGÓCIOS SUSTENTÁVEIS: MT e MS	2025_3_BR_300X250_PANTANAL_300X250_ACAO_DBT_SBRAE_2025_PAN0185	https://programas.sebraestartups.com.br/in/pantanaltr25?utm_source=serasa_experian&utm_medium=programatica&utm_campaign=dbt_sbrae_2025_pan&editoria=Comercialização&utm_term=300x250-dbt&utm_content=dbt_sbrae_2025_pan0185	Impulsionar	Comercialização	pantanal_300x250	300x250		saiba mais	pantanal_300x600		https://programas.sebraestartups.com.br/in/pantanaltr25	
21/3/2025	14/4/2025	Serasa Experian	programatica	Inova Pantanal	Ação	Alcance	Nacional	cpm		as 25+ interesse em negócios sustentáveis: MT e MS	David Cantidio	18/03/2025	2025_3_INOVA PANTANAL_ALC_COMERCIALIZAÇÃO_CPM	2025_3_BR_ALC_CPM_AS 25+ INTERESSE EM NEGÓCIOS SUSTENTÁVEIS: MT e MS	2025_3_BR_728X90_PANTANAL_728X90_ACAO_DBT_SBRAE_2025_PAN0186	https://programas.sebraestartups.com.br/in/pantanaltr25?utm_source=serasa_experian&utm_medium=programatica&utm_campaign=dbt_sbrae_2025_pan&editoria=Comercialização&utm_term=728x90-dbt&utm_content=dbt_sbrae_2025_pan0186	Impulsionar	Comercialização	pantanal_728x90	728x90		saiba mais	pantanal_320x480		https://programas.sebraestartups.com.br/in/pantanaltr25	
21/3/2025	14/4/2025	Serasa Experian	programatica	Inova Pantanal	Ação	Alcance	Nacional	cpm		as 25+ interesse em negócios sustentáveis: MT e MS	David Cantidio	18/03/2025	2025_3_INOVA PANTANAL_ALC_COMERCIALIZAÇÃO_CPM	2025_3_BR_ALC_CPM_AS 25+ INTERESSE EM NEGÓCIOS SUSTENTÁVEIS: MT e MS	2025_3_BR_970X90_PANTANAL_970X90_ACAO_DBT_SBRAE_2025_PAN0187	https://programas.sebraestartups.com.br/in/pantanaltr25?utm_source=serasa_experian&utm_medium=programatica&utm_campaign=dbt_sbrae_2025_pan&editoria=Comercialização&utm_term=970x90-dbt&utm_content=dbt_sbrae_2025_pan0187	Impulsionar	Comercialização	pantanal_970x90	970x90		saiba mais	pantanal_728x90		https://programas.sebraestartups.com.br/in/pantanaltr25	
21/3/2025	14/4/2025	Alright	programatica	Inova Cerrado e Pantanal	Ação	Alcance	Nacional	cpm		as 25+ interesse em negócios sustentáveis: MT e MS, MA, TO, BA, DF, GO, MG, PI	David Cantidio	18/03/2025	2025_3_INOVA CERRADO E PANTANAL_ALC_COMERCIALIZAÇÃO_CPM	2025_3_BR_ALC_CPM_AS 25+ INTERESSE EM NEGÓCIOS SUSTENTÁVEIS: MT e MS, MA, TO, BA, DF, GO, MG, PI	2025_3_BR_300X250_PANTANAL_E_CERRADO300X250_ACAO_DBT_SBRAE_2025_CER_PAN0188	https://programas.sebraestartups.com.br/in/pantanaltr25?utm_source=alright&utm_medium=programatica&utm_campaign=dbt_sbrae_2025_cer_pan&editoria=Comercialização&utm_term=300x250-dbt&utm_content=dbt_sbrae_2025_cer_pan0188	Impulsionar	Comercialização	pantanal_e_cerrado300x250	300x250		saiba mais	pantanal_e_cerrado300x250		https://programas.sebraestartups.com.br/in/pantanaltr25	
21/3/2025	14/4/2025	Alright	programatica	Inova Cerrado e Pantanal	Ação	Alcance	Nacional	cpm		as 25+ interesse em negócios sustentáveis: MT e MS, MA, TO, BA, DF, GO, MG, PI	David Cantidio	18/03/2025	2025_3_INOVA CERRADO E PANTANAL_ALC_COMERCIALIZAÇÃO_CPM	2025_3_BR_ALC_CPM_AS 25+ INTERESSE EM NEGÓCIOS SUSTENTÁVEIS: MT e MS, MA, TO, BA, DF, GO, MG, PI	2025_3_BR_300X600_PANTANAL_E_CERRADO300X600_ACAO_DBT_SBRAE_2025_CER_PAN0189	https://programas.sebraestartups.com.br/in/pantanaltr25?utm_source=alright&utm_medium=programatica&utm_campaign=dbt_sbrae_2025_cer_pan&editoria=Comercialização&utm_term=300x600-dbt&utm_content=dbt_sbrae_2025_cer_pan0189	Impulsionar	Comercialização	pantanal_e_cerrado300x600	300x600		saiba mais	pantanal_e_cerrado300x600		https://programas.sebraestartups.com.br/in/pantanaltr25	
21/3/2025	14/4/2025	Alright	programatica	Inova Cerrado e Pantanal	Ação	Alcance	Nacional	cpm		as 25+ interesse em negócios sustentáveis: MT e MS, MA, TO, BA, DF, GO, MG, PI	David Cantidio	18/03/2025	2025_3_INOVA CERRADO E PANTANAL_ALC_COMERCIALIZAÇÃO_CPM	2025_3_BR_ALC_CPM_AS 25+ INTERESSE EM NEGÓCIOS SUSTENTÁVEIS: MT e MS, MA, TO, BA, DF, GO, MG, PI	2025_3_BR_320X480_PANTANAL_E_CERRADO320X480_ACAO_DBT_SBRAE_2025_CER_PAN0190	https://programas.sebraestartups.com.br/in/pantanaltr25?utm_source=alright&utm_medium=programatica&utm_campaign=dbt_sbrae_2025_cer_pan&editoria=Comercialização&utm_term=320x480-dbt&utm_content=dbt_sbrae_2025_cer_pan0190	Impulsionar	Comercialização	pantanal_e_cerrado320x480	320x480		saiba mais	pantanal_e_cerrado320x480		https://programas.sebraestartups.com.br/in/pantanaltr25	
21/3/2025	14/4/2025	Alright	programatica	Inova Cerrado e Pantanal	Ação	Alcance	Nacional	cpm		as 25+ interesse em negócios sustentáveis: MT e MS, MA, TO, BA, DF, GO, MG, PI	David Cantidio	18/03/2025	2025_3_INOVA CERRADO E PANTANAL_ALC_COMERCIALIZAÇÃO_CPM	2025_3_BR_ALC_CPM_AS 25+ INTERESSE EM NEGÓCIOS SUSTENTÁVEIS: MT e MS, MA, TO, BA, DF, GO, MG, PI	2025_3_BR_728X90_PANTANAL_E_CERRADO728X90_ACAO_DBT_SBRAE_2025_CER_PAN0191	https://programas.sebraestartups.com.br/in/pantanaltr25?utm_source=alright&utm_medium=programatica&utm_campaign=dbt_sbrae_2025_cer_pan&editoria=Comercialização&utm_term=728x90-dbt&utm_content=dbt_sbrae_2025_cer_pan0191	Impulsionar	Comercialização	pantanal_e_cerrado728x90	728x90		saiba mais	pantanal_e_cerrado728x90		https://programas.sebraestartups.com.br/in/pantanaltr25	
21/3/2025	4/14/2025	Alright	programatica	Inova Cerrado e Pantanal	Ação	Alcance	Nacional	cpm		as 25+ interesse em negócios sustentáveis: MT e MS, MA, TO, BA, DF, GO, MG, PI	David Cantidio	18/03/2025	2025_3_INOVA CERRADO E PANTANAL_ALC_COMERCIALIZAÇÃO_CPM	2025_3_BR_ALC_CPM_AS 25+ INTERESSE EM NEGÓCIOS SUSTENTÁVEIS: MT e MS, MA, TO, BA, DF, GO, MG, PI	2025_3_BR_970X250_PANTANAL_E_CERRADO970X250_ACAO_DBT_SBRAE_2025_CER_PAN0192	https://programas.sebraestartups.com.br/in/pantanaltr25?utm_source=alright&utm_medium=programatica&utm_campaign=dbt_sbrae_2025_cer_pan&editoria=Comercialização&utm_term=970x250-dbt&utm_content=dbt_sbrae_2025_cer_pan0192	Impulsionar	Comercialização	pantanal_e_cerrado970x250	970x250		saiba mais	pantanal_e_cerrado970x250		https://programas.sebraestartups.com.br/in/pantanaltr25	
21/3/2025	14/4/2025	Serasa Experian	programatica	Inova Cerrado e Pantanal	Ação	Alcance	Nacional	cpm		as 25+ interesse em negócios sustentáveis: MT e MS, MA, TO, BA, DF, GO, MG, PI	David Cantidio	18/03/2025	2025_3_INOVA CERRADO E PANTANAL_ALC_COMERCIALIZAÇÃO_CPM	2025_3_BR_ALC_CPM_AS 25+ INTERESSE EM NEGÓCIOS SUSTENTÁVEIS: MT e MS, MA, TO, BA, DF, GO, MG, PI	2025_3_BR_160X600_PANTANAL_E_CERRADO160X600_ACAO_DBT_SBRAE_2025_CER_PAN0193	https://programas.sebraestartups.com.br/in/pantanaltr25?utm_source=serasa_experian&utm_medium=programatica&utm_campaign=dbt_sbrae_2025_cer_pan&editoria=Comercialização&utm_term=160x600-dbt&utm_content=dbt_sbrae_2025_cer_pan0193	Impulsionar	Comercialização	pantanal_e_cerrado160x600	160x600		saiba mais	pantanal_e_cerrado160x600		https://programas.sebraestartups.com.br/in/pantanaltr25	
21/3/2025	14/4/2025	Serasa Experian	programatica	Inova Cerrado e Pantanal	Ação	Alcance	Nacional	cpm		as 25+ interesse em negócios sustentáveis: MT e MS, MA, TO, BA, DF, GO, MG, PI	David Cantidio	18/03/2025	2025_3_INOVA CERRADO E PANTANAL_ALC_COMERCIALIZAÇÃO_CPM	2025_3_BR_ALC_CPM_AS 25+ INTERESSE EM NEGÓCIOS SUSTENTÁVEIS: MT e MS, MA, TO, BA, DF, GO, MG, PI	2025_3_BR_300X250_PANTANAL_E_CERRADO300X250_ACAO_DBT_SBRAE_2025_CER_PAN0194	https://programas.sebraestartups.com.br/in/pantanaltr25?utm_source=serasa_experian&utm_medium=programatica&utm_campaign=dbt_sbrae_2025_cer_pan&editoria=Comercialização&utm_term=300x250-dbt&utm_content=dbt_sbrae_2025_cer_pan0194	Impulsionar	Comercialização	pantanal_e_cerrado300x250	300x250		saiba mais	pantanal_e_cerrado300x250		https://programas.sebraestartups.com.br/in/pantanaltr25	
21/3/2025	14/4/2025	Serasa Experian	programatica	Inova Cerrado e Pantanal	Ação	Alcance	Nacional	cpm		as 25+ interesse em negócios sustentáveis: MT e MS, MA, TO, BA, DF, GO, MG, PI	David Cantidio	18/03/2025	2025_3_INOVA CERRADO E PANTANAL_ALC_COMERCIALIZAÇÃO_CPM	2025_3_BR_ALC_CPM_AS 25+ INTERESSE EM NEGÓCIOS SUSTENTÁVEIS: MT e MS, MA, TO, BA, DF, GO, MG, PI	2025_3_BR_728X90_PANTANAL_E_CERRADO728X90_ACAO_DBT_SBRAE_2025_CER_PAN0195	https://programas.sebraestartups.com.br/in/pantanaltr25?utm_source=serasa_experian&utm_medium=programatica&utm_campaign=dbt_sbrae_2025_cer_pan&editoria=Comercialização&utm_term=728x90-dbt&utm_content=dbt_sbrae_2025_cer_pan0195	Impulsionar	Comercialização	pantanal_e_cerrado728x90	728x90		saiba mais	pantanal_e_cerrado728x90		https://programas.sebraestartups.com.br/in/pantanaltr25	
21/3/2025	14/4/2025	Serasa Experian	programatica	Inova Cerrado e Pantanal	Ação	Alcance	Nacional	cpm		as 25+ interesse em negócios sustentáveis: MT e MS, MA, TO, BA, DF, GO, MG, PI	David Cantidio	18/03/2025	2025_3_INOVA CERRADO E PANTANAL_ALC_COMERCIALIZAÇÃO_CPM	2025_3_BR_ALC_CPM_AS 25+ INTERESSE EM NEGÓCIOS SUSTENTÁVEIS: MT e MS, MA, TO, BA, DF, GO, MG, PI	2025_3_BR_970X90_PANTANAL_E_CERRADO970X90_ACAO_DBT_SBRAE_2025_CER_PAN0196	https://programas.sebraestartups.com.br/in/pantanaltr25?utm_source=serasa_experian&utm_medium=programatica&utm_campaign=dbt_sbrae_2025_cer_pan&editoria=Comercialização&utm_term=970x90-dbt&utm_content=dbt_sbrae_2025_cer_pan0196	Impulsionar	Comercialização	pantanal_e_cerrado970x90	970x90		saiba mais	pantanal_e_cerrado970x90		https://programas.sebraestartups.com.br/in/pantanaltr25	
"""

# Cabeçalho (colunas) conforme você informou
colunas = [
    "Inicio",
    "Fim",
    "Veiculo",
    "Categoria",
    "Campanha",
    "Remarketing/Ação",
    "Objetivo de Mídia",
    "Região",
    "Tipo de compra",
    "Link das Redes",
    "Segmentação",
    "Responsável",
    "Data de preechimento",
    "TAXONOMIA - Campanha",
    "TAXONOMIA - \"CONJUNTO DE ANÚNCIO\"",
    "TAXONOMIA - \"CRIATIVO\"",
    "URL - Parametrizada",
    "Status",
    "Editoria",
    "Nome criativo",
    "Formato",
    "Conteúdo",
    "CTA",
    "Título peça",
    "Legenda peça",
    "URL de destino",
    "Peça na rede"
]

# Criando DataFrame
df = pd.read_csv(StringIO(dados), sep="\t", names=colunas, engine="python")

# Converte datas que estejam no padrão dd/mm/aaaa ou mm/dd/aaaa
# Tenta converter automaticamente usando "infer_datetime_format=True"
df["Inicio"] = pd.to_datetime(df["Inicio"], dayfirst=True, errors="coerce")
df["Fim"] = pd.to_datetime(df["Fim"], dayfirst=True, errors="coerce")

# Se quiser deixar num formato único "DD/MM/YYYY":
df["Inicio"] = df["Inicio"].dt.strftime("%d/%m/%Y")
df["Fim"] = df["Fim"].dt.strftime("%d/%m/%Y")

# Vamos ordenar por Veiculo e por data de Início
df = df.sort_values(by=["Veiculo","Inicio"])

# Se a ideia é extrair só as colunas essenciais para o PI (por exemplo):
colunas_para_pi = [
    "Veiculo",
    "Inicio",
    "Fim",
    "Campanha",
    "Objetivo de Mídia",
    "Tipo de compra",
    "Segmentação",
    "Formato",
    "URL - Parametrizada",
    "CTA",
]

df_pi = df[colunas_para_pi]

# Visualizando
print(df_pi)

