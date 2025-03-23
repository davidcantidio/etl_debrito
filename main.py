from utils.google_sheets import carregar_aba_google_sheets

df_meta = carregar_aba_google_sheets(
    creds_path='/home/debrito/Documentos/ETL/ELET_ETL_Projeto/creds.json',
    sheet_url='https://docs.google.com/spreadsheets/d/1DazUQxspLgT0utOFHcTINbFngXw7Fq0LOq6v4lRGixg/edit',
    nome_aba='Meta - Geral'
)

print(df_meta.head())
