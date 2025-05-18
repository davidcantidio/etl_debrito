# main.py

import os
import yaml
from extract.sheets_fetcher import SheetsFetcher
from treat.meta import treat_meta  # Exemplo de função de treat
# from treat.tiktok import treat_tiktok
# etc.

# 1) Carrega configuração de abas
with open("sheets_config.yaml", "r") as f:
    SHEETS_TO_FETCH = yaml.safe_load(f)

# 2) Instancia o fetcher (uma só vez)
fetcher = SheetsFetcher(
    creds_path=os.getenv("GOOGLE_CREDS_PATH", "creds.json"),
    spreadsheet_id=os.getenv("SPREADSHEET_ID")
)

def main():
    # 3) Extração
    dfs_meta = fetcher.get(SHEETS_TO_FETCH["meta"])      # lista de abas meta
    dfs_tiktok = fetcher.get(SHEETS_TO_FETCH["tiktok"])  # lista de abas tiktok
    # ...

    # 4) Tratamento
    df_meta_geral = treat_meta(dfs_meta)                 # implementado em treat/meta.py
    # df_tiktok_geral = treat_tiktok(dfs_tiktok)
    # ...

    # 5) Load (exemplo)
    # from load.common import write_to_sheet
    # write_to_sheet(df_meta_geral, "modeloGeral")
    # ...

if __name__ == "__main__":
    main()
