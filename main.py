# main.py

import argparse
import os
import sys
from typing import List, Optional

import yaml

from transform.extract.sheets_fetcher import SheetsFetcher
from transform.transform.platforms.meta import transform_meta
from transform.transform.platforms.linkedin import transform_linkedin  
from transform.transform.platforms.tiktok import transform_tiktok

# 1) Carrega configuração de abas
with open("sheets_config.yaml", "r") as f:
    SHEETS_TO_FETCH = yaml.safe_load(f)

# 2) Instancia o fetcher (uma só vez)
fetcher = SheetsFetcher(
    creds_path=os.getenv("GOOGLE_CREDS_PATH", "creds.json"),
    spreadsheet_id=os.getenv("SPREADSHEET_ID", "1jPFLqg7HIDZoxCwQacMpCS_bCKfRsnisvEniiHLljiE"),
)


def run_platform_pipeline(platform: str, fetcher: SheetsFetcher):
    """Execute pipeline para uma plataforma específica."""
    if platform not in SHEETS_TO_FETCH:
        print(f"❌ Plataforma '{platform}' não encontrada em sheets_config.yaml")
        return
    
    print(f"🚀 Executando pipeline para plataforma: {platform}")
    
    # Extração das abas da plataforma
    sheets_for_platform = SHEETS_TO_FETCH[platform]
    print(f"📥 Extraindo abas: {sheets_for_platform}")
    
    # Aqui você implementaria a lógica específica da plataforma
    # Por enquanto, apenas demonstrativo
    try:
        dfs = fetcher.get(sheets_for_platform)
        print(f"✅ {len(dfs)} abas extraídas com sucesso para {platform}")
        
        # Aplicar transformação específica da plataforma
        if platform == "meta":
            for sheet_name, df in dfs.items():
                df_transformed = transform_meta(df)
                print(f"🔄 Transformação Meta aplicada para {sheet_name}")
        elif platform == "linkedin":
            for sheet_name, df in dfs.items():
                df_transformed = transform_linkedin(df)
                print(f"🔄 Transformação LinkedIn aplicada para {sheet_name}")
        elif platform == "tiktok":
            for sheet_name, df in dfs.items():
                df_transformed = transform_tiktok(df)
                print(f"🔄 Transformação TikTok aplicada para {sheet_name}")
        
        print(f"✅ Pipeline {platform} concluído com sucesso")
        
    except Exception as e:
        print(f"❌ Erro no pipeline {platform}: {str(e)}")


def parse_platforms(platform_arg: str) -> List[str]:
    """Parse do argumento --platform para lista de plataformas."""
    if platform_arg == "all":
        return list(SHEETS_TO_FETCH.keys())
    return [p.strip() for p in platform_arg.split(",")]


def main():
    parser = argparse.ArgumentParser(description="ETL Debrito - Pipeline por plataforma")
    parser.add_argument(
        "--platform",
        default="all", 
        help="Plataforma(s) para executar: meta,linkedin,tiktok ou 'all' (default: all)"
    )
    
    args = parser.parse_args()
    
    # Parse das plataformas solicitadas
    platforms = parse_platforms(args.platform)
    
    print(f"🎯 Executando ETL para plataformas: {', '.join(platforms)}")
    
    # Executa pipeline para cada plataforma solicitada
    for platform in platforms:
        run_platform_pipeline(platform, fetcher)


if __name__ == "__main__":
    main()
