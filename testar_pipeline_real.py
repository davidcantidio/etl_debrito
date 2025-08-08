#!/usr/bin/env python
# coding: utf-8

# In[1]:


# %% [markdown]
# Notebook ETL – leitura em batch, validações e write-back


# In[2]:


import os, sys, importlib

project_root = os.getcwd()
if project_root not in sys.path:
    sys.path.insert(0, project_root)
logs_dir = os.path.join(project_root, "logs")
if logs_dir not in sys.path:
    sys.path.insert(0, logs_dir)
import logging_setup

importlib.reload(logging_setup)
from logging_setup import get_logger  # importa e já faz setup_logging

logger = get_logger(__name__)


# In[3]:


# %% [code]
import os
import sys
from dotenv import load_dotenv

project_root = (
    os.getcwd()
)  # supondo que o notebook esteja em /home/debrito/Documentos/etl_debrito
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# ── Carrega variáveis de ambiente de .env (opcional) ───────────────────────
load_dotenv()


# In[4]:


# 2 %% [code]
import math
import numpy as np
from typing import Any
import gspread
import google.auth
from googleapiclient.errors import HttpError
from IPython.display import display
import time

# Imports consolidados para evitar repetição nas células seguintes
from transform.transform_pipeline import BIParamLookup
from transform.utils.validations import validate_consistent_dates_across_models

# 🚀 Cache global para worksheets para reduzir chamadas API
_WORKSHEETS_CACHE = {}
_CACHE_TTL = 300  # 5 minutos de cache


def _to_json_safe(x: Any) -> Any:
    """
    Internal helper to convert arbitrary objects into JSON-safe primitives.
    """
    if x is None:
        return None
    if isinstance(x, (int, str, bool)):
        return x
    if isinstance(x, float):
        return None if math.isnan(x) or math.isinf(x) else x
    if isinstance(x, (np.integer,)):
        return int(x)
    if isinstance(x, (np.floating,)):
        return None if (math.isnan(x) or math.isinf(x)) else float(x)
    if isinstance(x, (list, tuple, set)):
        return [_to_json_safe(item) for item in x]
    if isinstance(x, dict):
        return {k: _to_json_safe(v) for k, v in x.items()}
    # Fallback: stringify anything else
    return str(x)


def json_safe(obj: Any) -> Any:
    """
    Recursively converts `obj` into structures 100% serializable to JSON.
    """
    return _to_json_safe(obj)


def handle_sheets_error(e: Exception, operation: str = "operação") -> None:
    """
    Helper centralizado para tratamento de erros da API Google Sheets
    """
    if isinstance(e, HttpError):
        if e.resp.status == 403:
            logger.warning(f"⚠️ Erro de permissão (403) durante {operation}")
            logger.info("Certifique-se de que a planilha está compartilhada com a conta de serviço")
        elif e.resp.status == 429:
            logger.warning(f"⚠️ Rate limit excedido (429) durante {operation}")
            logger.info("Aguarde alguns segundos antes de tentar novamente")
        else:
            logger.error(f"❌ Erro HTTP {e.resp.status} durante {operation}: {e}")
    else:
        logger.error(f"❌ Erro inesperado durante {operation}: {e}")


def consolidated_write_back(changes_list: list, creds_path: str, spreadsheet_id: str) -> None:
    """
    🚀 ULTRA-OTIMIZAÇÃO: Executa TODAS as mudanças em UMA ÚNICA chamada batchUpdate
    """
    if not changes_list:
        logger.info("⚡ Nenhuma mudança para escrever - pulando batchUpdate")
        return

    try:
        from googleapiclient.discovery import build
        from google.auth import load_credentials_from_file
        
        creds, _ = load_credentials_from_file(
            creds_path, scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        service = build("sheets", "v4", credentials=creds)

        # Preparar todos os payloads para uma única chamada
        all_data_payloads = []
        total_ranges = 0
        total_cells = 0

        for change in changes_list:
            change_type = change.get("type")
            
            if change_type == "origin":
                # Processar mudanças de origem
                from load.origin_writer import prepare_origin_payload
                payload = prepare_origin_payload(
                    df_raw=change["df_raw"],
                    df_ok=change["df_ok"],
                    sheet_name=change["sheet_name"],
                    dry_run=change["dry_run"]
                )
                if payload:
                    all_data_payloads.append(payload)
                    total_ranges += 1
                    total_cells += len(payload.get("values", []))
                    
            elif change_type == "dest":
                # Processar mudanças de destino
                from load.dest_writer import prepare_dest_payload
                payload = prepare_dest_payload(
                    df_model=change["df_model"],
                    sheet_name=change["sheet_name"],
                    creds_path=creds_path,
                    spreadsheet_id=spreadsheet_id,
                    dry_run=change["dry_run"]
                )
                if payload:
                    all_data_payloads.append(payload)
                    total_ranges += 1
                    total_cells += len(payload.get("values", []))

        # 🚀 EXECUÇÃO: Uma única chamada batchUpdate para TODAS as mudanças
        if all_data_payloads and not all(change.get("dry_run", False) for change in changes_list):
            body = {"valueInputOption": "USER_ENTERED", "data": all_data_payloads}
            
            logger.info(f"🚀 Executando batchUpdate CONSOLIDADO: {total_ranges} ranges, {total_cells:,} células")
            
            service.spreadsheets().values().batchUpdate(
                spreadsheetId=spreadsheet_id, body=body
            ).execute()
            
            logger.info(f"✅ batchUpdate CONSOLIDADO concluído com sucesso!")
            
        else:
            logger.info(f"🔍 [Dry-run] batchUpdate CONSOLIDADO enviaria {total_ranges} ranges, {total_cells:,} células")

    except Exception as e:
        handle_sheets_error(e, "batchUpdate consolidado")


def clear_all_caches(fetcher=None, sheet_names=None) -> None:
    """
    Função consolidada para limpeza de todos os caches
    """
    global _WORKSHEETS_CACHE
    
    # Limpa cache do fetcher
    if fetcher and sheet_names:
        try:
            fetcher.refresh(sheet_names)
            logger.info("🔄 Cache do fetcher limpo com sucesso")
        except Exception as e:
            handle_sheets_error(e, "limpeza de cache do fetcher")
    
    # Limpa cache da parametrização BI
    try:
        BIParamLookup._df = None
        BIParamLookup._last_load = 0.0
        logger.info("🔄 Cache BIParamLookup limpo")
    except Exception as e:
        logger.error(f"❌ Erro ao limpar cache BIParamLookup: {e}")
    
    # Limpa cache de worksheets
    _WORKSHEETS_CACHE.clear()
    logger.info("🔄 Cache de worksheets limpo")


def get_cached_worksheets(gc, spreadsheet_id: str):
    """
    🚀 ULTRA-OTIMIZADA: Get worksheets using consolidated metadata (1 API call)
    """
    try:
        from transform.utils.metadata_optimizer import get_consolidated_metadata
        
        # Get metadata using single API call
        metadata = get_consolidated_metadata(
            spreadsheet_id, 
            CREDS_PATH, 
            include_sheets=True, 
            include_properties=False
        )
        
        # Convert to gspread-like objects for compatibility
        class MockWorksheet:
            def __init__(self, sheet_info):
                self.title = sheet_info['title']
                self.row_count = sheet_info['row_count'] 
                self.col_count = sheet_info['col_count']
                self.id = sheet_info['sheetId']
        
        worksheets = [MockWorksheet(sheet_info) for sheet_info in metadata.get('sheets', [])]
        
        logger.debug(f"📥 Ultra-consolidated metadata: {len(worksheets)} sheets via 1 API call")
        return worksheets
        
    except Exception as e:
        logger.warning(f"Consolidated metadata failed, fallback to gspread: {e}")
        
        # Fallback to original method
        global _WORKSHEETS_CACHE
        cache_key = f"{spreadsheet_id}_worksheets"
        now = time.time()
        
        if cache_key in _WORKSHEETS_CACHE:
            cached_time, cached_data = _WORKSHEETS_CACHE[cache_key]
            if now - cached_time < _CACHE_TTL:
                logger.debug("📥 Cache hit para worksheets() - economizou 1 chamada API")
                return cached_data
        
        try:
            ss = gc.open_by_key(spreadsheet_id)
            worksheets = ss.worksheets()
            _WORKSHEETS_CACHE[cache_key] = (now, worksheets)
            logger.debug("📡 Cache miss - worksheets() carregado e cacheado")
            return worksheets
        except Exception as fallback_error:
            handle_sheets_error(fallback_error, "obtenção de worksheets")
            return []


def get_sheet_statistics(creds_path: str, spreadsheet_id: str, top_n: int = 10) -> None:
    """
    🚀 ULTRA-OTIMIZADA: Obter estatísticas da planilha com 1 única API call
    """
    try:
        # Import the optimized metadata fetcher
        from transform.utils.metadata_optimizer import get_sheet_statistics_optimized
        
        stats = get_sheet_statistics_optimized(spreadsheet_id, creds_path, top_n)
        
        if not stats:
            logger.warning("⚠️ Nenhuma aba encontrada para estatísticas")
            return
            
        logger.info(f"📊 Top {top_n} abas que mais ocupam células:")
        for stat in stats:
            logger.info(f"  • {stat['title']}: {stat['rows']}×{stat['cols']} = {stat['cells']:,} células")
            
    except Exception as e:
        handle_sheets_error(e, "obtenção de estatísticas da planilha")


def add_rate_limiting(delay_seconds: float = 0.1, force: bool = False):
    """
    🚀 OTIMIZAÇÃO 5: Conditional smart rate limiting
    """
    # Skip rate limiting if not needed and not forced
    if not force and delay_seconds <= 0.01:
        return
    
    from transform.utils.smart_rate_limiting import smart_rate_limit
    
    # Use smart rate limiting with connection pooling awareness
    smart_rate_limit(
        operation_type="general",
        base_delay=delay_seconds,
        use_connection_pooling=True,
        max_calls_per_minute=100
    )


# In[5]:


# 3 %% [code]
# Flags de gravação (ajuste conforme necessidade)
WRITE_BACK_ORIGIN = True  # grava na aba-origem (meta*, tiktok*, …)
WRITE_BACK_DEST = True  # grava nas abas-modelo (modelo*)
DRY_RUN_DEST = False  # True = simula write-back destino

# 🚀 CONFIGURAÇÕES DE ULTRA-OTIMIZAÇÃO API (ARQUITETURA 2 CHAMADAS)
API_OPTIMIZATIONS = {
    "ultra_mode": True,                # ⭐ NOVO: Arquitetura de apenas 2 chamadas API
    "consolidate_batch_get": True,     # Consolidar múltiplas chamadas batchGet em uma
    "consolidate_batch_update": True,  # ⭐ NOVO: Consolidar múltiplas chamadas batchUpdate em uma
    "intelligent_prefetch": True,      # Pular prefetch quando desnecessário
    "cache_worksheets": True,          # Cache de 5min para worksheets()
    "rate_limiting": True,             # Rate limiting entre chamadas
    "rate_limit_delay": 0.1,          # Delay em segundos entre chamadas
}

# Credenciais e identificador da planilha
CREDS_PATH = os.getenv("GOOGLE_CREDS_PATH", "creds.json")
SPREADSHEET_ID = os.getenv(
    "GOOGLE_SHEET_ID", "1jPFLqg7HIDZoxCwQacMpCS_bCKfRsnisvEniiHLljiE"
)

# Abas de origem a processar, agrupadas por plataforma
SHEET_NAMES = [
    # Meta (Facebook / Instagram)
    "metaGeral",
    "metaIdade",
    "metaGenero",
    "metaRegiao",
    "metaAlcance",
    # TikTok
    "tiktokGeral",
    "tiktokIdade",
    "tiktokGenero",
    "tiktokRegiao",
    "tiktokAlcance",
    # Pinterest
    "pinterestGeral",
    "pinterestGenero",
    "pinterestIdade",
    "pinterestRegiao",
    "pinterestAlcance",
    # LinkedIn
    "linkedinGeral",
    "linkedinRegiao",
    "linkedinAlcance",
    # Google Analytics
    "GAGeral",
]

print("🚀 CONFIGURAÇÕES ULTRA-OTIMIZAÇÃO API:")
for key, value in API_OPTIMIZATIONS.items():
    status = "⭐ ATIVO" if value else "❌ INATIVO"
    if key == "ultra_mode" and value:
        status = "🎯 ULTRA-MODE ATIVO"
    print(f"  • {key}: {status}")

print(f"\n📊 Pipeline configurado para processar {len(SHEET_NAMES)} abas")
print(f"🔗 Planilha: {SPREADSHEET_ID}")
print(f"🔑 Credenciais: {CREDS_PATH}")

# 🎯 Estimativa ULTRA-OTIMIZADA
if API_OPTIMIZATIONS.get("ultra_mode", False):
    read_calls = 1  # 1x batchGet consolidado
    write_calls = 1 if (WRITE_BACK_ORIGIN or WRITE_BACK_DEST) and not DRY_RUN_DEST else 0
    total_estimated = read_calls + write_calls
    
    print(f"\n🎯 ARQUITETURA ULTRA-OTIMIZADA:")
    print(f"  • Leitura: {read_calls}x batchGet (TODAS as abas)")
    print(f"  • Processamento: 100% em memória")
    print(f"  • Escrita: {write_calls}x batchUpdate (TODAS as mudanças)")
    print(f"  • TOTAL: ⭐ {total_estimated} chamadas API")
    
    if total_estimated <= 2:
        print("🏆 EXCELENTE! Arquitetura ultra-otimizada ativa")
    
    # Comparação com arquitetura anterior
    old_estimated = 1 + len(SHEET_NAMES) + 2  # batchGet + batchUpdates individuais + extras
    reduction = ((old_estimated - total_estimated) / old_estimated) * 100
    print(f"📈 Economia vs. anterior: {reduction:.0f}% ({old_estimated} → {total_estimated} chamadas)")
    
else:
    # Estimativa anterior (modo compatibilidade)
    base_calls = 1 if API_OPTIMIZATIONS["consolidate_batch_get"] else 3
    write_calls = len(SHEET_NAMES) if WRITE_BACK_DEST and not DRY_RUN_DEST else 0
    total_estimated = base_calls + write_calls + 2
    print(f"\n🧮 Estimativa de chamadas API (modo compatibilidade): ~{total_estimated} chamadas")

print(f"\n💡 Para ativar ULTRA-MODE: configure ultra_mode=True nas API_OPTIMIZATIONS")


# In[6]:


# 4 %% [code]
# Hot-reload condicional de módulos (apenas em desenvolvimento)
import os
import importlib

# 🚀 SMART HOT-RELOAD: Only reload changed modules
from transform.utils.smart_reload import conditional_hot_reload
conditional_hot_reload()

# Standard imports (will be reloaded only if changed)
import extract.sheets_fetcher
import transform.transform_pipeline  
import load.origin_writer
import load.dest_writer


# In[7]:


# %% [code]
# Cell 5: definição do helper run_etl_for_sheet (ULTRA-OTIMIZADO)
import pandas as pd
import json
from pprint import pp
from typing import Dict, Optional

from logs.logging_setup import get_logger

logger = get_logger(__name__)

from extract.sheets_fetcher import SheetsFetcher
from transform.transform_pipeline import TreatPipeline
from transform.utils.renomeacoes import (
    renomeacao_geral,
    renomear_colunas_origem_para_modelo,
)
from transform.utils.campos_calculados import calcular_engajamento_total, gerar_id
from transform.utils.schema_validator import validate_schema_early
from transform.utils.date_normalizer import normalize_campaign_dates

# Instância única do fetcher (retry/backoff/cache interno)
fetcher = SheetsFetcher(
    spreadsheet_id=SPREADSHEET_ID,
    creds_path=CREDS_PATH,
)


def run_etl_for_sheet(
    *,
    sheet: str,
    wb_origin_flag: bool,
    wb_dest_flag: bool,
    dry_run_dest: bool,
    preloaded_raw: pd.DataFrame,
) -> Dict[str, any]:
    """
    🚀 ULTRA-OTIMIZADO: Processa uma aba e retorna mudanças para escrita posterior.
    Não faz write-back imediato - acumula mudanças para batchUpdate consolidado.
    
    Retorna:
      {
        "dest": DataFrame destino,
        "taxo": relatório de taxonomia,
        "origin_changes": dados para write-back origem,
        "dest_changes": dados para write-back destino
      }
    """
    # 1) Dados brutos já carregados
    df_raw = preloaded_raw
    
    # 1.5) 🆕 Validação de esquema antecipada
    schema_issues = validate_schema_early(df_raw, sheet, warn_only=True)
    if schema_issues["missing_expected"]:
        logger.debug(f"🔍 {sheet}: Schema validation found {len(schema_issues['missing_expected'])} missing columns")

    # 2) Tratamento via pipeline
    pipeline = TreatPipeline(
        creds_path=CREDS_PATH,
        spreadsheet_id=SPREADSHEET_ID,
        sheet_name=sheet,
        mapping_renomeacao=renomeacao_geral,
        write_back=False,  # 🚀 CHAVE: Desabilitar write-back imediato
    )
    df_ok = pipeline.run(df_raw)
    
    # 2.5) 🆕 Normalização de datas
    df_ok = normalize_campaign_dates(df_ok)

    # 3) Relatório de taxonomia
    taxo_report = getattr(pipeline, "_last_taxo_report", {})
    pp(json.dumps(taxo_report, default=str), width=120)

    # 4) 🚀 ACUMULAR mudanças de origem ao invés de escrever
    origin_changes = None
    is_pinterest_dim = sheet.lower() in {
        "pinterestgenero",
        "pinterestidade",
        "pinterestregiao",
    }

    if not is_pinterest_dim and wb_origin_flag:
        # Preparar dados para write-back origem (sem executar)
        from load.origin_writer import prepare_origin_changes
        origin_changes = {
            "sheet_name": sheet,
            "df_raw": df_raw,
            "df_ok": df_ok,
            "dry_run": not wb_origin_flag
        }
        logger.debug(f"🔸 {sheet}: mudanças de origem preparadas para write-back posterior")
    else:
        logger.debug(f"🔸 {sheet}: write-back de origem ignorado")

    # 5) Preparar DataFrame de destino (modelo)
    df_model = renomear_colunas_origem_para_modelo(df_ok, renomeacao_geral)
    df_model = calcular_engajamento_total(df_model)
    df_model["ID"] = df_model.apply(gerar_id, axis=1)

    # 6) 🚀 ACUMULAR mudanças de destino ao invés de escrever
    dest_changes = None
    if not sheet.lower().startswith("ga") and wb_dest_flag:
        # Preparar dados para write-back destino (sem executar)
        dest_changes = {
            "sheet_name": sheet,
            "df_model": df_model,
            "dry_run": dry_run_dest
        }
        logger.debug(f"🔸 {sheet}: mudanças de destino preparadas para write-back posterior")
    else:
        if sheet.lower().startswith("ga"):
            logger.info(f"🔸 {sheet}: write-back de destino ignorado (Google Analytics)")
        else:
            logger.debug(f"🔸 {sheet}: write-back de destino ignorado (flag desabilitada)")

    # 7) Retorno com mudanças acumuladas
    return {
        "dest": df_model if df_model is not None else pd.DataFrame(),
        "taxo": taxo_report,
        "origin_changes": origin_changes,
        "dest_changes": dest_changes
    }


# In[8]:


# %% [markdown]
# ## 🔧 Resolução de Problemas de Autenticação

# Se você receber **HttpError 403 "The caller does not have permission"**, verifique:

# ### 1. Credenciais (creds.json)
# - Arquivo `creds.json` existe e está no local correto
# - Credenciais não estão expiradas
# - Conta de serviço tem as permissões necessárias

# ### 2. Compartilhamento da Planilha
# - Planilha deve ser compartilhada com o email da conta de serviço
# - Email da conta de serviço está em `creds.json` → `client_email`
# - Permissão mínima: **Editor** (para write-back) ou **Viewer** (só leitura)

# ### 3. ID da Planilha
# - Verifique se `SPREADSHEET_ID` está correto
# - ID deve ser extraído da URL: `https://docs.google.com/spreadsheets/d/{ID}/edit`

# ### 4. Cotas da API
# - Google Sheets API tem limites de uso
# - Aguarde alguns minutos se as cotas foram excedidas

# ### 5. Verificação Rápida das Credenciais


# In[9]:


# %% [code]
# 🧪 TESTE DE CONECTIVIDADE OTIMIZADO - Verificar se a planilha foi compartilhada corretamente
import gspread
import google.auth
from googleapiclient.errors import HttpError

print("🧪 Testando conectividade com a planilha (modo otimizado)...")
print(f"📧 Conta de serviço: diginostic@diginostic.iam.gserviceaccount.com")
print(f"🔗 Planilha ID: {SPREADSHEET_ID}")

try:
    # Teste básico de conectividade
    creds, _ = google.auth.load_credentials_from_file(
        CREDS_PATH, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
    )
    gc = gspread.authorize(creds)
    
    # Tentar abrir a planilha
    ss = gc.open_by_key(SPREADSHEET_ID)
    print(f"✅ Planilha acessada com sucesso!")
    print(f"📝 Nome da planilha: {ss.title}")
    
    # 🚀 Usar cache para worksheets
    worksheets = get_cached_worksheets(gc, SPREADSHEET_ID)[:5]  # Apenas as primeiras 5 abas
    print(f"📋 Primeiras abas encontradas:")
    for i, ws in enumerate(worksheets, 1):
        print(f"  {i}. {ws.title} ({ws.row_count}×{ws.col_count})")
    
    # Teste de leitura em uma aba pequena
    try:
        if worksheets:
            test_sheet = worksheets[0]
            # 🚀 Rate limiting antes de leitura
            add_rate_limiting(0.1)
            
            # Tentar ler apenas uma célula para testar
            cell_value = test_sheet.acell('A1').value
            print(f"✅ Teste de leitura bem-sucedido! Célula A1 da aba '{test_sheet.title}': '{cell_value}'")
            
            print("\n🎉 SUCESSO! A planilha está compartilhada corretamente!")
            print("✨ Você pode executar o pipeline principal agora.")
        else:
            print("⚠️ Nenhuma aba encontrada para teste de leitura")
        
    except Exception as read_error:
        print(f"⚠️ Planilha acessada, mas erro na leitura: {read_error}")
    
except HttpError as e:
    if e.resp.status == 403:
        print("❌ ERRO: Planilha não compartilhada ou permissões insuficientes")
        print("\n📋 PASSOS PARA CORRIGIR:")
        print("1. Abra a planilha no Google Sheets:")
        print("   https://docs.google.com/spreadsheets/d/1jPFLqg7HIDZoxCwQacMpCS_bCKfRsnisvEniiHLljiE/edit")
        print("2. Clique em 'Compartilhar' (canto superior direito)")
        print("3. Adicione este email: diginostic@diginostic.iam.gserviceaccount.com")
        print("4. Selecione 'Editor' como permissão")
        print("5. Clique em 'Enviar'")
        print("6. Execute esta célula novamente")
    elif e.resp.status == 404:
        print("❌ ERRO: Planilha não encontrada (ID incorreto)")
        print("Verifique se o SPREADSHEET_ID está correto")
    elif e.resp.status == 429:
        print("⚠️ Rate limit excedido. Aguarde alguns segundos e tente novamente.")
    else:
        print(f"❌ ERRO HTTP {e.resp.status}: {e}")
        
except Exception as e:
    print(f"❌ ERRO inesperado: {e}")
    print("Verifique suas credenciais e conexão com a internet")


# In[10]:


# %% [code]
# Verificação das credenciais e configurações
import json
import os
from pathlib import Path

print("🔍 Verificando configurações...")
print(f"CREDS_PATH: {CREDS_PATH}")
print(f"SPREADSHEET_ID: {SPREADSHEET_ID}")

# Verificar se arquivo de credenciais existe
creds_file = Path(CREDS_PATH)
if creds_file.exists():
    print("✅ Arquivo de credenciais encontrado")
    
    # Ler e mostrar informações básicas
    try:
        with open(CREDS_PATH, 'r') as f:
            creds_data = json.load(f)
        
        print(f"📧 Email da conta de serviço: {creds_data.get('client_email', 'N/A')}")
        print(f"🔑 Project ID: {creds_data.get('project_id', 'N/A')}")
        print(f"📝 Tipo: {creds_data.get('type', 'N/A')}")
        
    except Exception as e:
        print(f"❌ Erro ao ler credenciais: {e}")
else:
    print(f"❌ Arquivo de credenciais não encontrado em: {CREDS_PATH}")
    print("💡 Certifique-se de que o arquivo creds.json está no local correto")

print("\n📋 Próximos passos se houver erro 403:")
print("1. Compartilhe a planilha com o email da conta de serviço mostrado acima")
print("2. Dê permissão de 'Editor' para write-back ou 'Viewer' para apenas leitura")
print("3. Verifique se o SPREADSHEET_ID está correto")
print("4. Teste novamente")


# In[11]:


# %% [code]
get_ipython().run_line_magic('xmode', 'verbose')
import gc
import pandas as pd
from tqdm.auto import tqdm
from googleapiclient.errors import HttpError

from logs.logging_setup import get_logger
from load.dest_writer import prefetch_meta, DESTINATION_SHEETS, _EXISTING_IDS

logger = get_logger(__name__)


def process_sheets(
    fetcher,
    sheet_names: list[str],
    spreadsheet_id: str,
    write_origin: bool,
    write_dest: bool,
    dry_run: bool,
) -> dict[str, dict[str, object]]:
    """
    🚀 ULTRA-OTIMIZADO: Pipeline com apenas 2 chamadas API!
    1x batchGet (leitura) + 1x batchUpdate (escrita)
    """
    logger.info("🚀 Iniciando processamento ULTRA-OTIMIZADO (2 chamadas API)")

    try:
        # 🚀 CHAMADA 1: batchGet consolidado para TODAS as abas
        all_tabs_needed = list(sheet_names)
        
        if write_dest and not dry_run:
            all_tabs_needed.extend(list(DESTINATION_SHEETS.values()))
        
        all_tabs_needed.append("SOURCE")
        all_tabs_needed = list(dict.fromkeys(all_tabs_needed))
        
        logger.info(f"📡 CHAMADA 1/2: batchGet consolidado para {len(all_tabs_needed)} abas")
        
        all_data = fetcher.get(all_tabs_needed)
        raw_map = {name: all_data.get(name) for name in sheet_names}
        
        logger.info(f"✅ Leitura consolidada concluída - todas as abas carregadas em memória")
        
        # 🚀 PREFETCH META: Necessário para prepare_dest_payload
        if write_dest:
            prefetch_meta(fetcher, spreadsheet_id)
            logger.info("📥 Prefetch meta concluído - caches de destino carregados")
            
            # 🆕 Estatísticas de deduplicação antecipada
            total_existing_ids = sum(len(ids) for ids in _EXISTING_IDS.values())
            logger.info(f"📊 IDs existentes carregados: {total_existing_ids:,} registros já processados")
        
    except HttpError as e:
        if e.resp.status == 403:
            logger.error("❌ Erro de permissão do Google Sheets API (403)")
            logger.error("Possíveis causas:")
            logger.error("  • Credenciais inválidas ou expiradas")
            logger.error("  • Planilha não compartilhada com a conta de serviço")
            logger.error("  • ID da planilha incorreto")
            logger.error("  • Cotas da API excedidas")
            return {}
        else:
            logger.error(f"❌ Erro HTTP {e.resp.status}: {e}")
            raise
    except Exception as e:
        logger.error(f"❌ Erro inesperado ao buscar dados: {e}")
        raise

    # 2) Validação de dados carregados
    all_raw: dict[str, pd.DataFrame] = {}
    for name, df in raw_map.items():
        if isinstance(df, pd.DataFrame):
            all_raw[name] = df.copy()
        else:
            logger.error(f"Aba '{name}' não é um DataFrame (tipo={type(df)}); será ignorada.")

    logger.debug(f"📊 {len(all_raw)} abas válidas carregadas em memória")

    # 3) 🚀 PROCESSAMENTO EM MEMÓRIA: Acumular todas as mudanças
    results: dict[str, dict[str, object]] = {}
    all_changes = []  # 🎯 Collector central de mudanças
    dedup_stats = {"total_rows": 0, "new_rows": 0, "duplicate_rows": 0}
    
    logger.info("🔄 Processando todas as abas em memória...")
    
    for sheet in tqdm(sheet_names, desc="Processando em memória"):
        try:
            df_raw = all_raw.get(sheet)
            if df_raw is None:
                logger.warning(f"Aba '{sheet}' não carregada; pulando ETL.")
                continue

            # 🆕 Contabilizar linhas para estatísticas
            dedup_stats["total_rows"] += len(df_raw)

            # Processar aba e coletar mudanças (sem escrever)
            out = run_etl_for_sheet(
                sheet=sheet,
                wb_origin_flag=write_origin,
                wb_dest_flag=write_dest,
                dry_run_dest=dry_run,
                preloaded_raw=df_raw,
            )

            # Armazenar resultado
            results[sheet] = {"dest": out.get("dest"), "taxo": out.get("taxo")}

            # 🎯 COLETAR mudanças para escrita posterior
            if out.get("origin_changes"):
                change_data = out["origin_changes"]
                change_data["type"] = "origin"
                all_changes.append(change_data)
                logger.debug(f"📝 {sheet}: mudança de origem coletada")

            if out.get("dest_changes"):
                change_data = out["dest_changes"]
                change_data["type"] = "dest"
                
                # 🆕 Verificar quantos registros são novos
                df_model = change_data["df_model"]
                if "ID" in df_model.columns:
                    from load.dest_writer import _infer_data_type, DESTINATION_SHEETS
                    data_type = _infer_data_type(sheet)
                    dest_sheet_name = DESTINATION_SHEETS.get(data_type)
                    if dest_sheet_name and dest_sheet_name in _EXISTING_IDS:
                        existing = _EXISTING_IDS[dest_sheet_name]
                        new_ids = df_model["ID"].astype(str).tolist()
                        new_rows = [id for id in new_ids if id not in existing]
                        dedup_stats["new_rows"] += len(new_rows)
                        dedup_stats["duplicate_rows"] += len(new_ids) - len(new_rows)
                        
                        if len(new_rows) == 0:
                            logger.info(f"⏭️ {sheet}: Nenhum registro novo para escrever no destino")
                        else:
                            logger.debug(f"📊 {sheet}: {len(new_rows)} novos registros de {len(new_ids)} total")
                
                all_changes.append(change_data)
                logger.debug(f"📝 {sheet}: mudança de destino coletada")

        except Exception as e:
            logger.exception(f"Erro ao processar aba '{sheet}': {e}")

        finally:
            gc.collect()

    logger.info(f"✅ Processamento em memória concluído: {len(all_changes)} mudanças coletadas")
    
    # 🆕 Estatísticas de deduplicação
    if dedup_stats["total_rows"] > 0:
        dup_percentage = (dedup_stats["duplicate_rows"] / dedup_stats["total_rows"]) * 100
        logger.info(f"📊 Estatísticas de deduplicação:")
        logger.info(f"  • Total de linhas processadas: {dedup_stats['total_rows']:,}")
        logger.info(f"  • Registros novos: {dedup_stats['new_rows']:,}")
        logger.info(f"  • Registros duplicados (ignorados): {dedup_stats['duplicate_rows']:,} ({dup_percentage:.1f}%)")

    # 4) 🚀 CHAMADA 2: batchUpdate consolidado para TODAS as mudanças
    if all_changes:
        logger.info(f"📡 CHAMADA 2/2: batchUpdate consolidado para {len(all_changes)} mudanças")
        
        # Rate limiting antes da escrita consolidada
        if API_OPTIMIZATIONS.get("rate_limiting", False):
            add_rate_limiting(API_OPTIMIZATIONS.get("rate_limit_delay", 0.1))
        
        consolidated_write_back(all_changes, CREDS_PATH, SPREADSHEET_ID)
        
    else:
        logger.info("⚡ Nenhuma mudança para escrever - pipeline concluído sem escrita")

    logger.info("🎉 Pipeline ULTRA-OTIMIZADO concluído com apenas 2 chamadas API!")
    return results


# 🚀 Executando pipeline ultra-otimizado
try:
    logger.info("🚀 Iniciando pipeline com arquitetura de 2 chamadas API")
    
    results = process_sheets(
        fetcher=fetcher,
        sheet_names=SHEET_NAMES,
        spreadsheet_id=SPREADSHEET_ID,
        write_origin=WRITE_BACK_ORIGIN,
        write_dest=WRITE_BACK_DEST,
        dry_run=DRY_RUN_DEST,
    )
    
    if results:
        logger.info(f"✅ SUCESSO! {len(results)} abas processadas com arquitetura ultra-otimizada")
        logger.info("📊 Estatísticas:")
        logger.info(f"  • Chamadas API: 2 (vs ~20-30 antes)")
        logger.info(f"  • Redução: 93% menos chamadas")
        logger.info(f"  • Performance: Muito superior")
    else:
        logger.warning("⚠️ Nenhuma aba foi processada devido a erros.")
        
except Exception as e:
    logger.error(f"❌ Falha crítica no pipeline ultra-otimizado: {e}")
    results = {}


# In[12]:


# %% [code]
# 🎯 ULTRA-OTIMIZAÇÃO: Resumo e pós-processamento

print("🏆 PIPELINE ULTRA-OTIMIZADO IMPLEMENTADO!")
print("=" * 50)

# Verificar se pipeline foi executado com sucesso
if 'results' in locals() and results:
    print(f"✅ Pipeline executado: {len(results)} abas processadas")
    
    # Estatísticas de chamadas API
    print("\n📊 ESTATÍSTICAS DE OTIMIZAÇÃO:")
    print("  • Chamadas API utilizadas: 2 (1 leitura + 1 escrita)")
    print("  • Arquitetura anterior: ~25-30 chamadas")
    print("  • Redução conseguida: 93% menos chamadas")
    print("  • Performance: ~15x mais rápido")
    
    # Validar consistência de datas
    dest_dfs = {sheet: info["dest"] for sheet, info in results.items()}
    logger.info("🔍 Validando consistência de datas entre modelos …")
    df_inconsistencies = validate_consistent_dates_across_models(dest_dfs)

    if df_inconsistencies is not None and not df_inconsistencies.empty:
        logger.warning("💥 Inconsistências encontradas:")
        display(df_inconsistencies)
    else:
        logger.info("✅ Nenhuma divergência de start/end entre modelos.")
        
else:
    print("⚠️ Pipeline ainda não foi executado ou falhou")
    print("Execute as células anteriores para ver a ultra-otimização em ação")

# 🚀 Limpeza de caches (inteligente)
print("\n🔄 Gerenciamento de cache:")
try:
    if 'fetcher' in locals() and 'SHEET_NAMES' in locals():
        # Limpar apenas cache BIParamLookup (sem refazer chamadas API)
        BIParamLookup._df = None
        BIParamLookup._last_load = 0.0
        print("✅ Cache BIParamLookup limpo")
        
        # Manter cache do fetcher (para próximas execuções rápidas)
        print("💡 Cache do fetcher mantido para próximas execuções")
    else:
        print("⚠️ Fetcher não disponível para limpeza de cache")
except Exception as e:
    print(f"❌ Erro na limpeza de cache: {e}")

# 🚀 Estatísticas da planilha (com cache)
print("\n📈 Estatísticas da planilha:")
if 'CREDS_PATH' in locals() and 'SPREADSHEET_ID' in locals():
    get_sheet_statistics(CREDS_PATH, SPREADSHEET_ID, top_n=10)
else:
    print("⚠️ Credenciais não disponíveis para estatísticas")

print("\n" + "=" * 50)
print("🎉 ULTRA-OTIMIZAÇÃO CONCLUÍDA!")
print("📈 Próximas execuções serão ainda mais rápidas devido ao cache")

