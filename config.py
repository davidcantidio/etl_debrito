"""
Configuração central do projeto ETL.

Este módulo centraliza todas as configurações do projeto, evitando
duplicações e garantindo que todas as configurações sejam carregadas
a partir de variáveis de ambiente.
"""

import os
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

# Carrega variáveis de ambiente do arquivo .env
load_dotenv()

# ====================================================================
# CONFIGURAÇÕES GERAIS
# ====================================================================

# Data mínima para filtros temporais
MIN_DATE_STR = os.getenv("MIN_DATE", "2025-06-01")
MIN_DATE = date.fromisoformat(MIN_DATE_STR)

# Caminho para credenciais Google
GOOGLE_CREDS_PATH = Path(os.getenv("GOOGLE_CREDS_PATH", "creds.json"))

# ====================================================================
# CONFIGURAÇÕES DE PLANILHAS
# ====================================================================

# Planilha principal (para main.py e treat)
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "1jPFLqg7HIDZoxCwQacMpCS_bCKfRsnisvEniiHLljiE")

# Configurações específicas do módulo ponto_de_controle
ORIGIN_SHEET_ID = os.getenv("ORIGIN_SHEET_ID", "")
ORIGIN_TAB = os.getenv("ORIGIN_TAB", "modeloGeral")

DEST_SHEET_ID = os.getenv("DEST_SHEET_ID", "")
DEST_TAB = os.getenv("DEST_TAB", "IMPULSIONAMENTOS 2025")
HEAD_ROW_DEST = int(os.getenv("HEAD_ROW_DEST", "4"))  # zero-based

# ====================================================================
# CONFIGURAÇÕES DE LOGGING
# ====================================================================

# Diretório de logs
LOG_DIR = Path(os.getenv("LOG_DIR", "./logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "pipeline_debug.log"

# ====================================================================
# COLUNAS DE DESTINO (PONTO DE CONTROLE)
# ====================================================================

# Lista oficial de colunas para o destino
DEST_COLUMNS = [
    "Data",
    "Campanha",
    "Veiculo",
    "Link conteúdos impulsionados",
    "Periodo",
    "Agência",
    "Editoria",
    "Objetivo",
    "Meta",
    "Status",
    "Resultado"
]

# ====================================================================
# VALIDAÇÕES
# ====================================================================

def validate_config():
    """Valida as configurações carregadas."""
    errors = []
    
    if not GOOGLE_CREDS_PATH.exists():
        errors.append(f"Arquivo de credenciais não encontrado: {GOOGLE_CREDS_PATH}")
    
    if not SPREADSHEET_ID:
        errors.append("SPREADSHEET_ID não configurado")
    
    if not ORIGIN_SHEET_ID:
        errors.append("ORIGIN_SHEET_ID não configurado")
    
    if not DEST_SHEET_ID:
        errors.append("DEST_SHEET_ID não configurado")
    
    if errors:
        raise ValueError("Erros de configuração:\n" + "\n".join(errors))

# Executar validação ao importar (opcional - pode ser removido se preferir)
# validate_config()