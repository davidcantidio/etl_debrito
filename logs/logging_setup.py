import logging
import pathlib
import sys

# ── Configuração padrão de logging para todo o projeto ──────────────────────

# 1) Handler de console (STDOUT/STDERR)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(
    logging.Formatter("%(asctime)s %(levelname)s %(name)s › %(message)s", datefmt="%H:%M:%S")
)

# 2) Handler de arquivo (pipeline_debug.log)
file_log = pathlib.Path("/home/debrito/Documentos/etl_debrito/logs/pipeline_debug.log")
file_handler = logging.FileHandler(file_log, encoding="utf-8")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(
    logging.Formatter("%(asctime)s %(levelname)s %(name)s › %(message)s", datefmt="%H:%M:%S")
)

# 3) Configuração global
logging.basicConfig(
    level=logging.DEBUG,
    handlers=[
        console_handler,  # mostra INFO+ no console
        file_handler      # grava DEBUG+ em pipeline_debug.log
    ]
)

def get_logger(name: str) -> logging.Logger:
    """
    Retorna um logger configurado com o nível e formato definidos acima.
    Logs serão exibidos no console e gravados em pipeline_debug.log.
    """
    return logging.getLogger(name)

def _handle_unhandled_exception(exc_type, exc_value, exc_traceback):
    """
    Captura exceções não tratadas e registra o traceback completo nos handlers.
    KeyboardInterrupt é repassado ao excepthook padrão para permitir interrupção por Ctrl+C.
    """
    if issubclass(exc_type, KeyboardInterrupt):
        # Permite que o Ctrl+C encerre o processo normalmente
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logger = get_logger("unhandled")
    logger.critical("Exceção não capturada", exc_info=(exc_type, exc_value, exc_traceback))

# Substitui o excepthook padrão para capturar todos os erros não tratados
sys.excepthook = _handle_unhandled_exception
