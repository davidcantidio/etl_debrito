import logging
import pathlib

# ── Configuração padrão de logging para todo o projeto ──────────────────────

# 1) Handler de console (STDOUT/STDERR)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
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
        console_handler,  # mostra todos os logs no console
        file_handler      # grava todos os logs em pipeline_debug.log
    ]
)

def get_logger(name: str) -> logging.Logger:
    """
    Retorna um logger configurado com o nível e formato definidos acima.
    Logs serão exibidos no console e gravados em pipeline_debug.log.
    """
    return logging.getLogger(name)
