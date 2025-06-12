import logging
import pathlib
import sys
import warnings

# ── Caminho para o diretório de logs ────────────────────────────────────────
LOG_DIR = pathlib.Path("/home/debrito/Documentos/etl_debrito/logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "pipeline_debug.log"


def setup_logging() -> None:
    """
    Configura logging global:
      - Console (INFO+)
      - Arquivo pipeline_debug.log (DEBUG+)
      - Captura warnings do módulo warnings
      - Intercepta exceções não tratadas e grava o traceback completo
    Deve ser chamada **antes** do seu código (no seu entrypoint).
    """
    # Formato de log
    fmt    = "%(asctime)s %(levelname)s %(name)s › %(message)s"
    datefmt = "%H:%M:%S"

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    # Remove handlers pré-existentes (se houver)
    for h in root.handlers[:]:
        root.removeHandler(h)

    # 1) Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(fmt, datefmt=datefmt))
    root.addHandler(console_handler)

    # 2) File handler
    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8", mode="a")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(fmt, datefmt=datefmt))
    root.addHandler(file_handler)

    # 3) Captura warnings.do módulo warnings
    logging.captureWarnings(True)

    # 4) Captura exceções não tratadas
    def _handle_exception(exc_type, exc_value, exc_tb):
        if issubclass(exc_type, KeyboardInterrupt):
            # Permite Ctrl+C interromper normalmente
            sys.__excepthook__(exc_type, exc_value, exc_tb)
            return
        logger = logging.getLogger("unhandled")
        logger.critical("Exceção não capturada", exc_info=(exc_type, exc_value, exc_tb))

    sys.excepthook = _handle_exception


# Invoca na importação do módulo para garantir que o setup seja aplicado
setup_logging()


def get_logger(name: str) -> logging.Logger:
    """
    Retorna um logger já configurado.
    Em cada módulo do seu pipeline, faça:
    
        from logging_setup import get_logger
        logger = get_logger(__name__)
    """
    return logging.getLogger(name)
