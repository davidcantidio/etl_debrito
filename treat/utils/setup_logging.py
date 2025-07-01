# utils/setup_logging.py

import logging
from logging import StreamHandler, FileHandler, Formatter
from typing import Optional


def setup_logging(
    level: int = logging.INFO,
    log_file: Optional[str] = "etl_geral.log",
    console: bool = True,
):
    """
    Configura logging padrão para console e (opcionalmente) para arquivo.

    Parameters
    ----------
    level : int
        Nível de logging (e.g. logging.DEBUG, logging.INFO).
    log_file : str | None
        Caminho do arquivo de log. Se None, não grava em arquivo.
    console : bool
        Se True, adiciona handler para stdout.
    """
    # Remove handlers pré-existentes (evita duplicação)
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)

    handlers = []
    fmt = Formatter(
        "%(asctime)s | %(levelname)-5s | %(name)s:%(lineno)d | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if console:
        ch = StreamHandler()
        ch.setLevel(level)
        ch.setFormatter(fmt)
        handlers.append(ch)

    if log_file:
        fh = FileHandler(log_file, encoding="utf-8")
        fh.setLevel(level)
        fh.setFormatter(fmt)
        handlers.append(fh)

    logging.basicConfig(level=level, handlers=handlers)
