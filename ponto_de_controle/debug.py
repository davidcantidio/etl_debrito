"""Helpers de depuração usados pelos módulos do ponto de controle."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# opções básicas para enxergar melhor no terminal / notebook
pd.set_option("display.max_rows", 20)
pd.set_option("display.max_columns", None)


def debug_shape(df: pd.DataFrame, *, name: str | None = None) -> None:
    """Imprime shape, colunas e as 5 primeiras linhas de *df*.

    • Em notebooks: usa ``IPython.display.display`` para render HTML.  
    • Em scripts: faz fallback para ``print(df.head())``.
    """
    prefix = f"▼ {name}: " if name else "▼ "
    logger.info("%s%d × %d", prefix, df.shape[0], df.shape[1])

    # tenta usar display se estiver em ambiente Jupyter
    try:
        from IPython.display import display  # type: ignore

        display(df.head())
    except ImportError:
        # ambiente de linha de comando
        print(df.head())
