"""
Stub for early exit checker.

This module provides a placeholder implementation for an early exit checker.  In a full
implementation, the function defined here would examine incoming rows of data and
determine whether the ETL pipeline should stop processing early—e.g., if there
are no new rows or a certain sentinel condition is met.  For now, it simply logs
its invocation and always returns False so that the pipeline continues normally.
"""

from __future__ import annotations

import logging
from typing import Iterable, Any


def should_exit_early(rows: Iterable[Any]) -> bool:
    """Determine whether the ETL pipeline should exit early based on the provided rows.

    In this stub implementation, the function always returns ``False`` to indicate
    that the pipeline should continue.  It logs how many rows were inspected to
    aid debugging.  Future implementations might inspect the content of ``rows``
    or other state to decide whether to halt processing.

    Args:
        rows: An iterable collection of row-like objects to inspect.

    Returns:
        bool: ``False`` to indicate the pipeline should proceed.
    """
    try:
        num_rows = len(rows)  # type: ignore[arg-type]
    except Exception:
        # If rows is not sized, fall back to converting to list (could be generator)
