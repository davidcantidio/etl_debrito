"""
Performance guard to monitor API call counts.

This module defines a ``PerformanceGuard`` class that tracks the number
of external API calls during the ETL run.  It raises a ``PerformanceViolation``
exception if the maximum allowed calls is exceeded.  The guard can be used
as a context manager or invoked manually before each API call.
"""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)


class PerformanceViolation(RuntimeError):
    """Exception raised when the API call limit is exceeded."""

    pass


class PerformanceGuard:
    """Track the number of API calls and enforce a maximum limit."""

    def __init__(self, max_calls: int = 2) -> None:
        self.api_calls = 0
        self.max_calls = max_calls
        self.call_log: list[str] = []

    def before_api_call(self, operation: str) -> None:
        """Record an API call; raise if limit exceeded."""
        self.api_calls += 1
        self.call_log.append(operation)
        if self.api_calls > self.max_calls:
            msg = f"Exceeded {self.max_calls} API calls limit!"
            log.error(msg)
            raise PerformanceViolation(msg)

    def reset(self) -> None:
        """Reset counters between pipeline runs."""
        self.api_calls = 0
        self.call_log.clear()