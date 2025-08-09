from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Any, List, Dict
import logging


class UserDecision:
    """
    Enumeration of possible user decisions for how to handle a warning.
    Currently only IGNORE is implemented as stub.
    """
    IGNORE = "ignore"


@dataclass
class WarningContext:
    """
    Context information associated with a warning. This mirrors the structure
    proposed in the preproject documentation and can be expanded later.
    """
    warning_type: str
    sheet_name: str
    row_number: Optional[int] = None
    column_name: Optional[str] = None
    current_value: Optional[Any] = None
    dataframe_sample: Optional[Any] = None
    suggested_values: Optional[List[str]] = None
    campaign_context: Optional[Dict[str, Any]] = None


class WarningInterceptor:
    """
    Minimal stub for an interactive warning interceptor. For now it simply
    logs that a warning was intercepted and returns an IGNORE decision.
    """

    def __init__(self, interactive: bool = True) -> None:
        self.interactive = interactive

    def intercept(self, warning_msg: str, context: WarningContext) -> str:
        logging.getLogger(__name__).info(
            "Intercepted warning: %s with context: %s", warning_msg, context
        )
        # Always ignore in this stub implementation
        return UserDecision.IGNORE

    def is_active(self) -> bool:
        return self.interactive