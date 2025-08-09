from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional, List

from .interactive_handler import UserDecision, WarningContext


@dataclass
class DecisionResult:
    action_taken: str
    new_value: Optional[Any] = None
    rule_created: Optional[str] = None
    sheets_updated: bool = False
    cache_invalidated: List[str] | None = None


class DecisionResolver:
    """
    Stub for the decision resolver. Applies a user's decision to the current data.
    This implementation simply logs the decision and returns a default DecisionResult.
    """

    def apply_decision(
        self,
        decision: str,
        context: WarningContext,
        dataframe: Any = None,
    ) -> DecisionResult:
        logging.getLogger(__name__).info(
            "Applying decision %s for warning on %s", decision, context
        )
        # No real modification yet; just return a DecisionResult indicating no updates
        return DecisionResult(action_taken=decision, sheets_updated=False, cache_invalidated=[])