"""
Environment configuration utilities for the warning system.

This module determines whether the interactive warning system should be
activated based on environment variables.  It mirrors the functionality
proposed in the preproject documentation, allowing modes to be toggled
without modifying code.
"""

from __future__ import annotations

import os


class EnvironmentConfig:
    """Read environment variables to configure warning behavior."""

    def __init__(self) -> None:
        # INTERACTIVE_MODE=true habilita interceptação; default false
        self.interactive_mode = os.getenv("INTERACTIVE_MODE", "false").lower() == "true"
        # PRODUCTION_MODE=true desativa interatividade por segurança
        self.production_mode = os.getenv("PRODUCTION_MODE", "false").lower() == "true"

    def should_intercept(self) -> bool:
        """Return True if warnings should be intercepted interactively."""
        if self.production_mode:
            return False
        return self.interactive_mode

    def is_production(self) -> bool:
        """Indicate whether the system is running in production."""
        return self.production_mode