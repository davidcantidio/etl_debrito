"""
Warning database implementation using SQLite.

This module defines a ``WarningDatabase`` class for persisting user
decisions and automatic rules in a SQLite database.  It implements the
schema described in the preproject documentation (``user_decisions``,
``warning_rules``, ``geografia``) and provides basic CRUD operations.
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Optional


DEFAULT_SCHEMA = """
CREATE TABLE IF NOT EXISTS user_decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    warning_type TEXT NOT NULL,
    context_hash TEXT NOT NULL,
    user_decision TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS warning_rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pattern TEXT NOT NULL,
    action TEXT NOT NULL,
    replacement_value TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS geografia (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cidade TEXT,
    estado TEXT,
    regiao TEXT
);
"""


class WarningDatabase:
    """SQLite wrapper for storing decisions and rules."""

    def __init__(self, db_path: str | Path = "warnings.db") -> None:
        self.db_path = Path(db_path)
        self._ensure_schema()

    @contextmanager
    def _connect(self) -> Iterable[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path)
        try:
            yield conn
        finally:
            conn.commit()
            conn.close()

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(DEFAULT_SCHEMA)

    # User decisions
    def add_user_decision(self, warning_type: str, context_hash: str, user_decision: str) -> int:
        with self._connect() as conn:
            cur = conn.execute(
                "INSERT INTO user_decisions (warning_type, context_hash, user_decision) VALUES (?, ?, ?)",
                (warning_type, context_hash, user_decision),
            )
            return cur.lastrowid

    def get_user_decisions(self, warning_type: Optional[str] = None) -> list[tuple[str, str, str]]:
        query = "SELECT warning_type, context_hash, user_decision FROM user_decisions"
        params: tuple[Any, ...] = ()
        if warning_type:
            query += " WHERE warning_type = ?"
            params = (warning_type,)
        with self._connect() as conn:
            return conn.execute(query, params).fetchall()

    # Warning rules
    def add_warning_rule(self, pattern: str, action: str, replacement_value: Optional[str] = None) -> int:
        with self._connect() as conn:
            cur = conn.execute(
                "INSERT INTO warning_rules (pattern, action, replacement_value) VALUES (?, ?, ?)",
                (pattern, action, replacement_value),
            )
            return cur.lastrowid

    def get_warning_rules(self) -> list[tuple[int, str, str, Optional[str]]]:
        with self._connect() as conn:
            return conn.execute(
                "SELECT id, pattern, action, replacement_value FROM warning_rules WHERE 1"
            ).fetchall()

    # Geografia table
    def import_geografia(self, rows: Iterable[tuple[str, str, str]]) -> None:
        with self._connect() as conn:
            conn.executemany(
                "INSERT INTO geografia (cidade, estado, regiao) VALUES (?, ?, ?)",
                rows,
            )