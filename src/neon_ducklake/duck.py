"""DuckDB connection helpers shared by the lake writers.

DuckDB 2.0 refuses ``register``/``unregister`` while an earlier ``execute()`` result on the same
connection is unfinished, which every ``execute(...).fetchone()`` leaves behind (a plain ``execute`` still
closes it). ``complete()`` runs that result to its end; DML has already run by then, and it is a no-op when
nothing is pending.
"""

from __future__ import annotations

from typing import Any


def register(con: Any, name: str, table: Any) -> None:
    """``con.register(name, table)`` after finishing any unfinished result on ``con``."""
    con.complete()
    con.register(name, table)


def unregister(con: Any, name: str) -> None:
    """``con.unregister(name)`` after finishing any unfinished result on ``con``."""
    con.complete()
    con.unregister(name)
