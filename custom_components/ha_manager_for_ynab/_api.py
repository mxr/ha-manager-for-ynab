"""Thin wrappers around external YNAB libraries."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path


def default_db_path() -> Path:
    """Return the default SQLite export DB path."""
    from sqlite_export_for_ynab import default_db_path as sqlite_default_db_path

    return sqlite_default_db_path()


def run_pending_income(
    token: str, db_path: Path, *, for_real: bool, quiet: bool
) -> int:
    """Run pending income and return the updated transaction count."""
    from manager_for_ynab.pending_income import pending_income

    result = pending_income(
        db=db_path, for_real=for_real, quiet=quiet, token_override=token
    )
    return result.updated_count


async def run_sqlite_export(
    token: str, db_path: Path, *, full_refresh: bool, quiet: bool
) -> None:
    """Run sqlite-export-for-ynab."""
    from sqlite_export_for_ynab._main import sync

    await sync(token, db_path, full_refresh, quiet=quiet)
