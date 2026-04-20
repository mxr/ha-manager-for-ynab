"""Thin wrappers around external YNAB libraries."""

from __future__ import annotations

from manager_for_ynab.pending_income import pending_income
from sqlite_export_for_ynab._main import sync as sqlite_export_sync

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path


def run_pending_income(
    token: str, db_path: Path, *, for_real: bool, quiet: bool
) -> int:
    """Run pending income and return the updated transaction count."""

    result = pending_income(
        db=db_path, for_real=for_real, quiet=quiet, token_override=token
    )
    return result.updated_count


async def run_sqlite_export(
    token: str, db_path: Path, *, full_refresh: bool, quiet: bool
) -> None:
    """Run sqlite-export-for-ynab."""

    await sqlite_export_sync(token, db_path, full_refresh, quiet=quiet)
