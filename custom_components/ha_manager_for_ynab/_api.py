"""Thin wrappers around external YNAB libraries."""

from __future__ import annotations

import aiosqlite
from manager_for_ynab.auto_approve import AutoApproveResult, auto_approve
from manager_for_ynab.pending_income import pending_income, PendingIncomeResult
from sqlite_export_for_ynab._main import sync as sqlite_export_sync

from typing import TYPE_CHECKING
from typing import Any

if TYPE_CHECKING:
    from pathlib import Path


async def run_auto_approve(
    token: str, db_path: Path, *, for_real: bool, quiet: bool
) -> AutoApproveResult:
    """Run auto approve and return the transaction data + how many were updated."""

    return await auto_approve(
        db=db_path,
        full_refresh=False,
        for_real=for_real,
        quiet=quiet,
        token_override=token,
    )


async def run_pending_income(
    token: str, db_path: Path, *, for_real: bool, quiet: bool
) -> PendingIncomeResult:
    """Run pending income and return the transaction data + how many were updated."""

    return await pending_income(
        db=db_path,
        full_refresh=False,
        for_real=for_real,
        skip_matched=False,
        quiet=quiet,
        token_override=token,
    )


async def run_sqlite_export(
    token: str, db_path: Path, *, full_refresh: bool, quiet: bool
) -> None:
    """Run sqlite-export-for-ynab."""

    await sqlite_export_sync(token, db_path, full_refresh, quiet=quiet)


async def run_sql_query(db_path: Path, sql: str) -> dict[str, Any]:
    """Execute a SQL statement against the configured SQLite database."""

    async with aiosqlite.connect(f"file:{db_path}?mode=ro", uri=True) as connection:
        connection.row_factory = aiosqlite.Row
        async with connection.execute(sql) as cursor:
            if cursor.description is not None:
                rows = [dict(row) for row in await cursor.fetchall()]
                return {"rows": rows}

        return {}
