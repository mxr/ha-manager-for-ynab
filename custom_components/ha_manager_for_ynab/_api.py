"""Thin wrappers around external YNAB libraries."""

from __future__ import annotations

import asyncio
import csv
import io
import sqlite3
from datetime import date
from typing import TYPE_CHECKING
from typing import Any
from typing import Never

import ynab
from tldm import tldm
from manager_for_ynab.auto_approve import AutoApproveResult
from manager_for_ynab.auto_approve import build_updates as build_auto_approve_updates
from manager_for_ynab.auto_approve import fetch_auto_approve_transactions
from manager_for_ynab.auto_approve import print_found_txns as print_auto_approve_txns
from manager_for_ynab.pending_income import PendingIncomeResult
from manager_for_ynab.pending_income import (
    build_updates as build_pending_income_updates,
)
from manager_for_ynab.pending_income import fetch_pending_income
from manager_for_ynab.pending_income import (
    print_found_txns as print_pending_income_txns,
)
from sqlite_export_for_ynab._main import sync as sqlite_export_sync

if TYPE_CHECKING:
    from pathlib import Path


def _rows_to_csv(columns: list[str], rows: list[dict[str, Any]]) -> str:
    """Serialize query rows to CSV."""

    with io.StringIO() as output:
        writer = csv.DictWriter(output, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
        return output.getvalue()


async def run_pending_income(
    token: str, db_path: Path, *, for_real: bool, quiet: bool
) -> int:
    """Run pending income and return the updated transaction count."""

    await sqlite_export_sync(token, db_path, False, quiet=quiet)
    result = await asyncio.to_thread(
        _run_pending_income_after_refresh,
        token,
        db_path,
        for_real,
        quiet,
    )
    return result.updated_count


async def run_auto_approve(
    token: str, db_path: Path, *, for_real: bool, quiet: bool
) -> int:
    """Run auto approve and return the updated transaction count."""

    await sqlite_export_sync(token, db_path, False, quiet=quiet)
    result = await asyncio.to_thread(
        _run_auto_approve_after_refresh,
        token,
        db_path,
        for_real,
        quiet,
    )
    return result.updated_count


async def run_sqlite_export(
    token: str, db_path: Path, *, full_refresh: bool, quiet: bool
) -> None:
    """Run sqlite-export-for-ynab."""

    await sqlite_export_sync(token, db_path, full_refresh, quiet=quiet)


def run_sql_query(db_path: Path, sql: str, *, output_format: str) -> dict[str, Any]:
    """Execute a SQL statement against the configured SQLite database."""

    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as connection:
        connection.row_factory = sqlite3.Row
        cursor = connection.execute(sql)
        result: dict[str, Any] = {
            "output_format": output_format,
            "rowcount": cursor.rowcount,
        }

        if cursor.description is not None:
            columns = [description[0] for description in cursor.description]
            rows = [dict(row) for row in cursor.fetchall()]
            result["columns"] = columns
            result["rowcount"] = len(rows)
            if output_format == "csv":
                result["csv"] = _rows_to_csv(columns, rows)
            else:
                result["rows"] = rows

        return result


def _print(message: str, *, quiet: bool) -> None:
    if not quiet:
        print(message)


def _run_pending_income_after_refresh(
    token: str, db_path: Path, for_real: bool, quiet: bool
) -> PendingIncomeResult:
    with sqlite3.connect(db_path) as con:
        con.row_factory = sqlite3.Row
        txns_by_plan = fetch_pending_income(con.cursor(), skip_matched=False)

    found_txns = [txn for txns in txns_by_plan.values() for txn in txns]
    total_txns = len(found_txns)

    _print(f"Found {total_txns} income transaction(s) to update.", quiet=quiet)
    if found_txns:
        print_pending_income_txns(found_txns, quiet=quiet)

        if for_real:
            grouped = build_pending_income_updates(txns_by_plan, date.today())
            api_client = ynab.TransactionsApi(
                ynab.ApiClient(ynab.Configuration(access_token=token))
            )

            with tldm[Never](
                total=total_txns,
                desc=f"Updating {total_txns} transaction(s)",
                disable=quiet,
            ) as progress:
                for plan_id, txns in grouped.items():
                    api_client.update_transactions(
                        plan_id, ynab.PatchTransactionsWrapper(transactions=txns)
                    )
                    progress.update(len(txns))
            _print("Done", quiet=quiet)

    return PendingIncomeResult(
        transactions=found_txns, updated_count=total_txns if for_real else 0
    )


def _run_auto_approve_after_refresh(
    token: str, db_path: Path, for_real: bool, quiet: bool
) -> AutoApproveResult:
    with sqlite3.connect(db_path) as con:
        con.row_factory = sqlite3.Row
        txns_by_plan = fetch_auto_approve_transactions(con.cursor())

    found_txns = [txn for txns in txns_by_plan.values() for txn in txns]
    total_txns = len(found_txns)

    _print(f"Found {total_txns} matched transaction(s) to approve.", quiet=quiet)
    if found_txns:
        print_auto_approve_txns(found_txns, quiet=quiet)

        if for_real:
            grouped = build_auto_approve_updates(txns_by_plan)
            api_client = ynab.TransactionsApi(
                ynab.ApiClient(ynab.Configuration(access_token=token))
            )

            with tldm[Never](
                total=total_txns,
                desc=f"Approving {total_txns} transaction(s)",
                disable=quiet,
            ) as progress:
                for plan_id, txns in grouped.items():
                    api_client.update_transactions(
                        plan_id, ynab.PatchTransactionsWrapper(transactions=txns)
                    )
                    progress.update(len(txns) // 2)
            _print("Done", quiet=quiet)

    return AutoApproveResult(
        transactions=found_txns, updated_count=total_txns if for_real else 0
    )
