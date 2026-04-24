"""Thin wrappers around external YNAB libraries."""

from __future__ import annotations

import csv
import io
import sqlite3
from manager_for_ynab.auto_approve import auto_approve
from manager_for_ynab.pending_income import pending_income
from sqlite_export_for_ynab._main import sync as sqlite_export_sync

from typing import TYPE_CHECKING
from typing import Any

if TYPE_CHECKING:
    from pathlib import Path


def _rows_to_csv(columns: list[str], rows: list[dict[str, Any]]) -> str:
    """Serialize query rows to CSV."""

    with io.StringIO() as output:
        writer = csv.DictWriter(output, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
        return output.getvalue()


def run_pending_income(
    token: str, db_path: Path, *, for_real: bool, quiet: bool
) -> int:
    """Run pending income and return the updated transaction count."""

    result = pending_income(
        db=db_path, for_real=for_real, quiet=quiet, token_override=token
    )
    return result.updated_count


def run_auto_approve(token: str, db_path: Path, *, for_real: bool, quiet: bool) -> int:
    """Run auto approve and return the updated transaction count."""

    result = auto_approve(
        db=db_path, for_real=for_real, quiet=quiet, token_override=token
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
