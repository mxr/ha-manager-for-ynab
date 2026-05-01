"""Thin wrappers around external YNAB libraries."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import uuid

import aiosqlite
from manager_for_ynab.auto_approve import AutoApproveResult, auto_approve
from manager_for_ynab.pending_income import pending_income, PendingIncomeResult
from sqlite_export_for_ynab._main import sync as sqlite_export_sync
import ynab
from ynab.models.transaction_cleared_status import TransactionClearedStatus

from typing import TYPE_CHECKING
from typing import Any

if TYPE_CHECKING:
    import datetime
    from decimal import Decimal
    from pathlib import Path


@dataclass(frozen=True)
class AddTransactionSelection:
    """Resolved transaction fields loaded from the SQLite export."""

    plan_id: str
    account_id: str
    payee_id: str | None
    category_id: str | None


async def run_auto_approve(
    token: str, db_path: Path, *, for_real: bool, sync: bool, quiet: bool
) -> AutoApproveResult:
    """Run auto approve and return the transaction data + how many were updated."""

    return await auto_approve(
        db=db_path,
        full_refresh=False,
        should_sync=sync,
        for_real=for_real,
        quiet=quiet,
        token_override=token,
    )


async def run_pending_income(
    token: str, db_path: Path, *, for_real: bool, sync: bool, quiet: bool
) -> PendingIncomeResult:
    """Run pending income and return the transaction data + how many were updated."""

    return await pending_income(
        db=db_path,
        full_refresh=False,
        should_sync=sync,
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


async def run_add_transaction(
    token: str,
    db_path: Path,
    *,
    plan_name: str | None,
    account_name: str,
    payee_name: str,
    category_name: str | None,
    date: datetime.date,
    cleared: str,
    amount: Decimal,
    sync: bool,
    quiet: bool,
) -> None:
    """Create a YNAB transaction using choices resolved from the SQLite export."""

    if sync:
        await run_sqlite_export(token, db_path, full_refresh=False, quiet=quiet)

    selection = await _resolve_add_transaction_selection(
        db_path,
        plan_name=plan_name,
        account_name=account_name,
        payee_name=payee_name,
        category_name=category_name,
    )
    transaction = ynab.NewTransaction(
        account_id=uuid.UUID(selection.account_id),
        date=date,
        payee_id=uuid.UUID(selection.payee_id)
        if selection.payee_id is not None
        else None,
        payee_name=payee_name,
        amount=int(-1 * 1000 * amount),
        category_id=uuid.UUID(selection.category_id)
        if selection.category_id is not None
        else None,
        cleared=TransactionClearedStatus[cleared.upper()],
        approved=True,
    )

    with ynab.ApiClient(ynab.Configuration(access_token=token)) as api_client:
        transactions_api = ynab.TransactionsApi(api_client)
        await asyncio.to_thread(
            transactions_api.create_transaction,
            selection.plan_id,
            ynab.PostTransactionsWrapper(transaction=transaction),
        )


async def get_add_transaction_options(db_path: Path) -> dict[str, Any]:
    """Return current add-transaction form choices from the SQLite export."""

    async with aiosqlite.connect(f"file:{db_path}?mode=ro", uri=True) as connection:
        connection.row_factory = aiosqlite.Row

        plans = await _fetch_column(
            connection,
            "SELECT name FROM plans ORDER BY LOWER(name)",
        )
        categories = await _fetch_grouped_column(
            connection,
            """
            SELECT p.name AS plan_name, c.category_group_name || ' - ' || c.name AS name
            FROM categories AS c
            INNER JOIN plans AS p ON p.id = c.plan_id
            WHERE NOT c.deleted AND NOT c.hidden
            ORDER BY LOWER(c.category_group_name), LOWER(c.name)
            """,
        )
        accounts = await _fetch_grouped_column(
            connection,
            """
            SELECT p.name AS plan_name, a.name
            FROM accounts AS a
            INNER JOIN plans AS p ON p.id = a.plan_id
            WHERE NOT a.deleted AND NOT a.closed
            ORDER BY LOWER(a.name)
            """,
        )
        payees = await _fetch_grouped_column(
            connection,
            """
            SELECT p.name AS plan_name, payees.name
            FROM payees
            INNER JOIN plans AS p ON p.id = payees.plan_id
            WHERE NOT payees.deleted
            ORDER BY LOWER(payees.name)
            """,
        )

    return {
        "default_plan_name": plans[0] if len(plans) == 1 else None,
        "plans": plans,
        "categories_by_plan": categories,
        "accounts_by_plan": accounts,
        "payees_by_plan": payees,
        "cleared": ["uncleared", "cleared", "reconciled"],
    }


async def run_sql_query(db_path: Path, sql: str) -> dict[str, Any]:
    """Execute a SQL query (multiple statements) against the configured SQLite database."""

    async with aiosqlite.connect(f"file:{db_path}?mode=ro", uri=True) as connection:
        connection.row_factory = aiosqlite.Row

        rows: list[dict[str, Any]] = []
        for raw_statement in sql.split(";"):
            if statement := raw_statement.strip():
                async with connection.execute(statement) as cursor:
                    if cursor.description is not None:
                        rows.extend(dict(row) for row in await cursor.fetchall())

        return {"rows": rows} if rows else {}


async def _resolve_add_transaction_selection(
    db_path: Path,
    *,
    plan_name: str | None,
    account_name: str,
    payee_name: str,
    category_name: str | None,
) -> AddTransactionSelection:
    async with aiosqlite.connect(f"file:{db_path}?mode=ro", uri=True) as connection:
        connection.row_factory = aiosqlite.Row
        plan_id = await _resolve_plan_id(connection, plan_name)
        account_id = await _fetch_one_value(
            connection,
            """
            SELECT id
            FROM accounts
            WHERE plan_id = ? AND name = ? AND NOT deleted AND NOT closed
            """,
            (plan_id, account_name),
            f"No open account named {account_name!r} found in selected plan.",
        )
        payee_id = await _fetch_optional_value(
            connection,
            """
            SELECT id
            FROM payees
            WHERE plan_id = ? AND name = ? AND NOT deleted
            """,
            (plan_id, payee_name),
        )
        category_id = None
        if category_name:
            category_id = await _fetch_one_value(
                connection,
                """
                SELECT id
                FROM categories
                WHERE plan_id = ?
                  AND category_group_name || ' - ' || name = ?
                  AND NOT deleted
                  AND NOT hidden
                """,
                (plan_id, category_name),
                f"No category named {category_name!r} found in selected plan.",
            )

    return AddTransactionSelection(
        plan_id=plan_id,
        account_id=account_id,
        payee_id=payee_id,
        category_id=category_id,
    )


async def _resolve_plan_id(
    connection: aiosqlite.Connection, plan_name: str | None
) -> str:
    if plan_name:
        return await _fetch_one_value(
            connection,
            "SELECT id FROM plans WHERE name = ?",
            (plan_name,),
            f"No plan named {plan_name!r} found.",
        )

    async with connection.execute(
        "SELECT id FROM plans ORDER BY LOWER(name)"
    ) as cursor:
        rows = list(await cursor.fetchall())
    if len(rows) == 1:
        return str(rows[0]["id"])
    if not rows:
        raise RuntimeError("No plans found in SQLite export.")
    raise RuntimeError("Plan name is required when SQLite export has multiple plans.")


async def _fetch_column(connection: aiosqlite.Connection, sql: str) -> list[str]:
    async with connection.execute(sql) as cursor:
        rows = await cursor.fetchall()
    return [str(row[0]) for row in rows]


async def _fetch_grouped_column(
    connection: aiosqlite.Connection, sql: str
) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = {}
    async with connection.execute(sql) as cursor:
        rows = await cursor.fetchall()
    for row in rows:
        grouped.setdefault(str(row["plan_name"]), []).append(str(row["name"]))
    return grouped


async def _fetch_one_value(
    connection: aiosqlite.Connection,
    sql: str,
    params: tuple[str, ...],
    error_message: str,
) -> str:
    value = await _fetch_optional_value(connection, sql, params)
    if value is None:
        raise RuntimeError(error_message)
    return value


async def _fetch_optional_value(
    connection: aiosqlite.Connection,
    sql: str,
    params: tuple[str, ...],
) -> str | None:
    async with connection.execute(sql, params) as cursor:
        row = await cursor.fetchone()
    return str(row[0]) if row is not None else None
