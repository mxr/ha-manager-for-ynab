"""Thin wrappers around external YNAB libraries."""

from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING
from typing import Any

import aiosqlite
from asyncio_for_ynab import TransactionClearedStatus
from manager_for_ynab.add_transaction import ResolvedAccount
from manager_for_ynab.add_transaction import ResolvedCategory
from manager_for_ynab.add_transaction import ResolvedPayee
from manager_for_ynab.add_transaction import ResolvedPlan
from manager_for_ynab.add_transaction import ResolvedTransaction
from manager_for_ynab.add_transaction import add_transaction_and_move_funds
from manager_for_ynab.auto_approve import AutoApproveResult
from manager_for_ynab.auto_approve import auto_approve
from manager_for_ynab.pending_income import PendingIncomeResult
from manager_for_ynab.pending_income import pending_income
from sqlite_export_for_ynab._main import sync as sqlite_export_sync

from .const import CLEARED_OPTIONS

if TYPE_CHECKING:
    import datetime
    from decimal import Decimal
    from pathlib import Path


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

    resolved = await _resolve_add_transaction(
        db_path,
        plan_name=plan_name,
        account_name=account_name,
        payee_name=payee_name,
        category_name=category_name,
        date=date,
        cleared=cleared,
        amount=amount,
    )
    result = await add_transaction_and_move_funds(
        resolved=resolved,
        token=token,
        db=db_path,
        for_real=True,
        quiet=quiet,
    )
    if result != 0:
        raise RuntimeError("manager-for-ynab add_transaction_and_move_funds failed")


async def get_add_transaction_options(db_path: Path) -> dict[str, Any]:
    """Return current add-transaction form choices from the SQLite export."""

    async with aiosqlite.connect(f"file:{db_path}?mode=ro", uri=True) as con:
        con.row_factory = aiosqlite.Row

        plans = await _fetch_column(
            con,
            "SELECT name FROM plans ORDER BY LOWER(name)",
        )
        categories = await _fetch_grouped_column(
            con,
            """
            SELECT p.name AS plan_name, c.category_group_name || ' - ' || c.name AS name
            FROM categories AS c
            INNER JOIN plans AS p ON p.id = c.plan_id
            WHERE NOT c.deleted AND NOT c.hidden AND c.name != 'Uncategorized'
            GROUP BY p.name, c.category_group_name, c.name
            ORDER BY LOWER(p.name), CASE WHEN c.category_group_name = 'Credit Card Payments' THEN 1 ELSE 0 END, LOWER(c.category_group_name), LOWER(c.name)
            """,
        )
        accounts = await _fetch_grouped_column(
            con,
            """
            SELECT p.name AS plan_name, a.name
            FROM accounts AS a
            INNER JOIN plans AS p ON p.id = a.plan_id
            WHERE NOT a.deleted AND NOT a.closed
            GROUP BY p.name, a.name
            ORDER BY LOWER(p.name), LOWER(a.name)
            """,
        )
        payees = await _fetch_grouped_column(
            con,
            """
            SELECT p.name AS plan_name, payees.name
            FROM payees
            INNER JOIN plans AS p ON p.id = payees.plan_id
            WHERE NOT payees.deleted
            GROUP BY p.name, payees.name
            ORDER BY LOWER(p.name), LOWER(payees.name)
            """,
        )
        category_options = await _fetch_column(
            con,
            """
            SELECT c.category_group_name || ' - ' || c.name AS name
            FROM categories AS c
            WHERE NOT c.deleted AND NOT c.hidden AND c.name != 'Uncategorized'
            GROUP BY c.category_group_name, c.name
            ORDER BY CASE WHEN c.category_group_name = 'Credit Card Payments' THEN 1 ELSE 0 END, LOWER(c.category_group_name), LOWER(c.name)
            """,
        )
        account_options = await _fetch_column(
            con,
            """
            SELECT a.name
            FROM accounts AS a
            WHERE NOT a.deleted AND NOT a.closed
            GROUP BY a.name
            ORDER BY LOWER(a.name)
            """,
        )
        payee_options = await _fetch_column(
            con,
            """
            SELECT payees.name
            FROM payees
            WHERE NOT payees.deleted
            GROUP BY payees.name
            ORDER BY LOWER(payees.name)
            """,
        )

    return {
        "default_plan_name": plans[0] if len(plans) == 1 else None,
        "plans": plans,
        "categories": category_options,
        "accounts": account_options,
        "payees": payee_options,
        "categories_by_plan": categories,
        "accounts_by_plan": accounts,
        "payees_by_plan": payees,
        "cleared": CLEARED_OPTIONS,
    }


async def run_sql_query(db_path: Path, sql: str) -> dict[str, Any]:
    """Execute a SQL query (multiple statements) against the configured SQLite database."""

    async with aiosqlite.connect(f"file:{db_path}?mode=ro", uri=True) as con:
        con.row_factory = aiosqlite.Row

        rows: list[dict[str, Any]] = []
        for raw_statement in sql.split(";"):
            if statement := raw_statement.strip():
                async with con.execute(statement) as cur:
                    if cur.description is not None:
                        rows.extend(dict(row) for row in await cur.fetchall())

        return {"rows": rows} if rows else {}


async def _resolve_add_transaction(
    db_path: Path,
    *,
    plan_name: str | None,
    account_name: str,
    payee_name: str,
    category_name: str | None,
    date: datetime.date,
    cleared: str,
    amount: Decimal,
) -> ResolvedTransaction:
    async with aiosqlite.connect(f"file:{db_path}?mode=ro", uri=True) as con:
        con.row_factory = aiosqlite.Row
        plan = await _resolve_plan(con, plan_name)
        account = await _fetch_one_row(
            con,
            """
            SELECT id, name, type
            FROM accounts
            WHERE plan_id = ? AND name = ? AND NOT deleted AND NOT closed
            """,
            (plan.id, account_name),
            f"No open account named {account_name!r} found in selected plan.",
        )
        payee = await _fetch_one_row(
            con,
            """
            SELECT id, name, transfer_account_id
            FROM payees
            WHERE plan_id = ? AND name = ? AND NOT deleted
            """,
            (plan.id, payee_name),
            f"No payee named {payee_name!r} found in selected plan.",
        )
        category = None
        if category_name and payee["transfer_account_id"] is None:
            category_row = await _fetch_one_row(
                con,
                """
                SELECT id, name
                FROM categories
                WHERE plan_id = ?
                  AND category_group_name || ' - ' || name = ?
                  AND NOT deleted
                  AND NOT hidden
                """,
                (plan.id, category_name),
                f"No category named {category_name!r} found in selected plan.",
            )
            category = ResolvedCategory(
                id=str(category_row["id"]), name=str(category_row["name"])
            )

    return ResolvedTransaction(
        plan=plan,
        account=ResolvedAccount(
            id=str(account["id"]), name=str(account["name"]), type=str(account["type"])
        ),
        payee=ResolvedPayee(id=str(payee["id"]), name=str(payee["name"])),
        category=category,
        date=date,
        cleared=TransactionClearedStatus[cleared.upper()],
        amount=amount,
    )


async def _resolve_plan(
    con: aiosqlite.Connection, plan_name: str | None
) -> ResolvedPlan:
    if plan_name:
        row = await _fetch_one_row(
            con,
            "SELECT id, name FROM plans WHERE name = ?",
            (plan_name,),
            f"No plan named {plan_name!r} found.",
        )
        return ResolvedPlan(id=str(row["id"]), name=str(row["name"]))

    async with con.execute("SELECT id, name FROM plans ORDER BY LOWER(name)") as cur:
        rows = list(await cur.fetchall())
    if len(rows) == 1:
        return ResolvedPlan(id=str(rows[0]["id"]), name=str(rows[0]["name"]))
    if not rows:
        raise RuntimeError("No plans found in SQLite export.")
    raise RuntimeError("Plan name is required when SQLite export has multiple plans.")


async def _fetch_column(con: aiosqlite.Connection, sql: str) -> list[str]:
    async with con.execute(sql) as cur:
        rows = await cur.fetchall()
    return [str(row[0]) for row in rows]


async def _fetch_grouped_column(
    con: aiosqlite.Connection, sql: str
) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = defaultdict(list)
    async with con.execute(sql) as cur:
        rows = await cur.fetchall()
    for row in rows:
        grouped[str(row["plan_name"])].append(str(row["name"]))
    return grouped


async def _fetch_one_row(
    con: aiosqlite.Connection,
    sql: str,
    params: tuple[str, ...],
    error_message: str,
) -> Any:
    async with con.execute(sql, params) as cur:
        row = await cur.fetchone()
    if row is None:
        raise RuntimeError(error_message)
    return row
