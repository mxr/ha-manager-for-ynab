from __future__ import annotations

import datetime
import sqlite3
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import call
from unittest.mock import patch

import aiosqlite
import pytest
import voluptuous as vol
from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import State
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.service import async_get_cached_service_description
from homeassistant.setup import async_setup_component
from manager_for_ynab.auto_approve import AutoApproveResult
from manager_for_ynab.pending_income import PendingIncomeResult
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.ha_manager_for_ynab import ADD_TRANSACTION_SCHEMA
from custom_components.ha_manager_for_ynab import AUTO_APPROVE_SCHEMA
from custom_components.ha_manager_for_ynab import PENDING_INCOME_SCHEMA
from custom_components.ha_manager_for_ynab import SQLITE_EXPORT_SCHEMA
from custom_components.ha_manager_for_ynab import SQLITE_QUERY_SCHEMA
from custom_components.ha_manager_for_ynab import RuntimeData
from custom_components.ha_manager_for_ynab import _api
from custom_components.ha_manager_for_ynab import _async_register_services
from custom_components.ha_manager_for_ynab import _set_add_transaction_service_schema
from custom_components.ha_manager_for_ynab.config_flow import ManagerForYnabConfigFlow
from custom_components.ha_manager_for_ynab.config_flow import _user_schema
from custom_components.ha_manager_for_ynab.const import CLEARED_OPTIONS
from custom_components.ha_manager_for_ynab.const import CONF_DB_PATH
from custom_components.ha_manager_for_ynab.const import CONF_TOKEN
from custom_components.ha_manager_for_ynab.const import DOMAIN
from custom_components.ha_manager_for_ynab.const import SERVICE_ADD_TRANSACTION
from custom_components.ha_manager_for_ynab.const import SERVICE_AUTO_APPROVE
from custom_components.ha_manager_for_ynab.const import SERVICE_PENDING_INCOME
from custom_components.ha_manager_for_ynab.const import SERVICE_SQLITE_EXPORT
from custom_components.ha_manager_for_ynab.const import SERVICE_SQLITE_QUERY
from custom_components.ha_manager_for_ynab.sensor import AutoApproveApprovedCountSensor
from custom_components.ha_manager_for_ynab.sensor import AutoApproveClearedCountSensor
from custom_components.ha_manager_for_ynab.sensor import PendingIncomeUpdatedCountSensor
from custom_components.ha_manager_for_ynab.sensor import (
    async_setup_entry as sensor_async_setup_entry,
)

ADD_TRANSACTION_SEED = Path(__file__).parent / "sql" / "add_transaction" / "seed.sql"
ADD_TRANSACTION_NO_PLANS_SEED = (
    Path(__file__).parent / "sql" / "add_transaction" / "no_plans.sql"
)
ADD_TRANSACTION_SINGLE_PLAN_SEED = (
    Path(__file__).parent / "sql" / "add_transaction" / "single_plan.sql"
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from homeassistant.core import HomeAssistant
    from homeassistant.helpers.entity import Entity
    from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback


def seed_db(db_path: Path, seed_path: Path = ADD_TRANSACTION_SEED) -> None:
    with sqlite3.connect(db_path) as con:
        con.executescript(seed_path.read_text())
        con.commit()


async def setup_integration(
    hass: HomeAssistant,
    *,
    entry_id: str = "entry-1",
    db_path: str = "/tmp/db.sqlite3",
) -> MockConfigEntry:
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_TOKEN: "token", CONF_DB_PATH: db_path},
        entry_id=entry_id,
        title="Manager for YNAB",
        unique_id=DOMAIN,
    )
    entry.add_to_hass(hass)

    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    return entry


def test_runtime_data_listener_unsubscribe_is_idempotent() -> None:
    runtime_data = RuntimeData(token="token", db_path="")
    listener = Mock()

    unsubscribe = runtime_data.async_add_listener(listener)
    unsubscribe()
    # Calling unsubscribe again should be a no-op once the listener is gone.
    unsubscribe()
    runtime_data.async_set_pending_income_updated_count(7)

    listener.assert_not_called()


def test_runtime_data_notifies_listeners_on_pending_income_update() -> None:
    runtime_data = RuntimeData(token="token", db_path="")
    seen: list[int | None] = []

    def listener() -> None:
        seen.append(runtime_data.pending_income_updated_count)

    runtime_data.async_add_listener(listener)
    runtime_data.async_set_pending_income_updated_count(3)

    assert runtime_data.pending_income_updated_count == 3
    assert seen == [3]


def test_runtime_data_notifies_listeners_on_auto_approve_update() -> None:
    runtime_data = RuntimeData(token="token", db_path="")
    seen: list[tuple[int | None, int | None]] = []

    def listener() -> None:
        seen.append(
            (
                runtime_data.auto_approve_approved_count,
                runtime_data.auto_approve_cleared_count,
            )
        )

    runtime_data.async_add_listener(listener)
    runtime_data.async_set_auto_approve_counts(
        AutoApproveResult(transactions=[], updated_count=3, cleared=2)
    )

    assert runtime_data.auto_approve_approved_count == 3
    assert runtime_data.auto_approve_cleared_count == 2
    assert seen == [(3, 2)]


@pytest.mark.parametrize(
    ("setter", "expected_seen"),
    [
        pytest.param(
            lambda runtime_data: runtime_data.async_set_auto_approve_approved_count(3),
            [(3, None)],
            id="approved",
        ),
        pytest.param(
            lambda runtime_data: runtime_data.async_set_auto_approve_cleared_count(2),
            [(None, 2)],
            id="cleared",
        ),
    ],
)
def test_runtime_data_notifies_listeners_on_restored_auto_approve_count(
    setter: Callable[[RuntimeData], None],
    expected_seen: list[tuple[int | None, int | None]],
) -> None:
    runtime_data = RuntimeData(token="token", db_path="")
    seen: list[tuple[int | None, int | None]] = []

    def listener() -> None:
        seen.append(
            (
                runtime_data.auto_approve_approved_count,
                runtime_data.auto_approve_cleared_count,
            )
        )

    runtime_data.async_add_listener(listener)
    setter(runtime_data)

    assert seen == expected_seen


def test_pending_income_sensor_reads_runtime_state() -> None:
    runtime_data = RuntimeData(token="token", db_path="/tmp/ynab.sqlite3")
    runtime_data.async_set_pending_income_updated_count(5)

    sensor = PendingIncomeUpdatedCountSensor(runtime_data, "entry-1")

    assert sensor.native_value == 5


@pytest.mark.parametrize(
    ("sensor_cls", "setter", "expected"),
    [
        pytest.param(
            AutoApproveApprovedCountSensor,
            lambda runtime_data: runtime_data.async_set_auto_approve_approved_count(5),
            5,
            id="approved",
        ),
        pytest.param(
            AutoApproveClearedCountSensor,
            lambda runtime_data: runtime_data.async_set_auto_approve_cleared_count(4),
            4,
            id="cleared",
        ),
    ],
)
def test_auto_approve_sensor_reads_runtime_state(
    sensor_cls: type[AutoApproveApprovedCountSensor | AutoApproveClearedCountSensor],
    setter: Callable[[RuntimeData], None],
    expected: int,
) -> None:
    runtime_data = RuntimeData(token="token", db_path="/tmp/ynab.sqlite3")
    setter(runtime_data)

    sensor = sensor_cls(runtime_data, "entry-1")

    assert sensor.native_value == expected


@patch.object(PendingIncomeUpdatedCountSensor, "async_write_ha_state", autospec=True)
@patch.object(PendingIncomeUpdatedCountSensor, "async_on_remove", autospec=True)
@pytest.mark.asyncio
async def test_sensor_async_added_to_hass_registers_listener(
    async_on_remove: Mock, async_write_ha_state: Mock
) -> None:
    runtime_data = RuntimeData(token="token", db_path="/tmp/ynab.sqlite3")
    sensor = PendingIncomeUpdatedCountSensor(runtime_data, "entry-1")

    await sensor.async_added_to_hass()
    async_on_remove.assert_called_once()
    unsubscribe = async_on_remove.call_args.args[1]

    runtime_data.async_set_pending_income_updated_count(2)
    unsubscribe()
    runtime_data.async_set_pending_income_updated_count(3)

    async_write_ha_state.assert_called_once_with(sensor)


@patch.object(
    PendingIncomeUpdatedCountSensor,
    "async_get_last_state",
    new_callable=AsyncMock,
    return_value=State("sensor.pending_income_updated_count", "6"),
)
@patch.object(PendingIncomeUpdatedCountSensor, "async_on_remove", autospec=True)
@pytest.mark.asyncio
async def test_sensor_async_added_to_hass_restores_last_state(
    async_on_remove: Mock, async_get_last_state: AsyncMock
) -> None:
    runtime_data = RuntimeData(token="token", db_path="/tmp/ynab.sqlite3")
    sensor = PendingIncomeUpdatedCountSensor(runtime_data, "entry-1")

    await sensor.async_added_to_hass()

    assert runtime_data.pending_income_updated_count == 6
    async_get_last_state.assert_awaited_once()
    async_on_remove.assert_called_once()


@patch.object(
    PendingIncomeUpdatedCountSensor,
    "async_get_last_state",
    new_callable=AsyncMock,
    return_value=State("sensor.pending_income_updated_count", "6"),
)
@patch.object(PendingIncomeUpdatedCountSensor, "async_on_remove", autospec=True)
@pytest.mark.asyncio
async def test_sensor_async_added_to_hass_preserves_runtime_state(
    async_on_remove: Mock, async_get_last_state: AsyncMock
) -> None:
    runtime_data = RuntimeData(
        token="token", db_path="/tmp/ynab.sqlite3", pending_income_updated_count=2
    )
    sensor = PendingIncomeUpdatedCountSensor(runtime_data, "entry-1")

    await sensor.async_added_to_hass()

    assert runtime_data.pending_income_updated_count == 2
    async_get_last_state.assert_not_awaited()
    async_on_remove.assert_called_once()


@pytest.mark.asyncio
async def test_sensor_async_setup_entry_adds_entity() -> None:
    added: list[Entity] = []
    entry = MockConfigEntry(domain=DOMAIN, entry_id="entry-1")
    entry.runtime_data = RuntimeData(token="token", db_path="")

    def add_entities(
        new_entities: list[Entity],
        update_before_add: bool = False,
        *,
        config_subentry_id: str | None = None,
    ) -> None:
        del update_before_add, config_subentry_id
        added.extend(new_entities)

    await sensor_async_setup_entry(
        cast("HomeAssistant", None),
        entry,
        cast("AddConfigEntryEntitiesCallback", add_entities),
    )

    assert len(added) == 3
    assert isinstance(added[0], PendingIncomeUpdatedCountSensor)
    assert isinstance(added[1], AutoApproveApprovedCountSensor)
    assert isinstance(added[2], AutoApproveClearedCountSensor)


@patch(
    "custom_components.ha_manager_for_ynab._current_local_date",
    return_value=datetime.date(2026, 5, 6),
)
def test_service_schemas_default_values(_current_local_date: Mock) -> None:
    assert AUTO_APPROVE_SCHEMA({}) == {"for_real": False, "sync": True, "quiet": False}
    assert PENDING_INCOME_SCHEMA({}) == {
        "for_real": False,
        "sync": True,
        "quiet": False,
    }
    assert SQLITE_EXPORT_SCHEMA({}) == {"full_refresh": False, "quiet": False}
    assert SQLITE_QUERY_SCHEMA({"sql": "select 1"}) == {"sync": True, "sql": "select 1"}
    assert ADD_TRANSACTION_SCHEMA(
        {
            "account_name": "Checking",
            "payee_name": "Store",
            "amount": "12.34",
        }
    ) == {
        "account_name": "Checking",
        "payee_name": "Store",
        "use_current_date": True,
        "date": datetime.date(2026, 5, 6),
        "cleared": "uncleared",
        "amount": Decimal("12.34"),
        "sync": True,
        "quiet": False,
    }
    _current_local_date.assert_called_once_with()


@patch("custom_components.ha_manager_for_ynab.config_flow.sqlite_default_db_path")
def test_user_schema_uses_default_db_path(sqlite_default_db_path: Mock) -> None:
    default_db_path = Path("/tmp/default.sqlite3")
    sqlite_default_db_path.return_value = default_db_path

    assert _user_schema()({"token": "token"}) == {
        "token": "token",
        "db_path": str(default_db_path),
    }


@patch(
    "custom_components.ha_manager_for_ynab.config_flow.sqlite_default_db_path",
    return_value=Path("/tmp/default.sqlite3"),
)
def test_user_schema_rejects_empty_db_path(sqlite_default_db_path: Mock) -> None:
    del sqlite_default_db_path

    with pytest.raises(vol.Invalid):
        _user_schema()({"token": "token", "db_path": ""})


@patch(
    "custom_components.ha_manager_for_ynab._api.pending_income",
    new_callable=AsyncMock,
    return_value=PendingIncomeResult(transactions=[], updated_count=11),
)
@pytest.mark.asyncio
async def test_api_run_pending_income(pending_income: AsyncMock) -> None:
    ret = await _api.run_pending_income(
        "token", Path("/tmp/db.sqlite3"), for_real=True, sync=False, quiet=False
    )
    assert ret == PendingIncomeResult(transactions=[], updated_count=11)
    pending_income.assert_awaited_once_with(
        db=Path("/tmp/db.sqlite3"),
        full_refresh=False,
        should_sync=False,
        for_real=True,
        skip_matched=False,
        quiet=False,
        token_override="token",
    )


@patch(
    "custom_components.ha_manager_for_ynab._api.auto_approve",
    new_callable=AsyncMock,
    return_value=AutoApproveResult(transactions=[], updated_count=9, cleared=0),
)
@pytest.mark.asyncio
async def test_api_run_auto_approve(auto_approve: AsyncMock) -> None:
    ret = await _api.run_auto_approve(
        "token", Path("/tmp/db.sqlite3"), for_real=True, sync=False, quiet=False
    )
    assert ret == AutoApproveResult(transactions=[], updated_count=9, cleared=0)
    auto_approve.assert_awaited_once_with(
        db=Path("/tmp/db.sqlite3"),
        full_refresh=False,
        should_sync=False,
        for_real=True,
        quiet=False,
        token_override="token",
    )


@patch(
    "custom_components.ha_manager_for_ynab._api.sqlite_export_sync",
    new_callable=AsyncMock,
)
@pytest.mark.asyncio
async def test_api_run_sqlite_export_delegates(sqlite_export_sync: AsyncMock) -> None:
    await _api.run_sqlite_export(
        "token",
        Path("/tmp/db.sqlite3"),
        full_refresh=True,
        quiet=False,
    )

    sqlite_export_sync.assert_awaited_once_with(
        "token", Path("/tmp/db.sqlite3"), True, quiet=False
    )


@pytest.mark.asyncio
async def test_api_run_sql_query(tmp_path: Path) -> None:
    db_path = tmp_path / "db.sqlite3"
    with sqlite3.connect(db_path) as con:
        con.execute("create table budgets (id integer, name text)")
        con.execute("insert into budgets values (1, 'Home')")
        con.execute("insert into budgets values (2, 'Travel')")
        con.commit()

    assert await _api.run_sql_query(
        db_path, "select id, name from budgets order by id"
    ) == {
        "rows": [{"id": 1, "name": "Home"}, {"id": 2, "name": "Travel"}],
    }
    assert await _api.run_sql_query(db_path, "PRAGMA query_only = ON;") == {}
    assert await _api.run_sql_query(
        db_path,
        "SELECT id, name FROM budgets ORDER BY id; SELECT id, name FROM budgets WHERE id = 2;",
    ) == {
        "rows": [
            {"id": 1, "name": "Home"},
            {"id": 2, "name": "Travel"},
            {"id": 2, "name": "Travel"},
        ],
    }


@pytest.mark.asyncio
async def test_api_run_sql_query_write(tmp_path: Path) -> None:
    db_path = tmp_path / "db.sqlite3"
    with sqlite3.connect(db_path) as con:
        con.execute("create table budgets (id integer, name text)")
        con.commit()

    with pytest.raises(aiosqlite.DatabaseError):
        await _api.run_sql_query(db_path, "insert into budgets values (1, 'Home')")

    with sqlite3.connect(db_path) as con:
        assert con.execute("select count(*) from budgets").fetchone() == (0,)


@pytest.mark.asyncio
async def test_api_get_add_transaction_options_multiple_plans(tmp_path: Path) -> None:
    db_path = tmp_path / "db.sqlite3"
    seed_db(db_path)

    options = await _api.get_add_transaction_options(db_path)

    assert options["default_plan_name"] is None
    assert {"Budget A", "Budget B"} <= set(options["plans"])
    assert options["categories_by_plan"]["Budget A"] == ["Bills - Electric"]
    assert options["categories_by_plan"]["Budget B"] == ["Food - Groceries"]
    assert options["accounts_by_plan"]["Budget A"] == ["Checking A"]
    assert options["accounts_by_plan"]["Budget B"] == ["Checking B"]
    assert options["payees_by_plan"]["Budget A"] == ["Power Co"]
    assert options["payees_by_plan"]["Budget B"] == ["Market Co"]
    assert options["cleared"] == CLEARED_OPTIONS


@pytest.mark.asyncio
async def test_api_get_add_transaction_options_flat_lists_are_unique_and_sorted(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "db.sqlite3"
    seed_db(db_path)

    options = await _api.get_add_transaction_options(db_path)

    assert options["accounts"].count("Checking B") == 1
    assert options["payees"].count("Market Co") == 1
    assert options["categories"].count("Food - Groceries") == 1
    assert options["categories"][-1] == "Credit Card Payments - Visa"


@patch(
    "custom_components.ha_manager_for_ynab._api.add_transaction_and_move_funds",
    new_callable=AsyncMock,
    return_value=0,
)
@patch(
    "custom_components.ha_manager_for_ynab._api.run_sqlite_export",
    new_callable=AsyncMock,
)
@pytest.mark.asyncio
async def test_api_run_add_transaction(
    run_sqlite_export: AsyncMock,
    add_transaction_and_move_funds: AsyncMock,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "db.sqlite3"
    plan_id = "11111111-1111-1111-1111-111111111111"
    account_id = "22222222-2222-2222-2222-222222222222"
    payee_id = "33333333-3333-3333-3333-333333333333"
    category_id = "44444444-4444-4444-4444-444444444444"
    seed_db(db_path)

    await _api.run_add_transaction(
        "token",
        db_path,
        plan_name="Budget",
        account_name="Checking",
        payee_name="Power Co",
        category_name="Bills - Electric",
        date=datetime.date(2026, 5, 1),
        cleared="uncleared",
        amount=Decimal("12.34"),
        sync=True,
        quiet=True,
    )

    run_sqlite_export.assert_awaited_once_with(
        "token", db_path, full_refresh=False, quiet=True
    )
    add_transaction_and_move_funds.assert_awaited_once()
    assert add_transaction_and_move_funds.call_args.kwargs["token"] == "token"
    assert add_transaction_and_move_funds.call_args.kwargs["db"] == db_path
    assert add_transaction_and_move_funds.call_args.kwargs["for_real"] is True
    assert add_transaction_and_move_funds.call_args.kwargs["quiet"] is True
    resolved = add_transaction_and_move_funds.call_args.kwargs["resolved"]
    assert resolved.plan.id == plan_id
    assert resolved.plan.name == "Budget"
    assert resolved.account.id == account_id
    assert resolved.account.name == "Checking"
    assert resolved.account.type == "checking"
    assert resolved.payee.id == payee_id
    assert resolved.payee.name == "Power Co"
    assert resolved.category.id == category_id
    assert resolved.category.name == "Electric"
    assert resolved.date == datetime.date(2026, 5, 1)
    assert resolved.amount == Decimal("12.34")


@patch(
    "custom_components.ha_manager_for_ynab._api.add_transaction_and_move_funds",
    new_callable=AsyncMock,
    return_value=1,
)
@pytest.mark.asyncio
async def test_api_run_add_transaction_raises_on_nonzero_result(
    add_transaction_and_move_funds: AsyncMock, tmp_path: Path
) -> None:
    db_path = tmp_path / "db.sqlite3"
    seed_db(db_path)

    with pytest.raises(
        RuntimeError, match="manager-for-ynab add_transaction_and_move_funds failed"
    ):
        await _api.run_add_transaction(
            "token",
            db_path,
            plan_name="Budget",
            account_name="Checking",
            payee_name="Power Co",
            category_name="Bills - Electric",
            date=datetime.date(2026, 5, 1),
            cleared="uncleared",
            amount=Decimal("12.34"),
            sync=False,
            quiet=False,
        )

    add_transaction_and_move_funds.assert_awaited_once()


@patch(
    "custom_components.ha_manager_for_ynab._api.add_transaction_and_move_funds",
    return_value=0,
)
@pytest.mark.asyncio
async def test_api_run_add_transaction_ignores_transfer_category(
    add_transaction_and_move_funds: AsyncMock,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "db.sqlite3"
    seed_db(db_path)

    await _api.run_add_transaction(
        "token",
        db_path,
        plan_name="Transfer Budget",
        account_name="Checking",
        payee_name="Transfer",
        category_name="Bills - Electric",
        date=datetime.date(2026, 5, 1),
        cleared="uncleared",
        amount=Decimal("12.34"),
        sync=False,
        quiet=False,
    )

    add_transaction_and_move_funds.assert_awaited_once()
    resolved = add_transaction_and_move_funds.call_args.kwargs["resolved"]
    assert resolved.category is None


@pytest.mark.asyncio
async def test_api_run_add_transaction_missing_account_raises(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "db.sqlite3"
    seed_db(db_path)

    with pytest.raises(
        RuntimeError, match="No open account named 'Checking' found in selected plan"
    ):
        await _api.run_add_transaction(
            "token",
            db_path,
            plan_name="Empty Budget",
            account_name="Checking",
            payee_name="Power Co",
            category_name=None,
            date=datetime.date(2026, 5, 1),
            cleared="uncleared",
            amount=Decimal("12.34"),
            sync=False,
            quiet=False,
        )


@pytest.mark.asyncio
async def test_api_run_add_transaction_missing_plan_raises(tmp_path: Path) -> None:
    db_path = tmp_path / "db.sqlite3"
    seed_db(db_path)

    with pytest.raises(RuntimeError, match="No plan named 'Missing Budget' found"):
        await _api.run_add_transaction(
            "token",
            db_path,
            plan_name="Missing Budget",
            account_name="Checking",
            payee_name="Power Co",
            category_name="Bills - Electric",
            date=datetime.date(2026, 5, 1),
            cleared="uncleared",
            amount=Decimal("12.34"),
            sync=False,
            quiet=False,
        )


@pytest.mark.asyncio
async def test_api_run_add_transaction_multiple_plans_requires_plan_name(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "db.sqlite3"
    seed_db(db_path)

    with pytest.raises(
        RuntimeError,
        match="Plan name is required when SQLite export has multiple plans",
    ):
        await _api.run_add_transaction(
            "token",
            db_path,
            plan_name=None,
            account_name="Checking A",
            payee_name="Power Co",
            category_name=None,
            date=datetime.date(2026, 5, 1),
            cleared="uncleared",
            amount=Decimal("12.34"),
            sync=False,
            quiet=False,
        )


@patch(
    "custom_components.ha_manager_for_ynab._api.add_transaction_and_move_funds",
    new_callable=AsyncMock,
    return_value=0,
)
@pytest.mark.asyncio
async def test_api_run_add_transaction_uses_only_plan_when_plan_name_omitted(
    add_transaction_and_move_funds: AsyncMock,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "db.sqlite3"
    seed_db(db_path, ADD_TRANSACTION_SINGLE_PLAN_SEED)

    await _api.run_add_transaction(
        "token",
        db_path,
        plan_name=None,
        account_name="Checking",
        payee_name="Power Co",
        category_name=None,
        date=datetime.date(2026, 5, 1),
        cleared="uncleared",
        amount=Decimal("12.34"),
        sync=False,
        quiet=False,
    )

    add_transaction_and_move_funds.assert_awaited_once()
    resolved = add_transaction_and_move_funds.call_args.kwargs["resolved"]
    assert resolved.plan.name == "Single Budget"


@pytest.mark.asyncio
async def test_api_run_add_transaction_without_plans_raises(tmp_path: Path) -> None:
    db_path = tmp_path / "db.sqlite3"
    seed_db(db_path, ADD_TRANSACTION_NO_PLANS_SEED)

    with pytest.raises(RuntimeError, match="No plans found in SQLite export"):
        await _api.run_add_transaction(
            "token",
            db_path,
            plan_name=None,
            account_name="Checking",
            payee_name="Power Co",
            category_name=None,
            date=datetime.date(2026, 5, 1),
            cleared="uncleared",
            amount=Decimal("12.34"),
            sync=False,
            quiet=False,
        )


@patch(
    "custom_components.ha_manager_for_ynab._api.add_transaction_and_move_funds",
    new_callable=AsyncMock,
    return_value=0,
)
@pytest.mark.asyncio
async def test_api_run_add_transaction_explicit_plan_no_sync(
    add_transaction_and_move_funds: AsyncMock, tmp_path: Path
) -> None:
    db_path = tmp_path / "db.sqlite3"
    seed_db(db_path)

    await _api.run_add_transaction(
        "token",
        db_path,
        plan_name="Budget",
        account_name="Checking",
        payee_name="Power Co",
        category_name=None,
        date=datetime.date(2026, 5, 1),
        cleared="cleared",
        amount=Decimal("12.34"),
        sync=False,
        quiet=False,
    )

    add_transaction_and_move_funds.assert_awaited_once()
    resolved = add_transaction_and_move_funds.call_args.kwargs["resolved"]
    assert resolved.plan.name == "Budget"
    assert resolved.category is None


@patch.object(
    ManagerForYnabConfigFlow,
    "async_show_form",
    autospec=True,
    return_value={"type": "form"},
)
@pytest.mark.asyncio
async def test_config_flow_user_shows_form(async_show_form: MagicMock) -> None:
    flow = ManagerForYnabConfigFlow()

    result = await flow.async_step_user()

    assert result == {"type": "form"}
    async_show_form.assert_called_once()
    assert async_show_form.call_args.args == (flow,)
    assert async_show_form.call_args.kwargs["step_id"] == "user"
    assert "data_schema" in async_show_form.call_args.kwargs


@patch.object(
    ManagerForYnabConfigFlow,
    "async_create_entry",
    autospec=True,
    return_value={"type": "create_entry"},
)
@patch.object(ManagerForYnabConfigFlow, "_abort_if_unique_id_configured", autospec=True)
@patch.object(ManagerForYnabConfigFlow, "async_set_unique_id", autospec=True)
@pytest.mark.asyncio
async def test_config_flow_user_creates_entry(
    async_set_unique_id: AsyncMock,
    abort_if_unique_id_configured: MagicMock,
    async_create_entry: MagicMock,
) -> None:
    flow = ManagerForYnabConfigFlow()

    result = await flow.async_step_user(
        {"token": "token", "db_path": "/tmp/ynab.sqlite3"}
    )

    assert result == {"type": "create_entry"}
    async_set_unique_id.assert_awaited_once_with(flow, DOMAIN)
    abort_if_unique_id_configured.assert_called_once_with(flow)
    async_create_entry.assert_called_once_with(
        flow,
        title="Manager for YNAB",
        data={"token": "token", "db_path": "/tmp/ynab.sqlite3"},
    )


@pytest.mark.usefixtures("enable_custom_integrations")
@pytest.mark.asyncio
async def test_async_setup_registers_services(hass: HomeAssistant) -> None:
    setup_ok = await async_setup_component(hass, DOMAIN, {})
    await hass.async_block_till_done()

    assert setup_ok is True
    assert hass.services.has_service(DOMAIN, SERVICE_AUTO_APPROVE)
    assert hass.services.has_service(DOMAIN, SERVICE_ADD_TRANSACTION)
    assert hass.services.has_service(DOMAIN, SERVICE_PENDING_INCOME)
    assert hass.services.has_service(DOMAIN, SERVICE_SQLITE_EXPORT)
    assert hass.services.has_service(DOMAIN, SERVICE_SQLITE_QUERY)


@pytest.mark.usefixtures("enable_custom_integrations")
@pytest.mark.asyncio
async def test_async_setup_keeps_existing_services(hass: HomeAssistant) -> None:
    assert await async_setup_component(hass, DOMAIN, {})
    await hass.async_block_till_done()

    await _async_register_services(hass)

    assert hass.services.has_service(DOMAIN, SERVICE_AUTO_APPROVE)
    assert hass.services.has_service(DOMAIN, SERVICE_ADD_TRANSACTION)
    assert hass.services.has_service(DOMAIN, SERVICE_PENDING_INCOME)
    assert hass.services.has_service(DOMAIN, SERVICE_SQLITE_EXPORT)
    assert hass.services.has_service(DOMAIN, SERVICE_SQLITE_QUERY)


@pytest.mark.usefixtures("enable_custom_integrations")
@pytest.mark.asyncio
async def test_config_entry_setup_and_unload(hass: HomeAssistant) -> None:
    entry = await setup_integration(hass)

    unload_ok = await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()

    assert unload_ok is True
    assert entry.state is ConfigEntryState.NOT_LOADED
    assert hass.data[DOMAIN] == {}


@patch(
    "homeassistant.config_entries.ConfigEntries.async_unload_platforms",
    new_callable=AsyncMock,
    return_value=False,
)
@pytest.mark.usefixtures("enable_custom_integrations")
@pytest.mark.asyncio
async def test_config_entry_unload_failure_keeps_entry_data(
    async_unload_platforms: AsyncMock,
    hass: HomeAssistant,
) -> None:
    entry = await setup_integration(hass)

    unload_ok = await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()

    assert unload_ok is False
    assert entry.entry_id in hass.data[DOMAIN]
    async_unload_platforms.assert_awaited_once()


@patch(
    "custom_components.ha_manager_for_ynab._current_local_date",
    return_value=datetime.date(2026, 5, 6),
)
@pytest.mark.usefixtures("enable_custom_integrations")
@pytest.mark.asyncio
async def test_async_setup_entry_refreshes_add_transaction_schema(
    _current_local_date: Mock,
    hass: HomeAssistant,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "ynab-schema.sqlite3"
    seed_db(db_path)

    entry = await setup_integration(hass, db_path=str(db_path))
    description = async_get_cached_service_description(
        hass, DOMAIN, SERVICE_ADD_TRANSACTION
    )

    assert description is not None
    assert "default" not in description["fields"]["plan_name"]
    assert (
        "My Budget"
        in description["fields"]["plan_name"]["selector"]["select"]["options"]
    )
    assert (
        "My Account"
        in description["fields"]["account_name"]["selector"]["select"]["options"]
    )
    assert (
        "custom_value"
        not in description["fields"]["account_name"]["selector"]["select"]
    )
    assert (
        "My Category Group - My Category"
        in description["fields"]["category_name"]["selector"]["select"]["options"]
    )
    assert description["fields"]["category_name"]["required"] is True
    assert (
        "custom_value"
        not in description["fields"]["category_name"]["selector"]["select"]
    )
    assert description["fields"]["use_current_date"]["default"] is True
    assert description["fields"]["use_current_date"]["selector"] == {"boolean": {}}
    assert description["fields"]["date"]["default"] == "2026-05-06"
    assert entry.state is ConfigEntryState.LOADED
    _current_local_date.assert_called_once_with()
    assert (
        "My Payee"
        in description["fields"]["payee_name"]["selector"]["select"]["options"]
    )


@pytest.mark.usefixtures("enable_custom_integrations")
@pytest.mark.asyncio
async def test_set_add_transaction_service_schema_handles_bad_options(
    hass: HomeAssistant,
) -> None:
    await async_setup_component(hass, DOMAIN, {})
    await hass.async_block_till_done()

    _set_add_transaction_service_schema(
        hass,
        {
            "plans": "oops",
            "accounts_by_plan": "oops",
            "categories_by_plan": "oops",
            "payees_by_plan": "oops",
            "default_plan_name": "My Budget",
        },
    )
    description = async_get_cached_service_description(
        hass, DOMAIN, SERVICE_ADD_TRANSACTION
    )

    assert description is not None
    assert description["fields"]["plan_name"]["default"] == "My Budget"
    assert description["fields"]["plan_name"]["selector"]["select"]["options"] == []
    assert description["fields"]["account_name"]["selector"]["select"]["options"] == []
    assert description["fields"]["category_name"]["selector"]["select"]["options"] == []
    assert description["fields"]["payee_name"]["selector"]["select"]["options"] == []


@pytest.mark.usefixtures("enable_custom_integrations")
@pytest.mark.asyncio
async def test_config_entry_setup_registers_entity_and_device(
    hass: HomeAssistant,
) -> None:
    entry = await setup_integration(hass)
    entity_id = "sensor.manager_for_ynab_pending_income_updated_count"
    auto_approve_approved_entity_id = (
        "sensor.manager_for_ynab_auto_approve_approved_count"
    )
    auto_approve_cleared_entity_id = (
        "sensor.manager_for_ynab_auto_approve_cleared_count"
    )

    state = hass.states.get(entity_id)
    entity_entry = er.async_get(hass).async_get(entity_id)
    auto_approve_approved_state = hass.states.get(auto_approve_approved_entity_id)
    auto_approve_approved_entity_entry = er.async_get(hass).async_get(
        auto_approve_approved_entity_id
    )
    auto_approve_cleared_state = hass.states.get(auto_approve_cleared_entity_id)
    auto_approve_cleared_entity_entry = er.async_get(hass).async_get(
        auto_approve_cleared_entity_id
    )
    device = dr.async_get(hass).async_get_device(identifiers={(DOMAIN, entry.entry_id)})

    assert state is not None
    assert state.state == "unknown"
    assert entity_entry is not None
    assert entity_entry.unique_id == f"{entry.entry_id}_pending_income_updated_count"
    assert auto_approve_approved_state is not None
    assert auto_approve_approved_state.state == "unknown"
    assert auto_approve_approved_entity_entry is not None
    assert (
        auto_approve_approved_entity_entry.unique_id
        == f"{entry.entry_id}_auto_approve_approved_count"
    )
    assert auto_approve_cleared_state is not None
    assert auto_approve_cleared_state.state == "unknown"
    assert auto_approve_cleared_entity_entry is not None
    assert (
        auto_approve_cleared_entity_entry.unique_id
        == f"{entry.entry_id}_auto_approve_cleared_count"
    )
    assert device is not None
    assert device.name == "Manager for YNAB"


@pytest.mark.usefixtures("enable_custom_integrations")
@pytest.mark.asyncio
async def test_service_raises_without_a_loaded_entry(hass: HomeAssistant) -> None:
    await async_setup_component(hass, DOMAIN, {})
    await hass.async_block_till_done()

    with pytest.raises(HomeAssistantError, match="Manager for YNAB is not configured"):
        await hass.services.async_call(
            DOMAIN,
            SERVICE_PENDING_INCOME,
            {"for_real": False, "sync": False, "quiet": False},
            blocking=True,
        )


@patch(
    "custom_components.ha_manager_for_ynab._api.run_sql_query",
    new_callable=AsyncMock,
    return_value={"rows": [{"id": 1}]},
)
@patch(
    "custom_components.ha_manager_for_ynab._api.get_add_transaction_options",
    new_callable=AsyncMock,
    return_value={"plans": ["Budget"]},
)
@patch(
    "custom_components.ha_manager_for_ynab._api.run_add_transaction", return_value=None
)
@patch(
    "custom_components.ha_manager_for_ynab._api.run_sqlite_export", return_value=None
)
@patch(
    "custom_components.ha_manager_for_ynab._api.run_pending_income",
    return_value=PendingIncomeResult(transactions=[], updated_count=4),
)
@patch(
    "custom_components.ha_manager_for_ynab._api.run_auto_approve",
    return_value=AutoApproveResult(transactions=[], updated_count=3, cleared=2),
)
@pytest.mark.usefixtures("enable_custom_integrations")
@pytest.mark.asyncio
async def test_register_services_success_and_idempotence(
    run_auto_approve: Mock,
    run_pending_income: Mock,
    run_sqlite_export: Mock,
    run_add_transaction: Mock,
    get_add_transaction_options: AsyncMock,
    run_sql_query: AsyncMock,
    hass: HomeAssistant,
) -> None:
    await setup_integration(hass)

    await hass.services.async_call(
        DOMAIN,
        SERVICE_AUTO_APPROVE,
        {"for_real": True, "sync": True, "quiet": True},
        blocking=True,
    )
    await hass.services.async_call(
        DOMAIN,
        SERVICE_PENDING_INCOME,
        {"for_real": True, "sync": True, "quiet": True},
        blocking=True,
    )
    await hass.services.async_call(
        DOMAIN,
        SERVICE_SQLITE_EXPORT,
        {"full_refresh": True, "quiet": False},
        blocking=True,
    )
    result = await hass.services.async_call(
        DOMAIN,
        SERVICE_SQLITE_QUERY,
        {"sync": True, "sql": "select 1"},
        blocking=True,
        return_response=True,
    )
    await hass.services.async_call(
        DOMAIN,
        SERVICE_ADD_TRANSACTION,
        {
            "plan_name": "Budget",
            "account_name": "Checking",
            "payee_name": "Store",
            "category_name": "Food - Groceries",
            "use_current_date": False,
            "date": "2026-05-01",
            "cleared": "uncleared",
            "amount": "12.34",
            "sync": True,
            "quiet": True,
        },
        blocking=True,
    )

    await hass.async_block_till_done()

    assert result == {"rows": [{"id": 1}]}
    state = hass.states.get("sensor.manager_for_ynab_pending_income_updated_count")
    auto_approve_approved_state = hass.states.get(
        "sensor.manager_for_ynab_auto_approve_approved_count"
    )
    auto_approve_cleared_state = hass.states.get(
        "sensor.manager_for_ynab_auto_approve_cleared_count"
    )
    assert state is not None
    assert state.state == "4"
    assert auto_approve_approved_state is not None
    assert auto_approve_approved_state.state == "3"
    assert auto_approve_cleared_state is not None
    assert auto_approve_cleared_state.state == "2"
    run_auto_approve.assert_called_once_with(
        "token",
        Path("/tmp/db.sqlite3"),
        for_real=True,
        sync=True,
        quiet=True,
    )
    run_pending_income.assert_called_once_with(
        "token",
        Path("/tmp/db.sqlite3"),
        for_real=True,
        sync=True,
        quiet=True,
    )
    run_sqlite_export.assert_has_calls(
        [
            call("token", Path("/tmp/db.sqlite3"), full_refresh=True, quiet=False),
            call("token", Path("/tmp/db.sqlite3"), full_refresh=False, quiet=True),
        ]
    )
    run_sql_query.assert_awaited_once_with(
        Path("/tmp/db.sqlite3"),
        "select 1",
    )
    run_add_transaction.assert_called_once_with(
        "token",
        Path("/tmp/db.sqlite3"),
        plan_name="Budget",
        account_name="Checking",
        payee_name="Store",
        category_name="Food - Groceries",
        date=datetime.date(2026, 5, 1),
        cleared="uncleared",
        amount=Decimal("12.34"),
        sync=True,
        quiet=True,
    )
    assert get_add_transaction_options.await_count == 6
    get_add_transaction_options.assert_has_awaits([call(Path("/tmp/db.sqlite3"))] * 6)


@patch(
    "custom_components.ha_manager_for_ynab._api.run_add_transaction", return_value=None
)
@patch(
    "custom_components.ha_manager_for_ynab._api.run_pending_income",
    return_value=PendingIncomeResult(transactions=[], updated_count=4),
)
@patch(
    "custom_components.ha_manager_for_ynab._api.run_auto_approve",
    return_value=AutoApproveResult(transactions=[], updated_count=3, cleared=2),
)
@pytest.mark.usefixtures("enable_custom_integrations")
@pytest.mark.asyncio
async def test_register_services_sync_false_skips_schema_refresh(
    run_auto_approve: Mock,
    run_pending_income: Mock,
    run_add_transaction: Mock,
    hass: HomeAssistant,
) -> None:
    await setup_integration(hass)

    await hass.services.async_call(
        DOMAIN,
        SERVICE_AUTO_APPROVE,
        {"for_real": True, "sync": False, "quiet": True},
        blocking=True,
    )
    await hass.services.async_call(
        DOMAIN,
        SERVICE_PENDING_INCOME,
        {"for_real": True, "sync": False, "quiet": True},
        blocking=True,
    )
    await hass.services.async_call(
        DOMAIN,
        SERVICE_ADD_TRANSACTION,
        {
            "account_name": "Checking",
            "payee_name": "Store",
            "date": "2026-05-01",
            "cleared": "uncleared",
            "amount": "12.34",
            "sync": False,
            "quiet": True,
        },
        blocking=True,
    )

    await hass.async_block_till_done()

    state = hass.states.get("sensor.manager_for_ynab_pending_income_updated_count")
    auto_approve_approved_state = hass.states.get(
        "sensor.manager_for_ynab_auto_approve_approved_count"
    )
    auto_approve_cleared_state = hass.states.get(
        "sensor.manager_for_ynab_auto_approve_cleared_count"
    )
    assert state is not None
    assert state.state == "4"
    assert auto_approve_approved_state is not None
    assert auto_approve_approved_state.state == "3"
    assert auto_approve_cleared_state is not None
    assert auto_approve_cleared_state.state == "2"
    run_auto_approve.assert_called_once()
    run_pending_income.assert_called_once()
    run_add_transaction.assert_called_once()


@patch(
    "custom_components.ha_manager_for_ynab._api.get_add_transaction_options",
    new_callable=AsyncMock,
    return_value={},
)
@patch(
    "custom_components.ha_manager_for_ynab._api.run_add_transaction", return_value=None
)
@patch(
    "custom_components.ha_manager_for_ynab._current_local_date",
    return_value=datetime.date(2026, 5, 6),
)
@pytest.mark.usefixtures("enable_custom_integrations")
@pytest.mark.asyncio
async def test_add_transaction_service_uses_current_date_by_default(
    _current_local_date: Mock,
    run_add_transaction: Mock,
    get_add_transaction_options: AsyncMock,
    hass: HomeAssistant,
) -> None:
    await setup_integration(hass)

    await hass.services.async_call(
        DOMAIN,
        SERVICE_ADD_TRANSACTION,
        {
            "plan_name": "Budget",
            "account_name": "Checking",
            "payee_name": "Store",
            "category_name": "Food - Groceries",
            "use_current_date": True,
            "cleared": "uncleared",
            "amount": "12.34",
            "sync": False,
            "quiet": True,
        },
        blocking=True,
    )

    run_add_transaction.assert_called_once_with(
        "token",
        Path("/tmp/db.sqlite3"),
        plan_name="Budget",
        account_name="Checking",
        payee_name="Store",
        category_name="Food - Groceries",
        date=datetime.date(2026, 5, 6),
        cleared="uncleared",
        amount=Decimal("12.34"),
        sync=False,
        quiet=True,
    )
    assert _current_local_date.call_count == 3
    _current_local_date.assert_has_calls([call(), call(), call()])
    get_add_transaction_options.assert_awaited_once_with(Path("/tmp/db.sqlite3"))


@patch(
    "custom_components.ha_manager_for_ynab._api.run_sql_query",
    new_callable=AsyncMock,
    side_effect=RuntimeError("boom"),
)
@patch(
    "custom_components.ha_manager_for_ynab._api.run_add_transaction",
    side_effect=RuntimeError("boom"),
)
@patch(
    "custom_components.ha_manager_for_ynab._api.run_sqlite_export",
    side_effect=RuntimeError("boom"),
)
@patch(
    "custom_components.ha_manager_for_ynab._api.run_pending_income",
    side_effect=RuntimeError("boom"),
)
@patch(
    "custom_components.ha_manager_for_ynab._api.run_auto_approve",
    side_effect=RuntimeError("boom"),
)
@pytest.mark.parametrize(
    ("service_name", "data", "match"),
    [
        pytest.param(
            SERVICE_PENDING_INCOME,
            {"for_real": False, "sync": True, "quiet": False},
            "pending_income failed: boom",
            id=SERVICE_PENDING_INCOME,
        ),
        pytest.param(
            SERVICE_AUTO_APPROVE,
            {"for_real": False, "sync": True, "quiet": False},
            "auto_approve failed: boom",
            id=SERVICE_AUTO_APPROVE,
        ),
        pytest.param(
            SERVICE_SQLITE_EXPORT,
            {"full_refresh": False, "quiet": False},
            "sqlite_export failed: boom",
            id=SERVICE_SQLITE_EXPORT,
        ),
        pytest.param(
            SERVICE_SQLITE_QUERY,
            {"sync": False, "sql": "select 1"},
            "sqlite_query failed: boom",
            id=SERVICE_SQLITE_QUERY,
        ),
        pytest.param(
            SERVICE_ADD_TRANSACTION,
            {
                "account_name": "Checking",
                "payee_name": "Store",
                "use_current_date": False,
                "date": "2026-05-01",
                "cleared": "uncleared",
                "amount": "12.34",
                "sync": False,
                "quiet": True,
            },
            "add_transaction failed: boom",
            id=SERVICE_ADD_TRANSACTION,
        ),
    ],
)
@pytest.mark.usefixtures("enable_custom_integrations")
@pytest.mark.asyncio
async def test_register_services_error_paths_raise_home_assistant_error(
    run_auto_approve: Mock,
    run_pending_income: Mock,
    run_sqlite_export: Mock,
    run_add_transaction: Mock,
    run_sql_query: AsyncMock,
    service_name: str,
    data: dict[str, object],
    match: str,
    hass: HomeAssistant,
) -> None:
    del (
        run_auto_approve,
        run_pending_income,
        run_sqlite_export,
        run_add_transaction,
        run_sql_query,
    )
    await setup_integration(hass)

    with pytest.raises(HomeAssistantError, match=match):
        await hass.services.async_call(
            DOMAIN,
            service_name,
            data,
            blocking=True,
            return_response=service_name == SERVICE_SQLITE_QUERY,
        )
