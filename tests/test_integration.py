from __future__ import annotations

import asyncio
import datetime
import sqlite3
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import call
from unittest.mock import patch

import aiosqlite
import pytest
import voluptuous as vol
from homeassistant.core import State
from homeassistant.core import SupportsResponse
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.service import async_get_cached_service_description
from manager_for_ynab.auto_approve import AutoApproveResult
from manager_for_ynab.pending_income import PendingIncomeResult

from custom_components.ha_manager_for_ynab import ADD_TRANSACTION_SCHEMA
from custom_components.ha_manager_for_ynab import AUTO_APPROVE_SCHEMA
from custom_components.ha_manager_for_ynab import PENDING_INCOME_SCHEMA
from custom_components.ha_manager_for_ynab import SQLITE_EXPORT_SCHEMA
from custom_components.ha_manager_for_ynab import SQLITE_QUERY_SCHEMA
from custom_components.ha_manager_for_ynab import RuntimeData
from custom_components.ha_manager_for_ynab import _api
from custom_components.ha_manager_for_ynab import _async_register_services
from custom_components.ha_manager_for_ynab import _get_runtime_data
from custom_components.ha_manager_for_ynab import _set_add_transaction_service_schema
from custom_components.ha_manager_for_ynab import async_setup
from custom_components.ha_manager_for_ynab import async_setup_entry
from custom_components.ha_manager_for_ynab import async_unload_entry
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
from custom_components.ha_manager_for_ynab.sensor import PendingIncomeUpdatedCountSensor
from custom_components.ha_manager_for_ynab.sensor import (
    async_setup_entry as sensor_async_setup_entry,
)
from tests.fixtures import config_entry_factory as config_entry_factory

ADD_TRANSACTION_SEED = Path(__file__).parent / "sql" / "add_transaction" / "seed.sql"

if TYPE_CHECKING:
    from collections.abc import Callable
    from collections.abc import Coroutine

    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant
    from homeassistant.helpers.entity import Entity
    from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

    ServiceHandler = Callable[[object], Coroutine[Any, Any, object | None]]


def seed_db(db_path: Path) -> None:
    with sqlite3.connect(db_path) as con:
        con.executescript(ADD_TRANSACTION_SEED.read_text())
        con.commit()


class FakeServices:
    def __init__(self) -> None:
        self.registered: dict[
            tuple[str, str], dict[str, ServiceHandler | object | None]
        ] = {}

    def has_service(self, domain: str, service: str) -> bool:
        return (domain, service) in self.registered

    def supports_response(self, domain: str, service: str) -> SupportsResponse:
        registered = self.registered.get((domain, service))
        if registered is None:
            return SupportsResponse.NONE
        supports_response = registered["supports_response"]
        return (
            SupportsResponse.NONE
            if supports_response is None
            else cast("SupportsResponse", supports_response)
        )

    def async_register(
        self,
        domain: str,
        service: str,
        handler: ServiceHandler,
        schema: object = None,
        supports_response: object = None,
    ) -> None:
        self.registered[(domain, service)] = {
            "handler": handler,
            "schema": schema,
            "supports_response": supports_response,
        }


class FakeConfigEntries:
    def __init__(self) -> None:
        self.forward_calls: list[tuple[ConfigEntry[RuntimeData], object]] = []
        self.unload_result = True

    async def async_forward_entry_setups(
        self, entry: ConfigEntry[RuntimeData], platforms: object
    ) -> None:
        self.forward_calls.append((entry, platforms))

    async def async_unload_platforms(
        self, entry: ConfigEntry[RuntimeData], platforms: object
    ) -> bool:
        self.forward_calls.append((entry, platforms))
        return self.unload_result


class FakeHass:
    def __init__(self) -> None:
        self.data: dict[str, dict[str, RuntimeData]] = {}
        self.services = FakeServices()
        self.config_entries = FakeConfigEntries()
        self.tasks: list[asyncio.Task[object]] = []

    def async_create_task(
        self, target: Coroutine[Any, Any, object], name: str | None = None
    ) -> asyncio.Task[object]:
        del name
        task = asyncio.create_task(target)
        self.tasks.append(task)
        return task

    async def async_block_till_done(self) -> None:
        if self.tasks:
            await asyncio.gather(*self.tasks)


@dataclass
class FakeServiceCall:
    data: dict[str, object]


def test_runtime_data_listener_unsubscribe_path() -> None:
    runtime_data = RuntimeData(token="token", db_path="")
    listener = Mock()

    unsubscribe = runtime_data.async_add_listener(listener)
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


def test_fake_services_supports_response_defaults_to_none() -> None:
    assert (
        FakeServices().supports_response(DOMAIN, SERVICE_ADD_TRANSACTION)
        == SupportsResponse.NONE
    )


def test_pending_income_sensor_reads_runtime_state() -> None:
    runtime_data = RuntimeData(token="token", db_path="/tmp/ynab.sqlite3")
    runtime_data.async_set_pending_income_updated_count(5)

    sensor = PendingIncomeUpdatedCountSensor(runtime_data, "entry-1")

    assert sensor.native_value == 5


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
async def test_sensor_async_setup_entry_adds_entity(
    config_entry_factory: Callable[..., ConfigEntry[RuntimeData]],
) -> None:
    added: list[Entity] = []
    entry = config_entry_factory(runtime_data=RuntimeData(token="token", db_path=""))

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

    assert len(added) == 1
    assert isinstance(added[0], PendingIncomeUpdatedCountSensor)


def test_service_schemas_default_values() -> None:
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
        "date": datetime.date.today(),
        "cleared": "uncleared",
        "amount": Decimal("12.34"),
        "sync": True,
        "quiet": False,
    }


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
    return_value=AutoApproveResult(transactions=[], updated_count=9),
)
@pytest.mark.asyncio
async def test_api_run_auto_approve(auto_approve: AsyncMock) -> None:
    ret = await _api.run_auto_approve(
        "token", Path("/tmp/db.sqlite3"), for_real=True, sync=False, quiet=False
    )
    assert ret == AutoApproveResult(transactions=[], updated_count=9)
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


@pytest.mark.asyncio
async def test_api_run_add_transaction_explicit_plan_no_sync(tmp_path: Path) -> None:
    db_path = tmp_path / "db.sqlite3"
    seed_db(db_path)

    with patch(
        "custom_components.ha_manager_for_ynab._api.add_transaction_and_move_funds",
        new_callable=AsyncMock,
        return_value=0,
    ) as add_transaction_and_move_funds:
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


@pytest.mark.asyncio
async def test_async_setup_registers_services() -> None:
    fake_hass = FakeHass()
    hass = cast("HomeAssistant", fake_hass)

    setup_ok = await async_setup(hass, {})

    assert setup_ok is True
    assert hass.services.has_service(DOMAIN, SERVICE_AUTO_APPROVE)
    assert hass.services.has_service(DOMAIN, SERVICE_ADD_TRANSACTION)
    assert hass.services.has_service(DOMAIN, SERVICE_PENDING_INCOME)
    assert hass.services.has_service(DOMAIN, SERVICE_SQLITE_EXPORT)
    assert hass.services.has_service(DOMAIN, SERVICE_SQLITE_QUERY)


@pytest.mark.asyncio
async def test_async_setup_and_unload_entry(
    config_entry_factory: Callable[..., ConfigEntry[RuntimeData]],
) -> None:
    fake_hass = FakeHass()
    hass = cast("HomeAssistant", fake_hass)
    entry = config_entry_factory(data={CONF_TOKEN: "token", CONF_DB_PATH: ""})

    await async_setup(hass, {})
    await async_setup_entry(hass, entry)
    unload_ok = await async_unload_entry(hass, entry)

    assert unload_ok is True
    assert entry.runtime_data.token == "token"
    assert fake_hass.data[DOMAIN] == {}


@pytest.mark.asyncio
async def test_async_setup_entry_refreshes_add_transaction_schema(
    config_entry_factory: Callable[..., ConfigEntry[RuntimeData]],
    tmp_path: Path,
) -> None:
    fake_hass = FakeHass()
    hass = cast("HomeAssistant", fake_hass)
    db_path = tmp_path / "ynab-schema.sqlite3"
    seed_db(db_path)

    await async_setup(hass, {})
    entry = config_entry_factory(data={CONF_TOKEN: "token", CONF_DB_PATH: str(db_path)})

    await async_setup_entry(hass, entry)
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
        description["fields"]["account_name"]["selector"]["select"]["custom_value"]
        is True
    )
    assert (
        "My Category Group - My Category"
        in description["fields"]["category_name"]["selector"]["select"]["options"]
    )
    assert description["fields"]["category_name"]["required"] is True
    assert (
        description["fields"]["category_name"]["selector"]["select"]["custom_value"]
        is True
    )
    assert description["fields"]["date"]["default"] == datetime.date.today().isoformat()
    assert (
        "My Payee"
        in description["fields"]["payee_name"]["selector"]["select"]["options"]
    )


@pytest.mark.asyncio
async def test_set_add_transaction_service_schema_handles_bad_options() -> None:
    fake_hass = FakeHass()
    hass = cast("HomeAssistant", fake_hass)
    await async_setup(hass, {})

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


@pytest.mark.asyncio
async def test_async_unload_entry_false_does_not_remove_services(
    config_entry_factory: Callable[..., ConfigEntry[RuntimeData]],
) -> None:
    fake_hass = FakeHass()
    hass = cast("HomeAssistant", fake_hass)
    fake_hass.config_entries.unload_result = False
    entry = config_entry_factory(runtime_data=RuntimeData(token="token", db_path=""))
    fake_hass.data[DOMAIN] = {"entry-1": entry.runtime_data}

    unload_ok = await async_unload_entry(hass, entry)

    assert unload_ok is False
    assert fake_hass.data[DOMAIN] == {"entry-1": entry.runtime_data}


def test_get_runtime_data_raises_without_a_loaded_entry() -> None:
    fake_hass = FakeHass()
    hass = cast("HomeAssistant", fake_hass)

    with pytest.raises(HomeAssistantError, match="Manager for YNAB is not configured"):
        _get_runtime_data(hass)


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
    return_value=AutoApproveResult(transactions=[], updated_count=0),
)
@pytest.mark.asyncio
async def test_register_services_success_and_idempotence(
    run_auto_approve: Mock,
    run_pending_income: Mock,
    run_sqlite_export: Mock,
    run_add_transaction: Mock,
    get_add_transaction_options: AsyncMock,
    run_sql_query: AsyncMock,
    config_entry_factory: Callable[..., ConfigEntry[RuntimeData]],
) -> None:
    fake_hass = FakeHass()
    hass = cast("HomeAssistant", fake_hass)
    runtime_data = RuntimeData(token="token", db_path="/tmp/db.sqlite3")
    fake_hass.data[DOMAIN] = {"entry-1": runtime_data}
    entry = config_entry_factory(runtime_data=runtime_data)

    await _async_register_services(hass)
    await _async_register_services(hass)

    pending = cast(
        "ServiceHandler",
        fake_hass.services.registered[(DOMAIN, SERVICE_PENDING_INCOME)]["handler"],
    )
    auto_approve = cast(
        "ServiceHandler",
        fake_hass.services.registered[(DOMAIN, SERVICE_AUTO_APPROVE)]["handler"],
    )
    sqlite_export = cast(
        "ServiceHandler",
        fake_hass.services.registered[(DOMAIN, SERVICE_SQLITE_EXPORT)]["handler"],
    )
    sqlite_query = cast(
        "ServiceHandler",
        fake_hass.services.registered[(DOMAIN, SERVICE_SQLITE_QUERY)]["handler"],
    )
    add_transaction = cast(
        "ServiceHandler",
        fake_hass.services.registered[(DOMAIN, SERVICE_ADD_TRANSACTION)]["handler"],
    )

    await auto_approve(
        FakeServiceCall(data={"for_real": True, "sync": True, "quiet": True})
    )
    await pending(FakeServiceCall(data={"for_real": True, "sync": True, "quiet": True}))
    await sqlite_export(FakeServiceCall(data={"full_refresh": True, "quiet": False}))
    result = await sqlite_query(
        FakeServiceCall(
            data={
                "sync": True,
                "sql": "select 1",
            }
        )
    )
    await add_transaction(
        FakeServiceCall(
            data={
                "plan_name": "Budget",
                "account_name": "Checking",
                "payee_name": "Store",
                "category_name": "Food - Groceries",
                "date": datetime.date(2026, 5, 1),
                "cleared": "uncleared",
                "amount": Decimal("12.34"),
                "sync": True,
                "quiet": True,
            }
        )
    )

    await fake_hass.async_block_till_done()

    assert result == {"rows": [{"id": 1}]}
    assert len(fake_hass.services.registered) == 5
    assert entry.runtime_data.pending_income_updated_count == 4
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
    assert get_add_transaction_options.await_count == 5
    get_add_transaction_options.assert_has_awaits([call(Path("/tmp/db.sqlite3"))] * 5)


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
                "date": datetime.date(2026, 5, 1),
                "cleared": "uncleared",
                "amount": Decimal("12.34"),
                "sync": False,
                "quiet": True,
            },
            "add_transaction failed: boom",
            id=SERVICE_ADD_TRANSACTION,
        ),
    ],
)
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
) -> None:
    del (
        run_auto_approve,
        run_pending_income,
        run_sqlite_export,
        run_add_transaction,
        run_sql_query,
    )
    fake_hass = FakeHass()
    hass = cast("HomeAssistant", fake_hass)
    fake_hass.data[DOMAIN] = {
        "entry-1": RuntimeData(token="token", db_path="/tmp/db.sqlite3")
    }

    await _async_register_services(hass)
    handler = cast(
        "ServiceHandler",
        fake_hass.services.registered[(DOMAIN, service_name)]["handler"],
    )

    with pytest.raises(HomeAssistantError, match=match):
        await handler(FakeServiceCall(data=data))
