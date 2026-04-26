from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from typing import TYPE_CHECKING
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

import pytest
import voluptuous as vol
import aiosqlite
from homeassistant.core import State
from homeassistant.exceptions import HomeAssistantError
from manager_for_ynab.auto_approve import AutoApproveResult
from manager_for_ynab.pending_income import PendingIncomeResult

from custom_components.ha_manager_for_ynab import AUTO_APPROVE_SCHEMA
from custom_components.ha_manager_for_ynab import PENDING_INCOME_SCHEMA
from custom_components.ha_manager_for_ynab import SQLITE_QUERY_SCHEMA
from custom_components.ha_manager_for_ynab import SQLITE_EXPORT_SCHEMA
from custom_components.ha_manager_for_ynab import RuntimeData
from custom_components.ha_manager_for_ynab import _api
from custom_components.ha_manager_for_ynab import async_setup
from custom_components.ha_manager_for_ynab import _get_runtime_data
from custom_components.ha_manager_for_ynab import _async_register_services
from custom_components.ha_manager_for_ynab import async_setup_entry
from custom_components.ha_manager_for_ynab import async_unload_entry
from custom_components.ha_manager_for_ynab.config_flow import ManagerForYnabConfigFlow
from custom_components.ha_manager_for_ynab.config_flow import _user_schema
from custom_components.ha_manager_for_ynab.const import SERVICE_AUTO_APPROVE
from custom_components.ha_manager_for_ynab.const import CONF_DB_PATH
from custom_components.ha_manager_for_ynab.const import CONF_TOKEN
from custom_components.ha_manager_for_ynab.const import DOMAIN
from custom_components.ha_manager_for_ynab.const import SERVICE_PENDING_INCOME
from custom_components.ha_manager_for_ynab.const import SERVICE_SQLITE_EXPORT
from custom_components.ha_manager_for_ynab.const import SERVICE_SQLITE_QUERY
from custom_components.ha_manager_for_ynab.sensor import PendingIncomeUpdatedCountSensor
from custom_components.ha_manager_for_ynab.sensor import (
    async_setup_entry as sensor_async_setup_entry,
)
from tests.fixtures import config_entry_factory as config_entry_factory

if TYPE_CHECKING:
    from collections.abc import Callable
    from collections.abc import Coroutine

    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant
    from homeassistant.helpers.entity import Entity
    from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

    ServiceHandler = Callable[[object], Coroutine[Any, Any, object | None]]


class FakeServices:
    def __init__(self) -> None:
        self.registered: dict[
            tuple[str, str], dict[str, ServiceHandler | object | None]
        ] = {}

    def has_service(self, domain: str, service: str) -> bool:
        return (domain, service) in self.registered

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


def test_service_schemas_default_false_values() -> None:
    assert AUTO_APPROVE_SCHEMA({}) == {"for_real": False, "quiet": False}
    assert PENDING_INCOME_SCHEMA({}) == {"for_real": False, "quiet": False}
    assert SQLITE_EXPORT_SCHEMA({}) == {"full_refresh": False, "quiet": False}
    assert SQLITE_QUERY_SCHEMA({"sql": "select 1"}) == {"sql": "select 1"}


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
        "token", Path("/tmp/db.sqlite3"), for_real=True, quiet=False
    )
    assert ret == PendingIncomeResult(transactions=[], updated_count=11)
    pending_income.assert_awaited_once_with(
        db=Path("/tmp/db.sqlite3"),
        full_refresh=False,
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
        "token", Path("/tmp/db.sqlite3"), for_real=True, quiet=False
    )
    assert ret == AutoApproveResult(transactions=[], updated_count=9)
    auto_approve.assert_awaited_once_with(
        db=Path("/tmp/db.sqlite3"),
        full_refresh=False,
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
    with sqlite3.connect(db_path) as connection:
        connection.execute("create table budgets (id integer, name text)")
        connection.execute("insert into budgets values (1, 'Home')")
        connection.execute("insert into budgets values (2, 'Travel')")
        connection.commit()

    assert await _api.run_sql_query(
        db_path, "select id, name from budgets order by id"
    ) == {
        "rows": [{"id": 1, "name": "Home"}, {"id": 2, "name": "Travel"}],
    }
    assert await _api.run_sql_query(db_path, "PRAGMA query_only = ON;") == {}


@pytest.mark.asyncio
async def test_api_run_sql_query_write(tmp_path: Path) -> None:
    db_path = tmp_path / "db.sqlite3"
    with sqlite3.connect(db_path) as connection:
        connection.execute("create table budgets (id integer, name text)")
        connection.commit()

    with pytest.raises(aiosqlite.DatabaseError):
        await _api.run_sql_query(db_path, "insert into budgets values (1, 'Home')")

    with sqlite3.connect(db_path) as connection:
        assert connection.execute("select count(*) from budgets").fetchone() == (0,)


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

    await auto_approve(FakeServiceCall(data={"for_real": True, "quiet": True}))
    await pending(FakeServiceCall(data={"for_real": True, "quiet": True}))
    await sqlite_export(FakeServiceCall(data={"full_refresh": True, "quiet": False}))
    result = await sqlite_query(
        FakeServiceCall(
            data={
                "sql": "select 1",
            }
        )
    )

    assert result == {"rows": [{"id": 1}]}
    assert len(fake_hass.services.registered) == 4
    assert entry.runtime_data.pending_income_updated_count == 4
    run_auto_approve.assert_called_once_with(
        "token",
        Path("/tmp/db.sqlite3"),
        for_real=True,
        quiet=True,
    )
    run_pending_income.assert_called_once_with(
        "token",
        Path("/tmp/db.sqlite3"),
        for_real=True,
        quiet=True,
    )
    run_sqlite_export.assert_called_once_with(
        "token",
        Path("/tmp/db.sqlite3"),
        full_refresh=True,
        quiet=False,
    )
    run_sql_query.assert_awaited_once_with(
        Path("/tmp/db.sqlite3"),
        "select 1",
    )


@patch(
    "custom_components.ha_manager_for_ynab._api.run_sql_query",
    new_callable=AsyncMock,
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
@pytest.mark.asyncio
async def test_register_services_error_paths_raise_home_assistant_error(
    run_auto_approve: Mock,
    run_pending_income: Mock,
    run_sqlite_export: Mock,
    run_sql_query: AsyncMock,
) -> None:
    del run_auto_approve, run_pending_income, run_sqlite_export, run_sql_query
    fake_hass = FakeHass()
    hass = cast("HomeAssistant", fake_hass)
    fake_hass.data[DOMAIN] = {
        "entry-1": RuntimeData(token="token", db_path="/tmp/db.sqlite3")
    }

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

    with pytest.raises(HomeAssistantError, match="auto_approve failed: boom"):
        await auto_approve(FakeServiceCall(data={"for_real": False, "quiet": False}))

    with pytest.raises(HomeAssistantError, match="pending_income failed: boom"):
        await pending(FakeServiceCall(data={"for_real": False, "quiet": False}))

    with pytest.raises(HomeAssistantError, match="sqlite_export failed: boom"):
        await sqlite_export(
            FakeServiceCall(data={"full_refresh": False, "quiet": False})
        )

    with pytest.raises(HomeAssistantError, match="sqlite_query failed: boom"):
        await sqlite_query(FakeServiceCall(data={"sql": "select 1"}))
