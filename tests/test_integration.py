from __future__ import annotations

import asyncio
import sys
import types
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from typing import TYPE_CHECKING
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

import pytest
import voluptuous as vol
from homeassistant.exceptions import HomeAssistantError

from custom_components.ha_manager_for_ynab import PENDING_INCOME_SCHEMA
from custom_components.ha_manager_for_ynab import SQLITE_EXPORT_SCHEMA
from custom_components.ha_manager_for_ynab import RuntimeData
from custom_components.ha_manager_for_ynab import _api
from custom_components.ha_manager_for_ynab import _async_register_services
from custom_components.ha_manager_for_ynab import async_setup_entry
from custom_components.ha_manager_for_ynab import async_unload_entry
from custom_components.ha_manager_for_ynab.config_flow import ManagerForYnabConfigFlow
from custom_components.ha_manager_for_ynab.config_flow import _user_schema
from custom_components.ha_manager_for_ynab.const import CONF_DB_PATH
from custom_components.ha_manager_for_ynab.const import CONF_TOKEN
from custom_components.ha_manager_for_ynab.const import DOMAIN
from custom_components.ha_manager_for_ynab.const import SERVICE_PENDING_INCOME
from custom_components.ha_manager_for_ynab.const import SERVICE_SQLITE_EXPORT
from custom_components.ha_manager_for_ynab.sensor import PendingIncomeUpdatedCountSensor
from custom_components.ha_manager_for_ynab.sensor import (
    async_setup_entry as sensor_async_setup_entry,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from collections.abc import Coroutine

    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import CALLBACK_TYPE
    from homeassistant.core import HomeAssistant
    from homeassistant.core import ServiceCall
    from homeassistant.helpers.entity import Entity
    from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

    ServiceHandler = Callable[[ServiceCall], Coroutine[Any, Any, None]]


class FakeServices:
    def __init__(self) -> None:
        self.registered: dict[
            tuple[str, str], dict[str, ServiceHandler | object | None]
        ] = {}
        self.removed: list[tuple[str, str]] = []

    def has_service(self, domain: str, service: str) -> bool:
        return (domain, service) in self.registered

    def async_register(
        self, domain: str, service: str, handler: ServiceHandler, schema: object = None
    ) -> None:
        self.registered[(domain, service)] = {"handler": handler, "schema": schema}

    def async_remove(self, domain: str, service: str) -> None:
        self.removed.append((domain, service))


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
        self.services = FakeServices()
        self.config_entries = FakeConfigEntries()
        self.executor_jobs: list[Callable[[], object]] = []

    async def async_add_executor_job(self, func: Callable[[], object]) -> object:
        self.executor_jobs.append(func)
        return func()


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


def test_sensor_async_added_to_hass_registers_listener() -> None:
    runtime_data = RuntimeData(token="token", db_path="/tmp/ynab.sqlite3")
    sensor = PendingIncomeUpdatedCountSensor(runtime_data, "entry-1")
    removers: list[CALLBACK_TYPE] = []
    writes: list[str] = []

    def fake_async_on_remove(func: CALLBACK_TYPE) -> None:
        removers.append(func)

    sensor_any = cast("Any", sensor)
    sensor_any.async_on_remove = fake_async_on_remove
    sensor_any.async_write_ha_state = lambda: writes.append("write")

    asyncio.run(sensor.async_added_to_hass())
    unsubscribe = removers[0]
    runtime_data.async_set_pending_income_updated_count(2)
    unsubscribe()
    runtime_data.async_set_pending_income_updated_count(3)

    assert len(writes) == 1


def test_sensor_async_setup_entry_adds_entity() -> None:
    added: list[Entity] = []
    entry = cast(
        "ConfigEntry[RuntimeData]",
        SimpleNamespace(
            runtime_data=RuntimeData(token="token", db_path=""), entry_id="entry-1"
        ),
    )

    def add_entities(
        new_entities: list[Entity],
        update_before_add: bool = False,
        *,
        config_subentry_id: str | None = None,
    ) -> None:
        del update_before_add, config_subentry_id
        added.extend(new_entities)

    asyncio.run(
        sensor_async_setup_entry(
            cast("HomeAssistant", None),
            entry,
            cast("AddConfigEntryEntitiesCallback", add_entities),
        )
    )

    assert len(added) == 1
    assert isinstance(added[0], PendingIncomeUpdatedCountSensor)


def test_service_schemas_default_false_values() -> None:
    assert PENDING_INCOME_SCHEMA({}) == {"for_real": False, "quiet": False}
    assert SQLITE_EXPORT_SCHEMA({}) == {"full_refresh": False, "quiet": False}


def test_user_schema_uses_default_db_path() -> None:
    default_db_path = Path("/tmp/default.sqlite3")

    with patch(
        "custom_components.ha_manager_for_ynab.config_flow._api.default_db_path",
        return_value=default_db_path,
    ):
        assert _user_schema()({"token": "token"}) == {
            "token": "token",
            "db_path": str(default_db_path),
        }


def test_user_schema_rejects_empty_db_path() -> None:
    with (
        patch(
            "custom_components.ha_manager_for_ynab.config_flow._api.default_db_path",
            return_value=Path("/tmp/default.sqlite3"),
        ),
        pytest.raises(vol.Invalid),
    ):
        _user_schema()({"token": "token", "db_path": ""})


def test_api_default_db_path_delegates() -> None:
    fake_module = cast("Any", types.ModuleType("sqlite_export_for_ynab"))
    fake_module.default_db_path = lambda: Path("/tmp/default.sqlite3")

    with patch.dict(sys.modules, {"sqlite_export_for_ynab": fake_module}):
        assert _api.default_db_path() == Path("/tmp/default.sqlite3")


def test_api_run_pending_income_returns_updated_count() -> None:
    fake_module = cast("Any", types.ModuleType("manager_for_ynab.pending_income"))
    fake_module.pending_income = MagicMock(
        return_value=SimpleNamespace(updated_count=11)
    )

    with patch.dict(sys.modules, {"manager_for_ynab.pending_income": fake_module}):
        assert (
            _api.run_pending_income(
                "token", Path("/tmp/db.sqlite3"), for_real=True, quiet=False
            )
            == 11
        )
        fake_module.pending_income.assert_called_once_with(
            db=Path("/tmp/db.sqlite3"),
            for_real=True,
            quiet=False,
            token_override="token",
        )


def test_api_run_sqlite_export_delegates() -> None:
    fake_module = cast("Any", types.ModuleType("sqlite_export_for_ynab._main"))
    fake_sync = AsyncMock()
    fake_module.sync = fake_sync

    with patch.dict(sys.modules, {"sqlite_export_for_ynab._main": fake_module}):
        asyncio.run(
            _api.run_sqlite_export(
                "token",
                Path("/tmp/db.sqlite3"),
                full_refresh=True,
                quiet=False,
            )
        )

    fake_sync.assert_awaited_once_with(
        "token", Path("/tmp/db.sqlite3"), True, quiet=False
    )


def test_config_flow_user_shows_form() -> None:
    flow = ManagerForYnabConfigFlow()
    flow_any = cast("Any", flow)
    flow_any.async_show_form = MagicMock(return_value={"type": "form"})

    result = asyncio.run(flow.async_step_user())

    assert result == {"type": "form"}
    flow_any.async_show_form.assert_called_once()


def test_config_flow_user_creates_entry() -> None:
    flow = ManagerForYnabConfigFlow()
    flow_any = cast("Any", flow)
    flow_any.async_set_unique_id = AsyncMock()
    flow_any._abort_if_unique_id_configured = MagicMock()
    flow_any.async_create_entry = MagicMock(return_value={"type": "create_entry"})

    result = asyncio.run(
        flow.async_step_user({"token": "token", "db_path": "/tmp/ynab.sqlite3"})
    )

    assert result == {"type": "create_entry"}
    flow_any.async_set_unique_id.assert_awaited_once_with(DOMAIN)
    flow_any._abort_if_unique_id_configured.assert_called_once_with()
    flow_any.async_create_entry.assert_called_once_with(
        title="Manager for YNAB",
        data={"token": "token", "db_path": "/tmp/ynab.sqlite3"},
    )


def test_async_setup_and_unload_entry() -> None:
    hass = cast("HomeAssistant", FakeHass())
    entry = cast(
        "ConfigEntry[RuntimeData]",
        SimpleNamespace(data={CONF_TOKEN: "token", CONF_DB_PATH: "/tmp/db.sqlite3"}),
    )

    asyncio.run(async_setup_entry(hass, entry))
    unload_ok = asyncio.run(async_unload_entry(hass, entry))

    assert unload_ok is True
    assert entry.runtime_data.token == "token"
    assert hass.services.has_service(DOMAIN, SERVICE_PENDING_INCOME)
    assert hass.services.has_service(DOMAIN, SERVICE_SQLITE_EXPORT)
    assert (DOMAIN, SERVICE_PENDING_INCOME) in cast("FakeHass", hass).services.removed
    assert (DOMAIN, SERVICE_SQLITE_EXPORT) in cast("FakeHass", hass).services.removed


def test_async_unload_entry_false_does_not_remove_services() -> None:
    hass = cast("HomeAssistant", FakeHass())
    cast("Any", hass).config_entries.unload_result = False
    entry = cast(
        "ConfigEntry[RuntimeData]",
        SimpleNamespace(runtime_data=RuntimeData(token="token", db_path="")),
    )

    unload_ok = asyncio.run(async_unload_entry(hass, entry))

    assert unload_ok is False
    assert cast("FakeHass", hass).services.removed == []


def test_register_services_success_and_idempotence() -> None:
    hass = cast("HomeAssistant", FakeHass())
    entry = cast(
        "ConfigEntry[RuntimeData]",
        SimpleNamespace(
            runtime_data=RuntimeData(token="token", db_path="/tmp/db.sqlite3")
        ),
    )

    with (
        patch(
            "custom_components.ha_manager_for_ynab._api.run_pending_income",
            return_value=4,
        ),
        patch(
            "custom_components.ha_manager_for_ynab._api.run_sqlite_export",
            new=AsyncMock(),
        ) as run_sqlite_export,
    ):
        asyncio.run(_async_register_services(hass, entry))
        asyncio.run(_async_register_services(hass, entry))

        pending = cast(
            "ServiceHandler",
            cast("Any", hass).services.registered[(DOMAIN, SERVICE_PENDING_INCOME)][
                "handler"
            ],
        )
        sqlite_export = cast(
            "ServiceHandler",
            cast("Any", hass).services.registered[(DOMAIN, SERVICE_SQLITE_EXPORT)][
                "handler"
            ],
        )

        asyncio.run(
            pending(
                cast(
                    "ServiceCall",
                    SimpleNamespace(data={"for_real": True, "quiet": True}),
                )
            )
        )
        asyncio.run(
            sqlite_export(
                cast(
                    "ServiceCall",
                    SimpleNamespace(data={"full_refresh": True, "quiet": False}),
                )
            )
        )

    assert len(cast("Any", hass).services.registered) == 2
    assert entry.runtime_data.pending_income_updated_count == 4
    run_sqlite_export.assert_awaited_once_with(
        "token",
        Path("/tmp/db.sqlite3"),
        full_refresh=True,
        quiet=False,
    )


def test_register_services_error_paths_raise_home_assistant_error() -> None:
    hass = cast("HomeAssistant", FakeHass())
    entry = cast(
        "ConfigEntry[RuntimeData]",
        SimpleNamespace(
            runtime_data=RuntimeData(token="token", db_path="/tmp/db.sqlite3")
        ),
    )

    with (
        patch(
            "custom_components.ha_manager_for_ynab._api.run_pending_income",
            side_effect=RuntimeError("boom"),
        ),
        patch(
            "custom_components.ha_manager_for_ynab._api.run_sqlite_export",
            side_effect=RuntimeError("boom"),
        ),
    ):
        asyncio.run(_async_register_services(hass, entry))
        pending = cast(
            "ServiceHandler",
            cast("Any", hass).services.registered[(DOMAIN, SERVICE_PENDING_INCOME)][
                "handler"
            ],
        )
        sqlite_export = cast(
            "ServiceHandler",
            cast("Any", hass).services.registered[(DOMAIN, SERVICE_SQLITE_EXPORT)][
                "handler"
            ],
        )

        with pytest.raises(HomeAssistantError, match="pending_income failed: boom"):
            asyncio.run(
                pending(
                    cast(
                        "ServiceCall",
                        SimpleNamespace(data={"for_real": False, "quiet": False}),
                    )
                )
            )

        with pytest.raises(HomeAssistantError, match="sqlite_export failed: boom"):
            asyncio.run(
                sqlite_export(
                    cast(
                        "ServiceCall",
                        SimpleNamespace(data={"full_refresh": False, "quiet": False}),
                    )
                )
            )
