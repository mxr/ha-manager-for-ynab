"""The Manager for YNAB integration."""

from __future__ import annotations

import datetime
from dataclasses import dataclass
from dataclasses import field
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING

import voluptuous as vol
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant
from homeassistant.core import ServiceCall
from homeassistant.core import SupportsResponse
from homeassistant.core import callback
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers import service as service_helper

from . import _api
from .const import ATTR_ACCOUNT_NAME
from .const import ATTR_AMOUNT
from .const import ATTR_CATEGORY_NAME
from .const import ATTR_CLEARED
from .const import ATTR_DATE
from .const import ATTR_PAYEE_NAME
from .const import ATTR_PLAN_NAME
from .const import ATTR_SQL
from .const import CLEARED_DEFAULT
from .const import CLEARED_OPTIONS
from .const import CONF_DB_PATH
from .const import CONF_TOKEN
from .const import DOMAIN
from .const import LOGGER
from .const import SERVICE_ADD_TRANSACTION
from .const import SERVICE_AUTO_APPROVE
from .const import SERVICE_PENDING_INCOME
from .const import SERVICE_SQLITE_EXPORT
from .const import SERVICE_SQLITE_QUERY

if TYPE_CHECKING:
    from collections.abc import Callable

    from homeassistant.config_entries import ConfigEntry
    from homeassistant.helpers.typing import ConfigType

PLATFORMS: list[Platform] = [Platform.SENSOR]

PENDING_INCOME_SCHEMA = vol.Schema(
    {
        vol.Required("for_real", default=False): cv.boolean,
        vol.Required("sync", default=True): cv.boolean,
        vol.Required("quiet", default=False): cv.boolean,
    }
)
AUTO_APPROVE_SCHEMA = vol.Schema(
    {
        vol.Required("for_real", default=False): cv.boolean,
        vol.Required("sync", default=True): cv.boolean,
        vol.Required("quiet", default=False): cv.boolean,
    }
)
SQLITE_EXPORT_SCHEMA = vol.Schema(
    {
        vol.Required("full_refresh", default=False): cv.boolean,
        vol.Required("quiet", default=False): cv.boolean,
    }
)
SQLITE_QUERY_SCHEMA = vol.Schema(
    {
        vol.Required("sync", default=True): cv.boolean,
        vol.Required(ATTR_SQL): cv.string,
    }
)
ADD_TRANSACTION_SCHEMA = vol.Schema(
    {
        vol.Optional(ATTR_PLAN_NAME): cv.string,
        vol.Required(ATTR_ACCOUNT_NAME): cv.string,
        vol.Required(ATTR_PAYEE_NAME): cv.string,
        vol.Optional(ATTR_CATEGORY_NAME): cv.string,
        vol.Required(
            ATTR_DATE, default=lambda: datetime.date.today().isoformat()
        ): vol.Coerce(datetime.date.fromisoformat),
        vol.Required(ATTR_CLEARED, default=CLEARED_DEFAULT): vol.In(CLEARED_OPTIONS),
        vol.Required(ATTR_AMOUNT): vol.Coerce(lambda value: Decimal(str(value))),
        vol.Required("sync", default=True): cv.boolean,
        vol.Required("quiet", default=False): cv.boolean,
    }
)


@dataclass
class RuntimeData:
    """Mutable runtime state for the config entry."""

    token: str
    db_path: str
    pending_income_updated_count: int | None = None
    _listeners: list[Callable[[], None]] = field(default_factory=list)

    @callback
    def async_add_listener(self, listener: Callable[[], None]) -> Callable[[], None]:
        """Register a listener for in-memory state changes."""
        self._listeners.append(listener)

        @callback
        def unsubscribe() -> None:
            if listener in self._listeners:
                self._listeners.remove(listener)

        return unsubscribe

    @callback
    def async_set_pending_income_updated_count(self, updated_count: int) -> None:
        """Store the last successful pending income count."""
        self.pending_income_updated_count = updated_count
        for listener in list(self._listeners):
            listener()


type ManagerForYnabConfigEntry = ConfigEntry[RuntimeData]


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Set up the Manager for YNAB integration."""
    hass.data.setdefault(DOMAIN, {})
    await _async_register_services(hass)
    return True


async def async_setup_entry(
    hass: HomeAssistant, entry: ManagerForYnabConfigEntry
) -> bool:
    """Set up Manager for YNAB from a config entry."""
    entry.runtime_data = RuntimeData(
        token=entry.data[CONF_TOKEN], db_path=entry.data[CONF_DB_PATH]
    )
    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = entry.runtime_data
    await _update_add_transaction_service_schema(hass, entry.runtime_data)
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(
    hass: HomeAssistant, entry: ManagerForYnabConfigEntry
) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        hass.data[DOMAIN].pop(entry.entry_id, None)
    return unload_ok


async def _async_register_services(hass: HomeAssistant) -> None:
    """Register integration services."""

    async def async_handle_pending_income(call: ServiceCall) -> None:
        runtime_data = _get_runtime_data(hass)
        try:
            result = await _api.run_pending_income(
                runtime_data.token,
                Path(runtime_data.db_path),
                for_real=call.data["for_real"],
                sync=call.data["sync"],
                quiet=call.data["quiet"],
            )
        except Exception as err:
            LOGGER.exception("pending_income failed")
            raise HomeAssistantError(f"pending_income failed: {err}") from err

        runtime_data.async_set_pending_income_updated_count(result.updated_count)
        if call.data["sync"]:
            _schedule_update_add_transaction_service_schema(hass, runtime_data)

    async def async_handle_auto_approve(call: ServiceCall) -> None:
        runtime_data = _get_runtime_data(hass)
        try:
            await _api.run_auto_approve(
                runtime_data.token,
                Path(runtime_data.db_path),
                for_real=call.data["for_real"],
                sync=call.data["sync"],
                quiet=call.data["quiet"],
            )
        except Exception as err:
            LOGGER.exception("auto_approve failed")
            raise HomeAssistantError(f"auto_approve failed: {err}") from err

        if call.data["sync"]:
            _schedule_update_add_transaction_service_schema(hass, runtime_data)

    async def async_handle_sqlite_export(call: ServiceCall) -> None:
        runtime_data = _get_runtime_data(hass)
        try:
            await _api.run_sqlite_export(
                runtime_data.token,
                Path(runtime_data.db_path),
                full_refresh=call.data["full_refresh"],
                quiet=call.data["quiet"],
            )
            _schedule_update_add_transaction_service_schema(hass, runtime_data)
        except Exception as err:
            LOGGER.exception("sqlite_export failed")
            raise HomeAssistantError(f"sqlite_export failed: {err}") from err

    async def async_handle_sqlite_query(call: ServiceCall) -> dict[str, object]:
        runtime_data = _get_runtime_data(hass)
        try:
            if call.data["sync"]:
                await _api.run_sqlite_export(
                    runtime_data.token,
                    Path(runtime_data.db_path),
                    full_refresh=False,
                    quiet=True,
                )
                _schedule_update_add_transaction_service_schema(hass, runtime_data)
            result = await _api.run_sql_query(
                Path(runtime_data.db_path),
                call.data[ATTR_SQL],
            )
        except Exception as err:
            LOGGER.exception("sqlite_query failed")
            raise HomeAssistantError(f"sqlite_query failed: {err}") from err

        return result

    async def async_handle_add_transaction(call: ServiceCall) -> None:
        runtime_data = _get_runtime_data(hass)
        try:
            await _api.run_add_transaction(
                runtime_data.token,
                Path(runtime_data.db_path),
                plan_name=call.data.get(ATTR_PLAN_NAME),
                account_name=call.data[ATTR_ACCOUNT_NAME],
                payee_name=call.data[ATTR_PAYEE_NAME],
                category_name=call.data.get(ATTR_CATEGORY_NAME),
                date=call.data[ATTR_DATE],
                cleared=call.data[ATTR_CLEARED],
                amount=call.data[ATTR_AMOUNT],
                sync=call.data["sync"],
                quiet=call.data["quiet"],
            )
            if call.data["sync"]:
                _schedule_update_add_transaction_service_schema(hass, runtime_data)
        except Exception as err:
            LOGGER.exception("add_transaction failed")
            raise HomeAssistantError(f"add_transaction failed: {err}") from err

    if not hass.services.has_service(DOMAIN, SERVICE_PENDING_INCOME):
        hass.services.async_register(
            DOMAIN,
            SERVICE_PENDING_INCOME,
            async_handle_pending_income,
            schema=PENDING_INCOME_SCHEMA,
        )

    if not hass.services.has_service(DOMAIN, SERVICE_AUTO_APPROVE):
        hass.services.async_register(
            DOMAIN,
            SERVICE_AUTO_APPROVE,
            async_handle_auto_approve,
            schema=AUTO_APPROVE_SCHEMA,
        )

    if not hass.services.has_service(DOMAIN, SERVICE_SQLITE_EXPORT):
        hass.services.async_register(
            DOMAIN,
            SERVICE_SQLITE_EXPORT,
            async_handle_sqlite_export,
            schema=SQLITE_EXPORT_SCHEMA,
        )

    if not hass.services.has_service(DOMAIN, SERVICE_SQLITE_QUERY):
        hass.services.async_register(
            DOMAIN,
            SERVICE_SQLITE_QUERY,
            async_handle_sqlite_query,
            schema=SQLITE_QUERY_SCHEMA,
            supports_response=SupportsResponse.ONLY,
        )

    if not hass.services.has_service(DOMAIN, SERVICE_ADD_TRANSACTION):
        hass.services.async_register(
            DOMAIN,
            SERVICE_ADD_TRANSACTION,
            async_handle_add_transaction,
            schema=ADD_TRANSACTION_SCHEMA,
        )


@callback
def _schedule_update_add_transaction_service_schema(
    hass: HomeAssistant, runtime_data: RuntimeData
) -> None:
    hass.async_create_task(
        _update_add_transaction_service_schema(hass, runtime_data),
        "ha_manager_for_ynab update add_transaction schema",
    )


async def _update_add_transaction_service_schema(
    hass: HomeAssistant, runtime_data: RuntimeData
) -> None:
    """Update add-transaction service dropdowns from the current SQLite export."""
    try:
        options = await _api.get_add_transaction_options(Path(runtime_data.db_path))
    except Exception:
        LOGGER.debug("Could not load add_transaction service choices", exc_info=True)
        return

    _set_add_transaction_service_schema(hass, options)


@callback
def _set_add_transaction_service_schema(
    hass: HomeAssistant, options: dict[str, object]
) -> None:
    """Set dynamic service metadata for add_transaction."""
    plans = _as_string_list(options.get("plans"))
    accounts = _unique_sorted_options(options.get("accounts_by_plan"))
    categories = _unique_sorted_options(options.get("categories_by_plan"))
    payees = _unique_sorted_options(options.get("payees_by_plan"))
    default_plan_name = options.get("default_plan_name")

    plan_field: dict[str, object] = {
        "name": "Plan",
        "description": "Plan name from the SQLite export. Optional when only one plan exists.",
        "required": False,
        "selector": {"select": {"options": plans, "mode": "dropdown"}},
    }
    if isinstance(default_plan_name, str):
        plan_field["default"] = default_plan_name

    service_helper.async_set_service_schema(
        hass,
        DOMAIN,
        SERVICE_ADD_TRANSACTION,
        {
            "name": "Add transaction",
            "description": "Create a YNAB transaction using choices from the SQLite export.",
            "fields": {
                ATTR_PLAN_NAME: plan_field,
                ATTR_ACCOUNT_NAME: {
                    "name": "Account",
                    "description": "Account name from the SQLite export.",
                    "required": True,
                    "selector": {"select": {"options": accounts, "mode": "dropdown"}},
                },
                ATTR_PAYEE_NAME: {
                    "name": "Payee",
                    "description": "Payee name. Existing payees are resolved from the SQLite export.",
                    "required": True,
                    "selector": {
                        "select": {
                            "options": payees,
                            "custom_value": True,
                            "mode": "dropdown",
                        }
                    },
                },
                ATTR_CATEGORY_NAME: {
                    "name": "Category",
                    "description": "Category as category group and name separated by a hyphen.",
                    "required": False,
                    "selector": {"select": {"options": categories, "mode": "dropdown"}},
                },
                ATTR_DATE: {
                    "name": "Date",
                    "description": "Transaction date.",
                    "required": True,
                    "selector": {"date": {}},
                },
                ATTR_CLEARED: {
                    "name": "Cleared",
                    "description": "Cleared status for the new transaction.",
                    "required": True,
                    "default": CLEARED_DEFAULT,
                    "selector": {
                        "select": {
                            "options": CLEARED_OPTIONS,
                            "mode": "dropdown",
                        }
                    },
                },
                ATTR_AMOUNT: {
                    "name": "Amount",
                    "description": "Transaction amount. Positive values are expenses.",
                    "required": True,
                    "selector": {"number": {"mode": "box", "step": 0.01}},
                },
                "sync": {
                    "name": "Sync",
                    "description": "Sync the SQLite export before creating the transaction.",
                    "required": True,
                    "default": True,
                    "selector": {"boolean": {}},
                },
                "quiet": {
                    "name": "Quiet",
                    "description": "Suppress output from the underlying libraries.",
                    "required": True,
                    "default": False,
                    "selector": {"boolean": {}},
                },
            },
        },
    )


def _as_string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]


def _unique_sorted_options(value: object) -> list[str]:
    if not isinstance(value, dict):
        return []

    options: set[str] = set()
    for values in value.values():
        if isinstance(values, list):
            options.update(item for item in values if isinstance(item, str))
    return sorted(options, key=str.lower)


@callback
def _get_runtime_data(hass: HomeAssistant) -> RuntimeData:
    """Return the configured runtime data for the single integration entry."""
    runtime_data_by_entry: dict[str, RuntimeData] = hass.data.get(DOMAIN, {})
    if len(runtime_data_by_entry) != 1:
        raise HomeAssistantError("Manager for YNAB is not configured")

    return next(iter(runtime_data_by_entry.values()))
