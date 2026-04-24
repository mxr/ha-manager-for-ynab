"""The Manager for YNAB integration."""

from __future__ import annotations

from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING

import voluptuous as vol

from homeassistant.const import Platform
from homeassistant.core import HomeAssistant, ServiceCall, SupportsResponse, callback
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import config_validation as cv

from . import _api
from .const import ATTR_OUTPUT_FORMAT
from .const import ATTR_SQL
from .const import CONF_DB_PATH
from .const import CONF_TOKEN
from .const import DOMAIN
from .const import LOGGER
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
        vol.Required("quiet", default=False): cv.boolean,
    }
)
AUTO_APPROVE_SCHEMA = vol.Schema(
    {
        vol.Required("for_real", default=False): cv.boolean,
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
        vol.Required(ATTR_SQL): cv.string,
        vol.Optional(ATTR_OUTPUT_FORMAT, default="json"): vol.In(("json", "csv")),
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
            updated_count = await hass.async_add_executor_job(
                partial(
                    _api.run_pending_income,
                    runtime_data.token,
                    Path(runtime_data.db_path),
                    for_real=call.data["for_real"],
                    quiet=call.data["quiet"],
                )
            )
        except Exception as err:
            LOGGER.exception("pending_income failed")
            raise HomeAssistantError(f"pending_income failed: {err}") from err

        runtime_data.async_set_pending_income_updated_count(updated_count)

    async def async_handle_auto_approve(call: ServiceCall) -> None:
        runtime_data = _get_runtime_data(hass)
        try:
            await hass.async_add_executor_job(
                partial(
                    _api.run_auto_approve,
                    runtime_data.token,
                    Path(runtime_data.db_path),
                    for_real=call.data["for_real"],
                    quiet=call.data["quiet"],
                )
            )
        except Exception as err:
            LOGGER.exception("auto_approve failed")
            raise HomeAssistantError(f"auto_approve failed: {err}") from err

    async def async_handle_sqlite_export(call: ServiceCall) -> None:
        runtime_data = _get_runtime_data(hass)
        try:
            await _api.run_sqlite_export(
                runtime_data.token,
                Path(runtime_data.db_path),
                full_refresh=call.data["full_refresh"],
                quiet=call.data["quiet"],
            )
        except Exception as err:
            LOGGER.exception("sqlite_export failed")
            raise HomeAssistantError(f"sqlite_export failed: {err}") from err

    async def async_handle_sqlite_query(call: ServiceCall) -> dict[str, object]:
        runtime_data = _get_runtime_data(hass)
        try:
            result = await hass.async_add_executor_job(
                partial(
                    _api.run_sql_query,
                    Path(runtime_data.db_path),
                    call.data[ATTR_SQL],
                    output_format=call.data[ATTR_OUTPUT_FORMAT],
                )
            )
        except Exception as err:
            LOGGER.exception("sqlite_query failed")
            raise HomeAssistantError(f"sqlite_query failed: {err}") from err

        return result

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


@callback
def _get_runtime_data(hass: HomeAssistant) -> RuntimeData:
    """Return the configured runtime data for the single integration entry."""
    runtime_data_by_entry: dict[str, RuntimeData] = hass.data.get(DOMAIN, {})
    if len(runtime_data_by_entry) != 1:
        raise HomeAssistantError("Manager for YNAB is not configured")

    return next(iter(runtime_data_by_entry.values()))
