"""The Manager for YNAB integration."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import voluptuous as vol
from functools import partial

from homeassistant.const import Platform
from homeassistant.core import HomeAssistant, ServiceCall, callback
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import config_validation as cv

from . import _api
from .const import CONF_DB_PATH
from .const import CONF_TOKEN
from .const import DOMAIN
from .const import LOGGER
from .const import SERVICE_PENDING_INCOME
from .const import SERVICE_SQLITE_EXPORT

if TYPE_CHECKING:
    from collections.abc import Callable

    from homeassistant.config_entries import ConfigEntry

PLATFORMS: list[Platform] = [Platform.SENSOR]

PENDING_INCOME_SCHEMA = vol.Schema(
    {
        vol.Optional("for_real", default=False): cv.boolean,
        vol.Optional("quiet", default=False): cv.boolean,
    }
)
SQLITE_EXPORT_SCHEMA = vol.Schema(
    {
        vol.Optional("full_refresh", default=False): cv.boolean,
        vol.Optional("quiet", default=False): cv.boolean,
    }
)


@dataclass
class RuntimeData:
    """Mutable runtime state for the config entry."""

    token: str
    db_path: str
    pending_income_updated_count: int | None = None
    _listeners: list[Callable[[], None]] = field(default_factory=list)

    @property
    def resolved_db_path(self) -> Path:
        """Return the configured DB path or the library default path."""
        return Path(self.db_path) if self.db_path else _api.default_db_path()

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


async def async_setup_entry(
    hass: HomeAssistant, entry: ManagerForYnabConfigEntry
) -> bool:
    """Set up Manager for YNAB from a config entry."""
    entry.runtime_data = RuntimeData(
        token=entry.data[CONF_TOKEN], db_path=entry.data[CONF_DB_PATH]
    )

    await _async_register_services(hass, entry)
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(
    hass: HomeAssistant, entry: ManagerForYnabConfigEntry
) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        hass.services.async_remove(DOMAIN, SERVICE_PENDING_INCOME)
        hass.services.async_remove(DOMAIN, SERVICE_SQLITE_EXPORT)
    return unload_ok


async def _async_register_services(
    hass: HomeAssistant, entry: ManagerForYnabConfigEntry
) -> None:
    """Register integration services."""

    async def async_handle_pending_income(call: ServiceCall) -> None:
        runtime_data = entry.runtime_data
        try:
            updated_count = await hass.async_add_executor_job(
                partial(
                    _api.run_pending_income,
                    runtime_data.token,
                    runtime_data.resolved_db_path,
                    for_real=call.data["for_real"],
                    quiet=call.data["quiet"],
                )
            )
        except Exception as err:
            LOGGER.exception("pending_income failed")
            raise HomeAssistantError(f"pending_income failed: {err}") from err

        runtime_data.async_set_pending_income_updated_count(updated_count)

    async def async_handle_sqlite_export(call: ServiceCall) -> None:
        runtime_data = entry.runtime_data
        try:
            await _api.run_sqlite_export(
                runtime_data.token,
                runtime_data.resolved_db_path,
                full_refresh=call.data["full_refresh"],
                quiet=call.data["quiet"],
            )
        except Exception as err:
            LOGGER.exception("sqlite_export failed")
            raise HomeAssistantError(f"sqlite_export failed: {err}") from err

    if not hass.services.has_service(DOMAIN, SERVICE_PENDING_INCOME):
        hass.services.async_register(
            DOMAIN,
            SERVICE_PENDING_INCOME,
            async_handle_pending_income,
            schema=PENDING_INCOME_SCHEMA,
        )

    if not hass.services.has_service(DOMAIN, SERVICE_SQLITE_EXPORT):
        hass.services.async_register(
            DOMAIN,
            SERVICE_SQLITE_EXPORT,
            async_handle_sqlite_export,
            schema=SQLITE_EXPORT_SCHEMA,
        )
