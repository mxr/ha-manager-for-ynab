"""Sensor platform for Manager for YNAB."""

from __future__ import annotations

from typing import TYPE_CHECKING

from homeassistant.components.sensor import SensorEntity
from homeassistant.components.sensor import SensorEntityDescription
from homeassistant.components.sensor import SensorStateClass
from homeassistant.helpers.device_registry import DeviceEntryType
from homeassistant.helpers.device_registry import DeviceInfo

from .const import DOMAIN
from .const import NAME

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant
    from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

    from . import RuntimeData

SENSOR_DESCRIPTION = SensorEntityDescription(
    key="pending_income_updated_count",
    translation_key="pending_income_updated_count",
    icon="mdi:cash-clock",
    state_class=SensorStateClass.MEASUREMENT,
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry[RuntimeData],
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Set up Manager for YNAB sensors."""
    async_add_entities(
        [PendingIncomeUpdatedCountSensor(entry.runtime_data, entry.entry_id)]
    )


class PendingIncomeUpdatedCountSensor(SensorEntity):
    """Sensor that exposes the last pending income update count."""

    entity_description = SENSOR_DESCRIPTION
    _attr_has_entity_name = True

    def __init__(self, runtime_data: RuntimeData, entry_id: str) -> None:
        """Initialize the sensor."""
        self._runtime_data = runtime_data
        self._attr_unique_id = f"{entry_id}_pending_income_updated_count"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, entry_id)},
            name=NAME,
            manufacturer="YNAB",
            entry_type=DeviceEntryType.SERVICE,
        )

    async def async_added_to_hass(self) -> None:
        """Register update callback when added."""
        self.async_on_remove(
            self._runtime_data.async_add_listener(self.async_write_ha_state)
        )

    @property
    def native_value(self) -> int | None:
        """Return the latest pending income updated count."""
        return self._runtime_data.pending_income_updated_count
