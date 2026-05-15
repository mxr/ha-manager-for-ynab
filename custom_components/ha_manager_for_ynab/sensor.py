"""Sensor platform for Manager for YNAB."""

from __future__ import annotations

from contextlib import suppress
from typing import TYPE_CHECKING

from homeassistant.components.sensor import SensorEntity
from homeassistant.components.sensor import SensorEntityDescription
from homeassistant.components.sensor import SensorStateClass
from homeassistant.const import STATE_UNAVAILABLE
from homeassistant.const import STATE_UNKNOWN
from homeassistant.helpers.device_registry import DeviceEntryType
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.restore_state import RestoreEntity

from .const import DOMAIN
from .const import NAME

if TYPE_CHECKING:
    from collections.abc import Callable

    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant
    from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

    from . import RuntimeData

PENDING_INCOME_UPDATED_COUNT_DESCRIPTION = SensorEntityDescription(
    key="pending_income_updated_count",
    translation_key="pending_income_updated_count",
    icon="mdi:cash-clock",
    state_class=SensorStateClass.MEASUREMENT,
)

AUTO_APPROVE_APPROVED_COUNT_DESCRIPTION = SensorEntityDescription(
    key="auto_approve_approved_count",
    translation_key="auto_approve_approved_count",
    icon="mdi:cash-check",
    state_class=SensorStateClass.MEASUREMENT,
)

AUTO_APPROVE_CLEARED_COUNT_DESCRIPTION = SensorEntityDescription(
    key="auto_approve_cleared_count",
    translation_key="auto_approve_cleared_count",
    icon="mdi:cash-fast",
    state_class=SensorStateClass.MEASUREMENT,
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry[RuntimeData],
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Set up Manager for YNAB sensors."""
    async_add_entities(
        [
            PendingIncomeUpdatedCountSensor(entry.runtime_data, entry.entry_id),
            AutoApproveApprovedCountSensor(entry.runtime_data, entry.entry_id),
            AutoApproveClearedCountSensor(entry.runtime_data, entry.entry_id),
        ]
    )


class ManagerForYnabCountSensor(RestoreEntity, SensorEntity):
    """Sensor that exposes a restored runtime count."""

    _attr_has_entity_name = True

    def __init__(
        self,
        runtime_data: RuntimeData,
        entry_id: str,
        description: SensorEntityDescription,
        native_value_getter: Callable[[RuntimeData], int | None],
        native_value_setter: Callable[[RuntimeData, int], None],
    ) -> None:
        """Initialize the sensor."""
        self._runtime_data = runtime_data
        self.entity_description = description
        self._native_value_getter = native_value_getter
        self._native_value_setter = native_value_setter
        self._attr_unique_id = f"{entry_id}_{description.key}"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, entry_id)},
            name=NAME,
            manufacturer="YNAB",
            entry_type=DeviceEntryType.SERVICE,
        )

    async def async_added_to_hass(self) -> None:
        """Register update callback when added."""
        await super().async_added_to_hass()

        if (
            self.native_value is None
            and (last_state := await self.async_get_last_state()) is not None
            and last_state.state not in (STATE_UNAVAILABLE, STATE_UNKNOWN)
        ):
            with suppress(ValueError):
                self._native_value_setter(self._runtime_data, int(last_state.state))

        self.async_on_remove(
            self._runtime_data.async_add_listener(self.async_write_ha_state)
        )

    @property
    def native_value(self) -> int | None:
        """Return the latest count."""
        return self._native_value_getter(self._runtime_data)


class PendingIncomeUpdatedCountSensor(ManagerForYnabCountSensor):
    """Sensor that exposes the last pending income update count."""

    def __init__(self, runtime_data: RuntimeData, entry_id: str) -> None:
        """Initialize the sensor."""
        super().__init__(
            runtime_data,
            entry_id,
            PENDING_INCOME_UPDATED_COUNT_DESCRIPTION,
            lambda data: data.pending_income_updated_count,
            lambda data, value: data.async_set_pending_income_updated_count(value),
        )


class AutoApproveApprovedCountSensor(ManagerForYnabCountSensor):
    """Sensor that exposes the last auto approve approved count."""

    def __init__(self, runtime_data: RuntimeData, entry_id: str) -> None:
        """Initialize the sensor."""
        super().__init__(
            runtime_data,
            entry_id,
            AUTO_APPROVE_APPROVED_COUNT_DESCRIPTION,
            lambda data: data.auto_approve_approved_count,
            lambda data, value: data.async_set_auto_approve_approved_count(value),
        )


class AutoApproveClearedCountSensor(ManagerForYnabCountSensor):
    """Sensor that exposes the last auto approve cleared count."""

    def __init__(self, runtime_data: RuntimeData, entry_id: str) -> None:
        """Initialize the sensor."""
        super().__init__(
            runtime_data,
            entry_id,
            AUTO_APPROVE_CLEARED_COUNT_DESCRIPTION,
            lambda data: data.auto_approve_cleared_count,
            lambda data, value: data.async_set_auto_approve_cleared_count(value),
        )
