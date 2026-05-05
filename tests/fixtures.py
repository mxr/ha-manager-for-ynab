from __future__ import annotations

from types import MappingProxyType
from typing import TYPE_CHECKING

import pytest
from homeassistant.config_entries import SOURCE_USER, ConfigEntry

from custom_components.ha_manager_for_ynab.const import DOMAIN

if TYPE_CHECKING:
    from collections.abc import Callable

    from custom_components.ha_manager_for_ynab import RuntimeData


@pytest.fixture
def config_entry_factory() -> Callable[..., ConfigEntry[RuntimeData]]:
    def factory(
        *,
        entry_id: str = "entry-1",
        data: dict[str, object] | None = None,
        runtime_data: RuntimeData | None = None,
    ) -> ConfigEntry[RuntimeData]:
        entry = ConfigEntry(
            data=data or {},
            discovery_keys=MappingProxyType({}),
            domain=DOMAIN,
            entry_id=entry_id,
            minor_version=1,
            options={},
            source=SOURCE_USER,
            subentries_data=None,
            title="Manager for YNAB",
            unique_id=None,
            version=1,
        )
        if runtime_data is not None:
            entry.runtime_data = runtime_data
        return entry

    return factory
