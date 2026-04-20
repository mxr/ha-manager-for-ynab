"""Config flow for Manager for YNAB."""

from __future__ import annotations

from typing import Any

import voluptuous as vol

from homeassistant.config_entries import ConfigFlow
from homeassistant.config_entries import ConfigFlowResult

from . import _api
from .const import CONF_DB_PATH
from .const import CONF_TOKEN
from .const import DOMAIN
from .const import NAME


class ManagerForYnabConfigFlow(ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Manager for YNAB."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Handle the initial step."""
        if user_input is not None:
            await self.async_set_unique_id(DOMAIN)
            self._abort_if_unique_id_configured()
            return self.async_create_entry(title=NAME, data=user_input)

        return self.async_show_form(step_id="user", data_schema=_user_schema())


def _user_schema() -> vol.Schema:
    """Build the user step schema."""
    return vol.Schema(
        {
            vol.Required(CONF_TOKEN): str,
            vol.Optional(CONF_DB_PATH, default=str(_api.default_db_path())): vol.All(
                str, vol.Length(min=1)
            ),
        }
    )
