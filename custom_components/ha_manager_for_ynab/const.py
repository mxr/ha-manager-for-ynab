"""Constants for the Manager for YNAB integration."""

from __future__ import annotations

import logging
from typing import Final

DOMAIN: Final = "ha_manager_for_ynab"
NAME: Final = "Manager for YNAB"

CONF_DB_PATH: Final = "db_path"
CONF_TOKEN: Final = "token"

ATTR_UPDATED_COUNT: Final = "updated_count"
ATTR_DB_PATH: Final = "db_path"
ATTR_OUTPUT_FORMAT: Final = "output_format"
ATTR_SQL: Final = "sql"

SERVICE_PENDING_INCOME: Final = "pending_income"
SERVICE_SQLITE_EXPORT: Final = "sqlite_export"
SERVICE_SQLITE_QUERY: Final = "sqlite_query"

LOGGER = logging.getLogger(__name__)
