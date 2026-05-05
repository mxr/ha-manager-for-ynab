"""Constants for the Manager for YNAB integration."""

from __future__ import annotations

import logging
from typing import Final

from asyncio_for_ynab import TransactionClearedStatus

DOMAIN: Final = "ha_manager_for_ynab"
NAME: Final = "Manager for YNAB"

CONF_DB_PATH: Final = "db_path"
CONF_TOKEN: Final = "token"

ATTR_UPDATED_COUNT: Final = "updated_count"
ATTR_DB_PATH: Final = "db_path"
ATTR_SQL: Final = "sql"
ATTR_PLAN_NAME: Final = "plan_name"
ATTR_ACCOUNT_NAME: Final = "account_name"
ATTR_PAYEE_NAME: Final = "payee_name"
ATTR_CATEGORY_NAME: Final = "category_name"
ATTR_DATE: Final = "date"
ATTR_CLEARED: Final = "cleared"
ATTR_AMOUNT: Final = "amount"

CLEARED_DEFAULT: Final = TransactionClearedStatus.UNCLEARED.name.lower()
CLEARED_OPTIONS: Final = [status.name.lower() for status in TransactionClearedStatus]

SERVICE_PENDING_INCOME: Final = "pending_income"
SERVICE_AUTO_APPROVE: Final = "auto_approve"
SERVICE_SQLITE_EXPORT: Final = "sqlite_export"
SERVICE_SQLITE_QUERY: Final = "sqlite_query"
SERVICE_ADD_TRANSACTION: Final = "add_transaction"

LOGGER = logging.getLogger(__name__)
