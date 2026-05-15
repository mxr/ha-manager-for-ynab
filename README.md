# Manager for YNAB

Manager for YNAB is a Home Assistant custom integration for running `manager-for-ynab` and `sqlite-export-for-ynab` from Home Assistant automations.

## Features

- Config flow for a YNAB personal access token
- Optional SQLite DB path configuration
- `auto_approve` action with `for_real`, `sync`, and `quiet`
- `pending_income` action with `for_real`, `sync`, and `quiet`
- `sqlite_export` action with `full_refresh` and `quiet`
- `sqlite_query` action with arbitrary SQL and optional sync
- `add_transaction` action with values resolved from the SQLite export
- Sensors for the latest `pending_income` updated count and `auto_approve` approved and cleared counts

If the configured DB path is empty, the integration uses `sqlite-export-for-ynab`'s default database path.

## Install with HACS

1. Open HACS in Home Assistant.
2. Add this repository as a custom repository.
3. Category: `Integration`.
4. Install `Manager for YNAB`.
5. Restart Home Assistant.

## Configuration

Add the integration from Settings -> Devices & Services -> Add Integration -> `Manager for YNAB`.

You need:

- A YNAB personal access token
- An optional SQLite DB path

Leave the DB path empty to use the default path from `sqlite-export-for-ynab`.

## Actions

### `auto_approve`

- `for_real`: default `false`
- `sync`: default `true`
- `quiet`: default `false`

This runs `manager-for-ynab.auto_approve.auto_approve(...)` and updates the sensors to the returned `updated_count` and `cleared` counts.

### `pending_income`

- `for_real`: default `false`
- `sync`: default `true`
- `quiet`: default `false`

This runs `manager-for-ynab.pending_income.pending_income(...)` and updates the sensor to the returned `updated_count`.

### `sqlite_export`

- `full_refresh`: default `false`
- `quiet`: default `false`

This runs `sqlite-export-for-ynab` against the configured token and DB path.

### `sqlite_query`

- `sql`: required SQL statement
- `sync`: default `true`

This executes the SQL against the configured SQLite DB path and returns rows as service response data.

### `add_transaction`

- `plan_name`: optional when the SQLite export has exactly one plan
- `account_name`: required
- `payee_name`: required
- `category_name`: shown by default, formatted as `Category Group - Category Name`; ignored when the payee is another account, which makes the transaction a transfer
- `use_current_date`: default `true`; when enabled, the transaction uses the current date in Home Assistant's local timezone and ignores `date`
- `date`: required, default today; picker value used only when `use_current_date` is `false`
- `cleared`: default `uncleared`
- `amount`: required, positive values are expenses
- `sync`: default `true`
- `quiet`: default `false`

This creates a transaction with `manager-for-ynab`'s add-transaction fund-moving helper. Dropdown values still come from the configured SQLite export and refresh automatically after sync-capable actions.
