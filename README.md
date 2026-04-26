# Manager for YNAB

Manager for YNAB is a Home Assistant custom integration for running `manager-for-ynab` and `sqlite-export-for-ynab` from Home Assistant automations.

## Features

- Config flow for a YNAB personal access token
- Optional SQLite DB path configuration
- `auto_approve` action with `for_real` and `quiet`
- `pending_income` action with `for_real` and `quiet`
- `sqlite_export` action with `full_refresh` and `quiet`
- `sqlite_query` action with arbitrary SQL
- Sensor for the latest `pending_income` updated count

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
- `quiet`: default `false`

This runs `manager-for-ynab.auto_approve.auto_approve(...)`.

### `pending_income`

- `for_real`: default `false`
- `quiet`: default `false`

This runs `manager-for-ynab.pending_income.pending_income(...)` and updates the sensor to the returned `updated_count`.

### `sqlite_export`

- `full_refresh`: default `false`
- `quiet`: default `false`

This runs `sqlite-export-for-ynab` against the configured token and DB path.

### `sqlite_query`

- `sql`: required SQL statement

This executes the SQL against the configured SQLite DB path and returns rows as service response data.
