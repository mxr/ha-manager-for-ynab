CREATE TABLE plans (id TEXT PRIMARY KEY, name TEXT);
CREATE TABLE accounts (
    id TEXT PRIMARY KEY
    , plan_id TEXT
    , name TEXT
    , type TEXT
    , deleted BOOLEAN
    , closed BOOLEAN
)
;
CREATE TABLE categories (
    id TEXT PRIMARY KEY
    , plan_id TEXT
    , category_group_name TEXT
    , name TEXT
    , deleted BOOLEAN
    , hidden BOOLEAN
)
;
CREATE TABLE payees (
    id TEXT PRIMARY KEY
    , plan_id TEXT
    , name TEXT
    , transfer_account_id TEXT
    , deleted BOOLEAN
)
;

INSERT INTO plans VALUES ('single-plan', 'Single Budget');
INSERT INTO accounts VALUES (
    'single-account', 'single-plan', 'Checking', 'checking', 0, 0
)
;
INSERT INTO payees VALUES ('single-payee', 'single-plan', 'Power Co', NULL, 0);
