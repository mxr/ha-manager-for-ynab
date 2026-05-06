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

INSERT INTO plans VALUES ('11111111-1111-1111-1111-111111111111', 'Budget');
INSERT INTO accounts VALUES (
    '22222222-2222-2222-2222-222222222222'
    , '11111111-1111-1111-1111-111111111111'
    , 'Checking'
    , 'checking'
    , 0
    , 0
)
;
INSERT INTO payees VALUES (
    '33333333-3333-3333-3333-333333333333'
    , '11111111-1111-1111-1111-111111111111'
    , 'Power Co'
    , NULL
    , 0
)
;
INSERT INTO categories VALUES (
    '44444444-4444-4444-4444-444444444444'
    , '11111111-1111-1111-1111-111111111111'
    , 'Bills'
    , 'Electric'
    , 0
    , 0
)
;

INSERT INTO plans VALUES ('plan-a', 'Budget A');
INSERT INTO plans VALUES ('plan-b', 'Budget B');
INSERT INTO accounts VALUES (
    'account-a', 'plan-a', 'Checking A', 'checking', 0, 0
)
;
INSERT INTO accounts VALUES (
    'account-b', 'plan-b', 'Checking B', 'checking', 0, 0
)
;
INSERT INTO categories VALUES (
    'category-a', 'plan-a', 'Bills', 'Electric', 0, 0
)
;
INSERT INTO categories VALUES (
    'category-b', 'plan-b', 'Food', 'Groceries', 0, 0
)
;
INSERT INTO payees VALUES ('payee-a', 'plan-a', 'Power Co', NULL, 0);
INSERT INTO payees VALUES ('payee-b', 'plan-b', 'Market Co', NULL, 0);

INSERT INTO plans VALUES ('transfer-plan', 'Transfer Budget');
INSERT INTO accounts VALUES (
    'transfer-checking', 'transfer-plan', 'Checking', 'checking', 0, 0
)
;
INSERT INTO accounts VALUES (
    'transfer-savings', 'transfer-plan', 'Savings', 'checking', 0, 0
)
;
INSERT INTO payees VALUES (
    'transfer-payee', 'transfer-plan', 'Transfer', 'transfer-savings', 0
)
;

INSERT INTO plans VALUES ('empty-plan', 'Empty Budget');
INSERT INTO payees VALUES ('empty-payee', 'empty-plan', 'Power Co', NULL, 0);

INSERT INTO plans VALUES ('schema-plan', 'My Budget');
INSERT INTO accounts VALUES (
    'schema-account', 'schema-plan', 'My Account', 'checking', 0, 0
)
;
INSERT INTO categories VALUES (
    'schema-category', 'schema-plan', 'My Category Group', 'My Category', 0, 0
)
;
INSERT INTO accounts VALUES (
    'schema-duplicate-account', 'schema-plan', 'Checking B', 'checking', 0, 0
)
;
INSERT INTO categories VALUES (
    'schema-duplicate-category', 'schema-plan', 'Food', 'Groceries', 0, 0
)
;
INSERT INTO categories VALUES (
    'schema-credit-card-category'
    , 'schema-plan'
    , 'Credit Card Payments'
    , 'Visa'
    , 0
    , 0
)
;
INSERT INTO payees VALUES ('schema-payee', 'schema-plan', 'My Payee', NULL, 0);
INSERT INTO payees VALUES (
    'schema-duplicate-payee', 'schema-plan', 'Market Co', NULL, 0
)
;
