-- Destination schema: the replica table, the value-free receipts ledger
-- (one row per delivered change; columns are exactly commit_lsn/schema/
-- table/op — Critical Rule 1: there is deliberately NO value-bearing column),
-- and the single-row checkpoint.

CREATE TABLE orders (
  id         int          NOT NULL PRIMARY KEY,
  note       text         NOT NULL,
  payload    text         NOT NULL DEFAULT '',
  updated_at timestamptz  NOT NULL DEFAULT now()
);

CREATE TABLE cdc_receipts (
  commit_lsn   bigint      NOT NULL,
  schema_name  text        NOT NULL,
  table_name   text        NOT NULL,
  op           text        NOT NULL,
  delivered_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE pipeline_checkpoint (
  id                int     NOT NULL PRIMARY KEY CHECK (id = 1),
  slot_name         text    NOT NULL,
  system_identifier text,
  database          text,
  -- NULL = the session identity is bound but nothing has been delivered yet.
  commit_lsn        bigint,
  updated_at        timestamptz NOT NULL DEFAULT now()
);
