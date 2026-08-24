-- Destination schema: the replica table and the single-row checkpoint the
-- skeleton's sink writes, plus the value-free receipts table (columns are
-- exactly commit_lsn/schema/table/op — Critical Rule 1: there is deliberately
-- NO value-bearing column). The skeleton's sink does not write receipts.

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
  commit_lsn        bigint  NOT NULL,
  updated_at        timestamptz NOT NULL DEFAULT now()
);
