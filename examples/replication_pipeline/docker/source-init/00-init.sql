-- Source-server init: the watched table, the replication account, and the
-- publication. Replicant creates the slot itself; the PUBLICATION is the
-- operator's SQL (a pipeline naming a missing publication fails closed).
-- Password is a throwaway fixture for this isolated example. Host connections
-- use the engine's scram-sha-256 authentication.

CREATE TABLE orders (
  id         int          NOT NULL PRIMARY KEY,
  note       text         NOT NULL,
  payload    text         NOT NULL DEFAULT '',
  updated_at timestamptz  NOT NULL DEFAULT now()
);

-- Force `payload` out-of-line (>2KB rows, never compressed) so an UPDATE that
-- does not touch it DETERMINISTICALLY sends the unchanged-TOAST sentinel —
-- the example's Critical-Rule-4 path (see the sink's unchanged handling).
ALTER TABLE orders ALTER COLUMN payload SET STORAGE EXTERNAL;

CREATE ROLE replicant_reader LOGIN REPLICATION PASSWORD 'postgres';
GRANT SELECT ON public.orders TO replicant_reader;

CREATE PUBLICATION example_pub FOR TABLE public.orders;
