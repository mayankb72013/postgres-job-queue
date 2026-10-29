import pg from "pg";
import { makeLogger } from "./logger.js";

const log = makeLogger();

// `query_timeout` is client-side: a silent database ends in an error instead of a hang.
export function makePool(
  url: string,
  max: number,
  queryTimeoutMs: number,
  connectionTimeoutMs = 5_000,
): pg.Pool {
  const pool = new pg.Pool({
    connectionString: url,
    max,
    query_timeout: queryTimeoutMs,
    connectionTimeoutMillis: connectionTimeoutMs,
    keepAlive: true,
  });
  // Without a listener, a dropped idle connection is an unhandled 'error' and kills the process.
  pool.on("error", (err) => {
    log.warn("pool.idle_client_error", { error: err.message });
  });
  return pool;
}

const SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS jobs (
  id               BIGSERIAL PRIMARY KEY,
  type             TEXT        NOT NULL,
  payload          JSONB       NOT NULL DEFAULT '{}',
  status           TEXT        NOT NULL DEFAULT 'queued'
                   CHECK (status IN ('queued','running','succeeded','dead')),
  priority         INT         NOT NULL DEFAULT 0,
  run_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  attempts         INT         NOT NULL DEFAULT 0,
  max_attempts     INT         NOT NULL DEFAULT 5 CHECK (max_attempts BETWEEN 1 AND 20),
  last_error       TEXT,
  locked_by        TEXT,
  lease_expires_at TIMESTAMPTZ,
  idempotency_key  TEXT UNIQUE,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at      TIMESTAMPTZ,
  CONSTRAINT running_has_owner  CHECK (
       (status = 'running'  AND locked_by IS NOT NULL AND lease_expires_at IS NOT NULL)
    OR (status <> 'running' AND locked_by IS NULL     AND lease_expires_at IS NULL)),
  CONSTRAINT finished_iff_final CHECK ((status IN ('succeeded','dead')) = (finished_at IS NOT NULL)),
  CONSTRAINT attempts_in_range  CHECK (attempts >= 0 AND attempts <= max_attempts)
);

CREATE INDEX IF NOT EXISTS idx_jobs_ready ON jobs (priority DESC, run_at) WHERE status = 'queued';
CREATE INDEX IF NOT EXISTS idx_jobs_lease ON jobs (lease_expires_at)      WHERE status = 'running';
`;

// Idempotent: safe to run on every start.
export async function migrate(pool: pg.Pool): Promise<void> {
  await pool.query(SCHEMA_SQL);
}
