import type pg from "pg";

export type JobStatus = "queued" | "running" | "succeeded" | "dead";

// ids are strings: bigint can exceed Number.MAX_SAFE_INTEGER.
export type JobRow = {
  id: string;
  type: string;
  payload: Record<string, unknown>;
  status: JobStatus;
  priority: number;
  runAt: Date;
  attempts: number;
  maxAttempts: number;
  lastError: string | null;
  lockedBy: string | null;
  leaseExpiresAt: Date | null;
  idempotencyKey: string | null;
  createdAt: Date;
  updatedAt: Date;
  finishedAt: Date | null;
};

export type EnqueueInput = {
  type: string;
  payload: Record<string, unknown>;
  priority: number;
  maxAttempts: number;
  runAt?: Date | string;
  idempotencyKey?: string;
};

type DbJob = {
  id: string;
  type: string;
  payload: Record<string, unknown>;
  status: JobStatus;
  priority: number;
  run_at: Date;
  attempts: number;
  max_attempts: number;
  last_error: string | null;
  locked_by: string | null;
  lease_expires_at: Date | null;
  idempotency_key: string | null;
  created_at: Date;
  updated_at: Date;
  finished_at: Date | null;
};

export class IdempotencyConflict extends Error {
  constructor(readonly existingJobId: string) {
    super("idempotency key reused for a different request");
    this.name = "IdempotencyConflict";
  }
}

export function toJob(r: DbJob): JobRow {
  return {
    id: r.id,
    type: r.type,
    payload: r.payload,
    status: r.status,
    priority: r.priority,
    runAt: r.run_at,
    attempts: r.attempts,
    maxAttempts: r.max_attempts,
    lastError: r.last_error,
    lockedBy: r.locked_by,
    leaseExpiresAt: r.lease_expires_at,
    idempotencyKey: r.idempotency_key,
    createdAt: r.created_at,
    updatedAt: r.updated_at,
    finishedAt: r.finished_at,
  };
}

export async function enqueue(
  pool: pg.Pool,
  input: EnqueueInput,
): Promise<{ job: JobRow; created: boolean }> {
  const key = input.idempotencyKey ?? null;
  const payload = JSON.stringify(input.payload);

  const inserted = await pool.query<DbJob>(
    `INSERT INTO jobs (type, payload, priority, run_at, max_attempts, idempotency_key)
     VALUES ($1, $2::jsonb, $3, COALESCE($4::timestamptz, now()), $5, $6)
     ON CONFLICT (idempotency_key) DO NOTHING
     RETURNING *`,
    [input.type, payload, input.priority, input.runAt ?? null, input.maxAttempts, key],
  );
  const created = inserted.rows[0];
  if (created) return { job: toJob(created), created: true };

  // Nothing inserted means the key exists. jsonb equality ignores object key order.
  const existing = await pool.query<DbJob & { same: boolean }>(
    `SELECT *, (type = $1 AND payload = $2::jsonb) AS same
     FROM jobs WHERE idempotency_key = $3`,
    [input.type, payload, key],
  );
  const found = existing.rows[0];
  if (!found) throw new Error("idempotency row vanished");
  if (!found.same) throw new IdempotencyConflict(found.id);
  return { job: toJob(found), created: false };
}

export async function getJob(pool: pg.Pool, id: string): Promise<JobRow | null> {
  const result = await pool.query<DbJob>("SELECT * FROM jobs WHERE id = $1", [id]);
  const found = result.rows[0];
  return found ? toJob(found) : null;
}

// One statement: choosing and marking happen together, so no gap exists between them.
export async function claim(
  pool: pg.Pool,
  workerId: string,
  leaseSeconds: number,
): Promise<JobRow | null> {
  const result = await pool.query<DbJob>(
    `WITH next AS (
       SELECT id FROM jobs
       WHERE status = 'queued' AND run_at <= now()
       ORDER BY priority DESC, run_at
       FOR UPDATE SKIP LOCKED
       LIMIT 1
     )
     UPDATE jobs
     SET status = 'running',
         locked_by = $1,
         lease_expires_at = now() + make_interval(secs => $2),
         attempts = attempts + 1,
         updated_at = now()
     FROM next
     WHERE jobs.id = next.id
     RETURNING jobs.*`,
    [workerId, leaseSeconds],
  );
  const claimed = result.rows[0];
  return claimed ? toJob(claimed) : null;
}

// Ownership in the WHERE clause is the fence: zero rows means "discard your result".
export async function complete(pool: pg.Pool, id: string, workerId: string): Promise<boolean> {
  const result = await pool.query(
    `UPDATE jobs
     SET status = 'succeeded', locked_by = NULL, lease_expires_at = NULL,
         finished_at = now(), updated_at = now()
     WHERE id = $1 AND locked_by = $2 AND status = 'running'
     RETURNING id`,
    [id, workerId],
  );
  return result.rowCount === 1;
}

export type FailureInput = { message: string; retryable: boolean };

export type BackoffSettings = { baseSeconds: number; maxSeconds: number; jitterSeconds: number };

// Returns the new status, or null when this worker no longer owns the job.
// `attempts` is already counted by claim, so the first retry waits base x 2, not base.
export async function fail(
  pool: pg.Pool,
  id: string,
  workerId: string,
  error: FailureInput,
  backoff: BackoffSettings,
): Promise<"queued" | "dead" | null> {
  const result = await pool.query<{ status: "queued" | "dead" }>(
    `UPDATE jobs
     SET status = CASE WHEN NOT $3::boolean OR attempts >= max_attempts THEN 'dead' ELSE 'queued' END,
         run_at = CASE WHEN NOT $3::boolean OR attempts >= max_attempts THEN run_at
                  ELSE now() + (LEAST($5::float8 * power(2, attempts), $6::float8)
                                + random() * $7::float8) * interval '1 second' END,
         last_error = left($4, 2000),
         locked_by = NULL, lease_expires_at = NULL,
         finished_at = CASE WHEN NOT $3::boolean OR attempts >= max_attempts THEN now() ELSE NULL END,
         updated_at = now()
     WHERE id = $1 AND locked_by = $2 AND status = 'running'
     RETURNING status`,
    [
      id,
      workerId,
      error.retryable,
      error.message,
      backoff.baseSeconds,
      backoff.maxSeconds,
      backoff.jitterSeconds,
    ],
  );
  return result.rows[0]?.status ?? null;
}

// Being interrupted by a deploy is not the job's fault, so the attempt is given back.
export async function release(
  pool: pg.Pool,
  id: string,
  workerId: string,
  reason: string,
): Promise<boolean> {
  const result = await pool.query(
    `UPDATE jobs
     SET status = 'queued', locked_by = NULL, lease_expires_at = NULL,
         attempts = GREATEST(attempts - 1, 0), run_at = now(), last_error = $3, updated_at = now()
     WHERE id = $1 AND locked_by = $2 AND status = 'running'
     RETURNING id`,
    [id, workerId, reason],
  );
  return result.rowCount === 1;
}

export async function requeueDead(
  pool: pg.Pool,
  id: string,
): Promise<"ok" | "not_dead" | "not_found"> {
  const result = await pool.query(
    `UPDATE jobs
     SET status = 'queued', attempts = 0, run_at = now(), last_error = NULL,
         finished_at = NULL, updated_at = now()
     WHERE id = $1 AND status = 'dead'
     RETURNING id`,
    [id],
  );
  if (result.rowCount === 1) return "ok";

  const exists = await pool.query("SELECT 1 FROM jobs WHERE id = $1", [id]);
  return exists.rowCount === 1 ? "not_dead" : "not_found";
}
