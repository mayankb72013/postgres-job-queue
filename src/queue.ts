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
