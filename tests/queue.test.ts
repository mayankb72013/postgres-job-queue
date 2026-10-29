import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { migrate } from "../src/db.js";
import {
  IdempotencyConflict,
  claim,
  complete,
  enqueue,
  fail,
  getJob,
  heartbeat,
  reap,
  release,
  requeueDead,
} from "../src/queue.js";
import { newJob, pool, resetDb, row } from "./helpers.js";

beforeEach(resetDb);
afterAll(() => pool.end());

async function insertQueued(): Promise<string> {
  const result = await pool.query("INSERT INTO jobs (type) VALUES ('echo') RETURNING id");
  return result.rows[0].id;
}

// 23514 is check_violation; `constraint` names the rule that rejected the change.
function rejectedBy(sql: string, params: unknown[], constraint: string) {
  return expect(pool.query(sql, params)).rejects.toMatchObject({ code: "23514", constraint });
}

describe("schema", () => {
  it("T01 migrate twice does not fail", async () => {
    await migrate(pool);
    await migrate(pool);
  });

  it("T18 a running job needs an owner, and a queued job cannot have one", async () => {
    const id = await insertQueued();
    await rejectedBy("UPDATE jobs SET status = 'running' WHERE id = $1", [id], "running_has_owner");
    await rejectedBy("UPDATE jobs SET locked_by = 'w' WHERE id = $1", [id], "running_has_owner");
  });

  it("T19 finished_at is set exactly when the job is succeeded or dead", async () => {
    const id = await insertQueued();
    await rejectedBy("UPDATE jobs SET status = 'dead' WHERE id = $1", [id], "finished_iff_final");
    await rejectedBy("UPDATE jobs SET finished_at = now() WHERE id = $1", [id], "finished_iff_final");
  });

  it("T20 attempts stay within 0..max_attempts and status is one of four values", async () => {
    const id = await insertQueued();
    await rejectedBy(
      "UPDATE jobs SET max_attempts = 3, attempts = 4 WHERE id = $1",
      [id],
      "attempts_in_range",
    );
    await rejectedBy("UPDATE jobs SET attempts = -1 WHERE id = $1", [id], "attempts_in_range");
    await rejectedBy("UPDATE jobs SET status = 'paused' WHERE id = $1", [id], "jobs_status_check");
  });
});

async function countRows(): Promise<number> {
  const result = await pool.query("SELECT count(*)::int AS n FROM jobs");
  return result.rows[0].n;
}

describe("enqueue", () => {
  it("T02 enqueue with defaults creates a queued job", async () => {
    const { job, created } = await enqueue(pool, newJob());
    expect(created).toBe(true);
    expect(job).toMatchObject({ status: "queued", attempts: 0, maxAttempts: 5 });
    expect(await getJob(pool, job.id)).toEqual(job);
    expect(await getJob(pool, "999999")).toBeNull();
  });

  it("T03 same key and same request returns the existing job, whatever the key order", async () => {
    const first = await enqueue(pool, newJob({ idempotencyKey: "k", payload: { a: 1, b: 2 } }));
    const second = await enqueue(pool, newJob({ idempotencyKey: "k", payload: { b: 2, a: 1 } }));
    expect(first.created).toBe(true);
    expect(second.created).toBe(false);
    expect(second.job.id).toBe(first.job.id);
  });

  it("T03b priority is not part of the request identity, array order is", async () => {
    const payload = { list: [1, 2] };
    const first = await enqueue(pool, newJob({ idempotencyKey: "k", payload, priority: 0 }));
    const second = await enqueue(pool, newJob({ idempotencyKey: "k", payload, priority: 9 }));
    expect(second.created).toBe(false);
    expect(second.job.id).toBe(first.job.id);
    expect(second.job.priority).toBe(0);

    await expect(
      enqueue(pool, newJob({ idempotencyKey: "k", payload: { list: [2, 1] } })),
    ).rejects.toBeInstanceOf(IdempotencyConflict);
  });

  it("T04 same key with a different payload or type is a conflict", async () => {
    const first = await enqueue(pool, newJob({ idempotencyKey: "k", payload: { a: 1 } }));
    const byPayload = enqueue(pool, newJob({ idempotencyKey: "k", payload: { a: 2 } }));
    await expect(byPayload).rejects.toBeInstanceOf(IdempotencyConflict);
    await expect(byPayload).rejects.toMatchObject({ existingJobId: first.job.id });
    await expect(
      enqueue(pool, newJob({ idempotencyKey: "k", payload: { a: 1 }, type: "sleep" })),
    ).rejects.toBeInstanceOf(IdempotencyConflict);
  });

  it("T05 50 parallel enqueues with one key create exactly one job", async () => {
    const results = await Promise.all(
      Array.from({ length: 50 }, () => enqueue(pool, newJob({ idempotencyKey: "same" }))),
    );
    expect(new Set(results.map((r) => r.job.id)).size).toBe(1);
    expect(results.filter((r) => r.created)).toHaveLength(1);
    expect(await countRows()).toBe(1);
  });

  it("T05b enqueues without a key never collide", async () => {
    const a = await enqueue(pool, newJob());
    const b = await enqueue(pool, newJob());
    expect(a.job.id).not.toBe(b.job.id);
    expect(await countRows()).toBe(2);
    expect((await row(a.job.id)).idempotency_key).toBeNull();
  });
});

const LEASE_SECONDS = 30;

describe("claim", () => {
  it("T06 highest priority first, then oldest; an empty queue returns nothing", async () => {
    const low = await enqueue(pool, newJob({ priority: 0, runAt: "2020-01-01T00:00:00Z" }));
    const newest = await enqueue(pool, newJob({ priority: 5, runAt: "2020-01-03T00:00:00Z" }));
    const older = await enqueue(pool, newJob({ priority: 5, runAt: "2020-01-02T00:00:00Z" }));

    const order: (string | undefined)[] = [];
    for (let i = 0; i < 4; i++) order.push((await claim(pool, "w", LEASE_SECONDS))?.id);

    expect(order).toEqual([older.job.id, newest.job.id, low.job.id, undefined]);
  });

  it("T07 a job with a future runAt is not claimable until it is due", async () => {
    const { job } = await enqueue(pool, newJob());
    // The database clock decides both sides of the comparison, so the test sets run_at in SQL.
    await pool.query("UPDATE jobs SET run_at = now() + interval '1.5 seconds' WHERE id = $1", [
      job.id,
    ]);

    expect(await claim(pool, "w", LEASE_SECONDS)).toBeNull();
    await new Promise((resolve) => setTimeout(resolve, 1_700));
    expect((await claim(pool, "w", LEASE_SECONDS))?.id).toBe(job.id);
  });

  it("T08 100 jobs and 8 concurrent claimers: each job is claimed exactly once", async () => {
    for (let i = 0; i < 100; i++) await enqueue(pool, newJob());

    const claimer = async (name: string): Promise<string[]> => {
      const ids: string[] = [];
      while (true) {
        const job = await claim(pool, name, LEASE_SECONDS);
        if (!job) return ids;
        ids.push(job.id);
      }
    };
    const results = await Promise.all(Array.from({ length: 8 }, (_, i) => claimer(`w${i}`)));
    const all = results.flat();

    expect(all).toHaveLength(100);
    expect(new Set(all).size).toBe(100);
    const wrong = await pool.query("SELECT count(*)::int AS n FROM jobs WHERE attempts <> 1");
    expect(wrong.rows[0].n).toBe(0);
  });

  it("T08b a claimed job is running, owned, counted and leased", async () => {
    await enqueue(pool, newJob());
    const job = await claim(pool, "worker-a", LEASE_SECONDS);

    expect(job).toMatchObject({ status: "running", lockedBy: "worker-a", attempts: 1 });
    const result = await pool.query<{ lease_ahead: number }>(
      "SELECT extract(epoch FROM lease_expires_at - now())::float AS lease_ahead FROM jobs WHERE id = $1",
      [job?.id],
    );
    expect(result.rows[0]?.lease_ahead).toBeGreaterThan(20);
  });

  it("T08c a claim skips a row another transaction has locked instead of waiting", async () => {
    const best = await enqueue(pool, newJob({ priority: 5 }));
    const next = await enqueue(pool, newJob({ priority: 0 }));

    const holder = await pool.connect();
    try {
      await holder.query("BEGIN");
      await holder.query("SELECT id FROM jobs WHERE id = $1 FOR UPDATE", [best.job.id]);

      let timer: NodeJS.Timeout | undefined;
      const timeout = new Promise<"waited">((resolve) => {
        timer = setTimeout(() => resolve("waited"), 1_500);
      });
      const outcome = await Promise.race([claim(pool, "w", LEASE_SECONDS), timeout]);
      clearTimeout(timer);

      expect(outcome).not.toBe("waited");
      expect(outcome).toMatchObject({ id: next.job.id });
    } finally {
      await holder.query("ROLLBACK");
      holder.release();
    }
  });
});

const BACKOFF = { baseSeconds: 5, maxSeconds: 300, jitterSeconds: 5 };
const RETRYABLE = { message: "boom", retryable: true };

async function claimOne(worker = "w"): Promise<string> {
  const job = await claim(pool, worker, LEASE_SECONDS);
  if (!job) throw new Error("expected a claimable job");
  return job.id;
}

async function secondsUntilRunAt(id: string): Promise<number> {
  const result = await pool.query<{ delay: number }>(
    "SELECT extract(epoch FROM run_at - now())::float AS delay FROM jobs WHERE id = $1",
    [id],
  );
  return result.rows[0]?.delay ?? Number.NaN;
}

describe("complete, fail, release, requeue", () => {
  it("T09 only the owner can complete, and only once", async () => {
    const { job } = await enqueue(pool, newJob());
    await claim(pool, "A", LEASE_SECONDS);

    expect(await complete(pool, job.id, "B")).toBe(false);
    expect(await complete(pool, job.id, "A")).toBe(true);
    expect(await complete(pool, job.id, "A")).toBe(false);
    expect(await row(job.id)).toMatchObject({
      status: "succeeded",
      locked_by: null,
      lease_expires_at: null,
    });
  });

  it("T10 a retryable failure waits base x 2^attempts plus jitter, capped", async () => {
    const { job } = await enqueue(pool, newJob());

    for (const [attempt, expected] of [[1, 10], [2, 20], [3, 40], [4, 80]] as const) {
      await claimOne();
      expect(await fail(pool, job.id, "w", RETRYABLE, BACKOFF)).toBe("queued");

      const stored = await row(job.id);
      expect(stored).toMatchObject({ status: "queued", attempts: attempt, locked_by: null });
      expect(stored.finished_at).toBeNull();
      const delay = await secondsUntilRunAt(job.id);
      expect(delay).toBeGreaterThan(expected - 2);
      expect(delay).toBeLessThanOrEqual(expected + 5);
      await pool.query("UPDATE jobs SET run_at = now() WHERE id = $1", [job.id]);
    }

    // Attempt 12 would wait 5 x 2^12 = 20,480 s without the cap.
    await pool.query("UPDATE jobs SET run_at = now() + interval '1 hour' WHERE id = $1", [job.id]);
    const late = await enqueue(pool, newJob({ maxAttempts: 20 }));
    await pool.query("UPDATE jobs SET attempts = 11 WHERE id = $1", [late.job.id]);
    expect(await claimOne()).toBe(late.job.id);
    expect(await fail(pool, late.job.id, "w", RETRYABLE, BACKOFF)).toBe("queued");
    const capped = await secondsUntilRunAt(late.job.id);
    expect(capped).toBeGreaterThan(298);
    expect(capped).toBeLessThan(306);
  });

  it("T11 failing the last attempt makes the job dead", async () => {
    const { job } = await enqueue(pool, newJob({ maxAttempts: 1 }));
    await claim(pool, "w", LEASE_SECONDS);

    expect(await fail(pool, job.id, "w", RETRYABLE, BACKOFF)).toBe("dead");
    const stored = await row(job.id);
    expect(stored).toMatchObject({ status: "dead", last_error: "boom", locked_by: null });
    expect(stored.finished_at).not.toBeNull();
  });

  it("T12 a permanent failure is dead at once", async () => {
    const { job } = await enqueue(pool, newJob());
    await claim(pool, "w", LEASE_SECONDS);

    const outcome = await fail(pool, job.id, "w", { message: "bad", retryable: false }, BACKOFF);
    expect(outcome).toBe("dead");
    expect(await row(job.id)).toMatchObject({ status: "dead", attempts: 1, last_error: "bad" });
  });

  it("T12b a long error message is cut to 2,000 characters", async () => {
    const { job } = await enqueue(pool, newJob());
    await claim(pool, "w", LEASE_SECONDS);

    await fail(pool, job.id, "w", { message: "x".repeat(5_000), retryable: true }, BACKOFF);
    expect((await row(job.id)).last_error).toHaveLength(2_000);
  });

  it("fail by a non-owner changes nothing", async () => {
    const { job } = await enqueue(pool, newJob());
    await claim(pool, "A", LEASE_SECONDS);

    expect(await fail(pool, job.id, "B", RETRYABLE, BACKOFF)).toBeNull();
    expect(await row(job.id)).toMatchObject({ status: "running", locked_by: "A" });
  });

  it("T17 requeueDead only revives dead jobs", async () => {
    const { job } = await enqueue(pool, newJob());
    await claim(pool, "w", LEASE_SECONDS);
    expect(await requeueDead(pool, job.id)).toBe("not_dead");

    await fail(pool, job.id, "w", { message: "bad", retryable: false }, BACKOFF);
    expect(await requeueDead(pool, job.id)).toBe("ok");
    const stored = await row(job.id);
    expect(stored).toMatchObject({ status: "queued", attempts: 0, last_error: null });
    expect(stored.finished_at).toBeNull();

    expect(await requeueDead(pool, "999999")).toBe("not_found");
  });

  it("T17b release puts the job back and gives the attempt back", async () => {
    const { job } = await enqueue(pool, newJob());
    await claim(pool, "w", LEASE_SECONDS);

    expect(await release(pool, job.id, "other", "released on worker shutdown")).toBe(false);
    expect(await release(pool, job.id, "w", "released on worker shutdown")).toBe(true);
    expect(await row(job.id)).toMatchObject({
      status: "queued",
      attempts: 0,
      locked_by: null,
      lease_expires_at: null,
      last_error: "released on worker shutdown",
    });
    expect(await claim(pool, "w2", LEASE_SECONDS)).toMatchObject({ id: job.id, attempts: 1 });
  });
});

// "Forced" means the test edits the row in SQL to simulate the passage of time.
async function expireLease(id: string): Promise<void> {
  await pool.query("UPDATE jobs SET lease_expires_at = now() - interval '1 minute' WHERE id = $1", [
    id,
  ]);
}

describe("heartbeat and reap", () => {
  it("T13 reap requeues expired jobs, kills exhausted ones and leaves live leases alone", async () => {
    const retry = await enqueue(pool, newJob());
    const exhausted = await enqueue(pool, newJob({ maxAttempts: 1 }));
    const live = await enqueue(pool, newJob());
    for (let i = 0; i < 3; i++) await claimOne();
    await expireLease(retry.job.id);
    await expireLease(exhausted.job.id);

    const handled = await reap(pool, 100);

    expect(handled).toHaveLength(2);
    expect(handled).toContainEqual({ id: retry.job.id, status: "queued" });
    expect(handled).toContainEqual({ id: exhausted.job.id, status: "dead" });
    expect(await row(retry.job.id)).toMatchObject({
      status: "queued",
      attempts: 1,
      locked_by: null,
      lease_expires_at: null,
      last_error: "lease expired (worker presumed dead)",
    });
    expect((await row(retry.job.id)).finished_at).toBeNull();
    const dead = await row(exhausted.job.id);
    expect(dead).toMatchObject({ status: "dead", locked_by: null });
    expect(dead.finished_at).not.toBeNull();
    expect(await row(live.job.id)).toMatchObject({ status: "running", locked_by: "w" });
  });

  it("T14 300 expired jobs and 10 concurrent reapers: each job is handled exactly once", async () => {
    await pool.query(
      `INSERT INTO jobs (type, status, attempts, locked_by, lease_expires_at)
       SELECT 'echo', 'running', 1, 'gone', now() - interval '1 minute'
       FROM generate_series(1, 300)`,
    );

    const reaper = async (): Promise<string[]> => {
      const ids: string[] = [];
      while (true) {
        const handled = await reap(pool, 50);
        if (handled.length === 0) return ids;
        ids.push(...handled.map((h) => h.id));
      }
    };
    const results = await Promise.all(Array.from({ length: 10 }, reaper));
    const all = results.flat();

    expect(all).toHaveLength(300);
    expect(new Set(all).size).toBe(300);
    const left = await pool.query("SELECT count(*)::int AS n FROM jobs WHERE status = 'running'");
    expect(left.rows[0].n).toBe(0);
  });

  it("T15 only the owner can heartbeat, and it moves the lease forward", async () => {
    const { job } = await enqueue(pool, newJob());
    await claim(pool, "A", LEASE_SECONDS);

    expect(await heartbeat(pool, job.id, "B", 100)).toBe(false);
    expect(await heartbeat(pool, job.id, "A", 100)).toBe(true);
    const result = await pool.query<{ lease_ahead: number }>(
      "SELECT extract(epoch FROM lease_expires_at - now())::float AS lease_ahead FROM jobs WHERE id = $1",
      [job.id],
    );
    expect(result.rows[0]?.lease_ahead).toBeGreaterThan(90);
  });

  it("T16 a worker that lost its lease cannot complete or heartbeat the new owner's job", async () => {
    const { job } = await enqueue(pool, newJob());
    await claim(pool, "A", LEASE_SECONDS);
    await expireLease(job.id);
    expect(await reap(pool, 100)).toEqual([{ id: job.id, status: "queued" }]);
    expect(await claim(pool, "B", LEASE_SECONDS)).toMatchObject({ id: job.id, attempts: 2 });

    expect(await complete(pool, job.id, "A")).toBe(false);
    expect(await heartbeat(pool, job.id, "A", LEASE_SECONDS)).toBe(false);
    expect(await row(job.id)).toMatchObject({ status: "running", locked_by: "B" });
    expect(await complete(pool, job.id, "B")).toBe(true);
  });

  it("T16b a late heartbeat or completion still works until the reaper takes the job", async () => {
    const { job } = await enqueue(pool, newJob());
    await claim(pool, "A", LEASE_SECONDS);
    await expireLease(job.id);

    expect(await heartbeat(pool, job.id, "A", LEASE_SECONDS)).toBe(true);
    await expireLease(job.id);
    expect(await complete(pool, job.id, "A")).toBe(true);
    expect((await row(job.id)).status).toBe("succeeded");
  });
});
