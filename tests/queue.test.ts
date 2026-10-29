import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { migrate } from "../src/db.js";
import { IdempotencyConflict, enqueue, getJob } from "../src/queue.js";
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
