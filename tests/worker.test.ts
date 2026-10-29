import { afterAll, afterEach, beforeEach, describe, expect, it } from "vitest";
import { enqueue } from "../src/queue.js";
import { newJob, pool, resetDb, row, startWorker, waitFor, type WorkerProcess } from "./helpers.js";

// Fast settings so each test finishes in seconds.
const FAST = {
  LEASE_SECONDS: "3",
  HEARTBEAT_SECONDS: "1",
  REAPER_INTERVAL_SECONDS: "1",
  POLL_INTERVAL_MS: "100",
  BACKOFF_BASE_SECONDS: "0",
  BACKOFF_JITTER_SECONDS: "0",
  SHUTDOWN_GRACE_SECONDS: "5",
  JOB_TIMEOUT_SECONDS: "30",
};

const children: WorkerProcess[] = [];

function worker(env: Record<string, string> = {}): WorkerProcess {
  const child = startWorker({ ...FAST, ...env });
  children.push(child);
  return child;
}

beforeEach(resetDb);

// No graceful stop exists yet, so tests clean up with SIGKILL.
afterEach(() => {
  for (const child of children.splice(0)) child.kill("SIGKILL");
});

afterAll(() => pool.end());

describe("worker", () => {
  it("W01 two workers run 40 echo jobs to success", async () => {
    for (let i = 0; i < 40; i++) await enqueue(pool, newJob());
    worker();
    worker();

    const done = await waitFor(async () => {
      const r = await pool.query("SELECT count(*)::int AS n FROM jobs WHERE status = 'succeeded'");
      return r.rows[0].n === 40;
    }, 20_000);
    expect(done).toBe(true);
  });

  it("W02 an always_fail job is retried, then dead after maxAttempts with its error kept", async () => {
    const { job } = await enqueue(pool, newJob({ type: "always_fail", maxAttempts: 3 }));
    worker();

    expect(await waitFor(async () => (await row(job.id)).status === "dead", 15_000)).toBe(true);
    const final = await row(job.id);
    expect(final.attempts).toBe(3);
    expect(final.last_error).toBeTruthy();
    expect(final.finished_at).not.toBeNull();
  });

  it("W03 a permanent_fail job is dead after exactly one attempt", async () => {
    const { job } = await enqueue(pool, newJob({ type: "permanent_fail" }));
    worker();

    expect(await waitFor(async () => (await row(job.id)).status === "dead", 15_000)).toBe(true);
    expect((await row(job.id)).attempts).toBe(1);
  });

  it("W04 a type without a handler goes straight to dead with a clear message", async () => {
    const { job } = await enqueue(pool, newJob({ type: "no_such_type" }));
    worker();

    expect(await waitFor(async () => (await row(job.id)).status === "dead", 15_000)).toBe(true);
    const final = await row(job.id);
    expect(final.attempts).toBe(1);
    expect(final.last_error).toBe('no handler registered for type "no_such_type"');
  });

  it("W08 a handler that outlives JOB_TIMEOUT_SECONDS fails with 'job exceeded' and is retried", async () => {
    const { job } = await enqueue(
      pool,
      newJob({ type: "sleep", payload: { ms: 60_000 }, maxAttempts: 3 }),
    );
    worker({ JOB_TIMEOUT_SECONDS: "1" });

    const retried = await waitFor(async () => {
      const r = await row(job.id);
      return r.attempts >= 2 && String(r.last_error).startsWith("job exceeded");
    }, 15_000);
    expect(retried).toBe(true);
    expect((await row(job.id)).last_error).toBe("job exceeded 1s timeout");
  });
});
