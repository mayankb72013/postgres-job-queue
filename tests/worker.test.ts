import { afterAll, afterEach, beforeEach, describe, expect, it } from "vitest";
import { claim, complete, enqueue, reap } from "../src/queue.js";
import {
  exited,
  newJob,
  pool,
  resetDb,
  row,
  startWorker,
  stopWorker,
  waitFor,
  type WorkerProcess,
} from "./helpers.js";

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

const sleepMs = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function executionsOf(jobId: string): Promise<string[]> {
  const r = await pool.query("SELECT worker FROM executions WHERE job_id = $1 ORDER BY ts", [jobId]);
  return r.rows.map((x) => x.worker);
}

const hasExited = (child: WorkerProcess) => child.exitCode !== null || child.signalCode !== null;

// SIGKILL is the cleanup of last resort; it also covers workers a test already stopped.
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

  it("W05 a killed worker's job is re-run by another worker after the lease expires", async () => {
    const { job } = await enqueue(pool, newJob({ type: "record", payload: { ms: 3000 } }));
    const a = worker();
    expect(await waitFor(async () => (await executionsOf(job.id)).length === 1, 15_000)).toBe(true);

    const killedAt = performance.now();
    a.kill("SIGKILL");
    await exited(a);
    worker();

    expect(await waitFor(async () => (await executionsOf(job.id)).length === 2, 20_000)).toBe(true);
    console.log(`W05 recovery time: ${((performance.now() - killedAt) / 1000).toFixed(1)} s`);
    expect(await waitFor(async () => (await row(job.id)).status === "succeeded", 15_000)).toBe(true);

    const runners = await executionsOf(job.id);
    expect(runners).toHaveLength(2);
    expect(new Set(runners).size).toBe(2);
    expect((await row(job.id)).attempts).toBe(2);
  });

  it("W06 stop during a job lets it finish, then exits 0", async () => {
    const { job } = await enqueue(pool, newJob({ type: "sleep", payload: { ms: 1500 } }));
    const a = worker();
    expect(await waitFor(async () => (await row(job.id)).status === "running", 15_000)).toBe(true);

    stopWorker(a);
    expect(await waitFor(() => hasExited(a), 10_000)).toBe(true);
    expect(a.exitCode).toBe(0);
    const final = await row(job.id);
    expect(final.status).toBe("succeeded");
    expect(final.attempts).toBe(1);
  });

  it.skipIf(process.platform === "win32")("W06s a real SIGTERM during a job behaves the same", async () => {
    const { job } = await enqueue(pool, newJob({ type: "sleep", payload: { ms: 1500 } }));
    const a = worker();
    expect(await waitFor(async () => (await row(job.id)).status === "running", 15_000)).toBe(true);

    a.kill("SIGTERM");
    expect(await waitFor(() => hasExited(a), 10_000)).toBe(true);
    expect(a.exitCode).toBe(0);
    const final = await row(job.id);
    expect(final.status).toBe("succeeded");
    expect(final.attempts).toBe(1);
  });

  it("W07 when the grace period ends, the job is released and the worker exits 0", async () => {
    const { job } = await enqueue(pool, newJob({ type: "sleep", payload: { ms: 60_000 } }));
    const a = worker({ SHUTDOWN_GRACE_SECONDS: "1" });
    expect(await waitFor(async () => (await row(job.id)).status === "running", 15_000)).toBe(true);

    stopWorker(a);
    expect(await waitFor(() => hasExited(a), 4_000)).toBe(true);
    expect(a.exitCode).toBe(0);
    const final = await row(job.id);
    expect(final.status).toBe("queued");
    expect(final.attempts).toBe(0);
    expect(final.locked_by).toBeNull();
    expect(final.last_error).toBe("released on worker shutdown");
  });

  it("W07b stop while idle exits 0 within 2 s", async () => {
    const a = worker({ LOG_LEVEL: "info" });
    expect(await waitFor(() => a.out.includes("worker.started"), 15_000)).toBe(true);

    stopWorker(a);
    expect(await waitFor(() => hasExited(a), 2_000)).toBe(true);
    expect(a.exitCode).toBe(0);
  });

  it("W09 a blocked event loop loses the lease; the stale result is dropped and the new owner keeps the job", async () => {
    const { job } = await enqueue(pool, newJob({ type: "block", payload: { ms: 6000 } }));
    const a = worker();
    expect(await waitFor(async () => (await row(job.id)).status === "running", 15_000)).toBe(true);

    await sleepMs(4500);
    expect(await reap(pool, 100)).toHaveLength(1);
    const taken = await claim(pool, "B", 30);
    expect(taken?.id).toBe(job.id);
    expect(taken?.attempts).toBe(2);

    expect(await waitFor(() => /job\.aborted|job\.result_discarded/.test(a.out), 15_000)).toBe(true);
    const mid = await row(job.id);
    expect(mid.status).toBe("running");
    expect(mid.locked_by).toBe("B");
    expect(mid.attempts).toBe(2);
    expect(await complete(pool, job.id, "B")).toBe(true);
    expect((await row(job.id)).status).toBe("succeeded");
  });

  it("W09b a result sent after takeover is refused by fencing", async () => {
    const { job } = await enqueue(pool, newJob({ type: "record", payload: { ms: 800 } }));
    // The worker's own reaper is slowed so only the test's reap can take the job.
    const a = worker({ REAPER_INTERVAL_SECONDS: "60" });
    expect(await waitFor(async () => (await executionsOf(job.id)).length === 1, 15_000)).toBe(true);

    await pool.query("UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1", [job.id]);
    expect(await reap(pool, 100)).toHaveLength(1);
    const taken = await claim(pool, "B", 30);
    expect(taken?.attempts).toBe(2);

    expect(await waitFor(() => a.out.includes("job.result_discarded"), 10_000)).toBe(true);
    expect(a.out).toContain("not owner");
    const mid = await row(job.id);
    expect(mid.status).toBe("running");
    expect(mid.locked_by).toBe("B");
    expect(mid.attempts).toBe(2);
    expect(await complete(pool, job.id, "B")).toBe(true);
  });
});
