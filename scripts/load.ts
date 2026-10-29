// Shared by bench.ts (X2) and hot.ts (X4): runs real worker processes against the test database.
// The SQL here only measures the run; the queue's own SQL stays in src/queue.ts.
import os from "node:os";
import { setTimeout as sleepFor } from "node:timers/promises";
import { metrics } from "../src/queue.js";
import { exited, pool, startWorker, stopWorker, waitFor, type WorkerProcess } from "../tests/helpers.js";

export type LoadSpec = { type: "echo" | "record"; jobs: number; workers: number };

export type LoadResult = LoadSpec & {
  seconds: number;
  jobsPerSecond: number;
  badAttempts: number;
  notSucceeded: number;
  // Only the `record` handler writes executions.
  executions: number | null;
  distinctJobs: number | null;
};

const READY_TIMEOUT_MS = 30_000;
const RUN_TIMEOUT_MS = 300_000;
const STOP_TIMEOUT_MS = 10_000;

// If the benchmark is interrupted, no worker may be left running and consuming the queue.
const live = new Set<WorkerProcess>();
process.once("SIGINT", () => {
  for (const worker of live) worker.kill("SIGKILL");
  process.exit(130);
});

function spawnWorker(): WorkerProcess {
  // "info" prints worker.started, which is the readiness signal. The short idle poll only makes
  // workers notice the new jobs together; the busy path is unchanged.
  const worker = startWorker({ LOG_LEVEL: "info", POLL_INTERVAL_MS: "50" });
  live.add(worker);
  worker.once("exit", () => live.delete(worker));
  return worker;
}

async function stopAll(workers: WorkerProcess[]): Promise<void> {
  await Promise.all(
    workers.map(async (worker) => {
      if (worker.connected) stopWorker(worker);
      const exitedInTime = await Promise.race([
        exited(worker).then(() => true),
        sleepFor(STOP_TIMEOUT_MS).then(() => false),
      ]);
      if (!exitedInTime) worker.kill("SIGKILL");
    }),
  );
}

// Expects an empty jobs table (call resetDb() first).
export async function runLoad(spec: LoadSpec): Promise<LoadResult> {
  const workers: WorkerProcess[] = [];
  try {
    for (let i = 0; i < spec.workers; i++) workers.push(spawnWorker());
    const ready = await waitFor(
      () => workers.every((w) => w.out.includes('"event":"worker.started"')),
      READY_TIMEOUT_MS,
    );
    if (!ready) throw new Error("workers did not start in time");

    // One statement, so every job becomes visible to every worker at the same commit.
    await pool.query("INSERT INTO jobs (type) SELECT $1 FROM generate_series(1, $2::int)", [
      spec.type,
      spec.jobs,
    ]);

    const finished = await waitFor(
      async () => {
        const { counts } = await metrics(pool);
        return counts.succeeded + counts.dead === spec.jobs;
      },
      RUN_TIMEOUT_MS,
      250,
    );
    if (!finished) throw new Error(`${spec.jobs} jobs did not finish within ${RUN_TIMEOUT_MS / 1000} s`);
  } finally {
    await stopAll(workers);
  }
  return measure(spec);
}

// Time is the database clock from the first finished job to the last, so process start-up and
// the polling of this script are not counted. N jobs make N - 1 intervals.
async function measure(spec: LoadSpec): Promise<LoadResult> {
  const stats = await pool.query<{ bad_attempts: number; not_succeeded: number; seconds: number | null }>(
    `SELECT count(*) FILTER (WHERE attempts <> 1)::int AS bad_attempts,
            count(*) FILTER (WHERE status <> 'succeeded')::int AS not_succeeded,
            extract(epoch FROM max(finished_at) - min(finished_at))::float8 AS seconds
     FROM jobs`,
  );
  const { bad_attempts, not_succeeded, seconds } = stats.rows[0] ?? { bad_attempts: 0, not_succeeded: 0, seconds: null };
  const elapsed = seconds ?? 0;

  let executions: number | null = null;
  let distinctJobs: number | null = null;
  if (spec.type === "record") {
    const log = await pool.query<{ executions: number; distinct_jobs: number }>(
      "SELECT count(*)::int AS executions, count(DISTINCT job_id)::int AS distinct_jobs FROM executions",
    );
    executions = log.rows[0]?.executions ?? 0;
    distinctJobs = log.rows[0]?.distinct_jobs ?? 0;
  }

  return {
    ...spec,
    seconds: elapsed,
    jobsPerSecond: elapsed > 0 ? (spec.jobs - 1) / elapsed : 0,
    badAttempts: bad_attempts,
    notSucceeded: not_succeeded,
    executions,
    distinctJobs,
  };
}

// The two invariants of the project: every job ran exactly once, and none ran twice.
export function isClean(r: LoadResult): boolean {
  const noDuplicates = r.executions === null || (r.executions === r.jobs && r.distinctJobs === r.jobs);
  return r.badAttempts === 0 && r.notSucceeded === 0 && noDuplicates;
}

export async function describeEnvironment(): Promise<string[]> {
  const cpus = os.cpus();
  const version = await pool.query<{ server_version: string }>("SHOW server_version");
  return [
    `CPU: ${cpus[0]?.model ?? "unknown"}, ${cpus.length} logical CPUs`,
    `RAM: ${(os.totalmem() / 1024 ** 3).toFixed(1)} GB`,
    `OS: ${os.type()} ${os.release()}`,
    `Node: ${process.version}`,
    `PostgreSQL: ${version.rows[0]?.server_version ?? "unknown"} (Docker; shares the CPUs above with the workers)`,
  ];
}
