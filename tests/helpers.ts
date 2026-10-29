import { spawn, type ChildProcess } from "node:child_process";
import { makePool, migrate } from "../src/db.js";

export const TEST_URL =
  process.env.TEST_DATABASE_URL ?? "postgres://jobqueue:jobqueue@localhost:5432/jobqueue_test";

export const pool = makePool(TEST_URL, 30, 10_000);

// `executions` exists only in the test database; the test-only `record` handler writes to it.
export async function resetDb(): Promise<void> {
  await migrate(pool);
  await pool.query(
    `CREATE TABLE IF NOT EXISTS executions (
       job_id BIGINT      NOT NULL,
       worker TEXT        NOT NULL,
       ts     TIMESTAMPTZ NOT NULL DEFAULT now()
     )`,
  );
  await pool.query("TRUNCATE jobs, executions RESTART IDENTITY");
}

export type JobInput = {
  type: string;
  payload: Record<string, unknown>;
  priority: number;
  maxAttempts: number;
  runAt?: Date | string;
  idempotencyKey?: string;
};

export function newJob(overrides: Partial<JobInput> = {}): JobInput {
  return { type: "echo", payload: {}, priority: 0, maxAttempts: 5, ...overrides };
}

export async function row(id: string | number) {
  const result = await pool.query("SELECT * FROM jobs WHERE id = $1", [id]);
  return result.rows[0];
}

export async function waitFor(
  fn: () => boolean | Promise<boolean>,
  ms = 5_000,
  step = 50,
): Promise<boolean> {
  const deadline = performance.now() + ms;
  while (true) {
    if (await fn()) return true;
    if (performance.now() >= deadline) return false;
    await new Promise((resolve) => setTimeout(resolve, step));
  }
}

export type WorkerProcess = ChildProcess & { out: string };

export function startWorker(env: Record<string, string> = {}): WorkerProcess {
  const child = spawn(process.execPath, ["--import", "tsx", "src/worker-main.ts"], {
    env: { ...process.env, DATABASE_URL: TEST_URL, TEST_HANDLERS: "1", LOG_LEVEL: "warn", ...env },
    stdio: ["ignore", "pipe", "inherit", "ipc"],
  }) as WorkerProcess;
  child.out = "";
  child.stdout?.on("data", (chunk: Buffer) => {
    child.out += chunk.toString();
  });
  return child;
}

// An IPC message works on every OS, unlike a signal.
export function stopWorker(child: ChildProcess): void {
  child.send("stop");
}

export function exited(child: ChildProcess): Promise<number | null> {
  if (child.exitCode !== null) return Promise.resolve(child.exitCode);
  return new Promise((resolve) => child.once("exit", (code) => resolve(code)));
}

export function urlViaPort(port: number): string {
  const url = new URL(TEST_URL);
  url.port = String(port);
  return url.toString();
}
