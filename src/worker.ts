import { randomBytes } from "node:crypto";
import { hostname } from "node:os";
import { setTimeout as sleepFor } from "node:timers/promises";
import type pg from "pg";
import type { Config } from "./config.js";
import { PermanentError, type Handler } from "./handlers.js";
import type { Logger } from "./logger.js";
import { claim, complete, fail, type JobRow } from "./queue.js";

// The random suffix keeps a restarted worker from looking like its previous process.
export function makeWorkerId(): string {
  return `${hostname()}-${process.pid}-${randomBytes(3).toString("hex")}`;
}

const CLAIM_BACKOFF_START_MS = 500;
const CLAIM_BACKOFF_MAX_MS = 30_000;
const REPORT_RETRY_DELAYS_MS = [200, 400, 800];

type Verdict =
  | { kind: "success" }
  | { kind: "error"; error: unknown }
  | { kind: "timeout" };

const jitter = (ms: number) => ms * (0.8 + Math.random() * 0.4);
const messageOf = (err: unknown) => (err instanceof Error ? err.message : String(err));

export class Worker {
  constructor(
    private readonly pool: pg.Pool,
    private readonly config: Config,
    private readonly handlers: Record<string, Handler>,
    private readonly log: Logger,
    readonly workerId: string = makeWorkerId(),
  ) {}

  async run(): Promise<never> {
    this.log.info("worker.started", { worker: this.workerId });
    let claimBackoffMs = CLAIM_BACKOFF_START_MS;
    while (true) {
      let job: JobRow | null;
      try {
        job = await claim(this.pool, this.workerId, this.config.leaseSeconds);
        claimBackoffMs = CLAIM_BACKOFF_START_MS;
      } catch (err) {
        this.log.error("claim.error", { worker: this.workerId, error: messageOf(err), retryInMs: claimBackoffMs });
        await sleepFor(claimBackoffMs);
        claimBackoffMs = Math.min(claimBackoffMs * 2, CLAIM_BACKOFF_MAX_MS);
        continue;
      }
      if (!job) {
        await sleepFor(jitter(this.config.pollIntervalMs));
        continue;
      }
      await this.execute(job);
    }
  }

  // Two separate steps: an error while reporting must never change the handler's verdict.
  private async execute(job: JobRow): Promise<void> {
    this.log.info("job.started", { job: job.id, worker: this.workerId, attempt: job.attempts });
    const verdict = await this.runHandler(job);
    await this.report(job, verdict);
  }

  private async runHandler(job: JobRow): Promise<Verdict> {
    const handler = this.handlers[job.type];
    if (!handler) {
      return { kind: "error", error: new PermanentError(`no handler registered for type "${job.type}"`) };
    }

    const controller = new AbortController();
    let timer: NodeJS.Timeout | undefined;
    const timedOut = new Promise<Verdict>((resolve) => {
      timer = setTimeout(() => {
        controller.abort();
        resolve({ kind: "timeout" });
      }, this.config.jobTimeoutSeconds * 1000);
    });
    // The race lets the worker move on even when a handler ignores the abort signal.
    const finished = (async () => handler(job.payload, { signal: controller.signal, jobId: job.id }))().then(
      (): Verdict => ({ kind: "success" }),
      (error: unknown): Verdict => ({ kind: "error", error }),
    );

    try {
      return await Promise.race([finished, timedOut]);
    } finally {
      clearTimeout(timer);
    }
  }

  private async report(job: JobRow, verdict: Verdict): Promise<void> {
    const ids = { job: job.id, worker: this.workerId };
    try {
      if (verdict.kind === "success") {
        const owned = await this.withRetries(() => complete(this.pool, job.id, this.workerId));
        if (owned) this.log.info("job.succeeded", ids);
        else this.log.warn("job.result_discarded", { ...ids, reason: "not owner" });
        return;
      }

      const failure =
        verdict.kind === "timeout"
          ? { message: `job exceeded ${this.config.jobTimeoutSeconds}s timeout`, retryable: true }
          : { message: messageOf(verdict.error), retryable: !(verdict.error instanceof PermanentError) };
      const outcome = await this.withRetries(() =>
        fail(this.pool, job.id, this.workerId, failure, {
          baseSeconds: this.config.backoffBaseSeconds,
          maxSeconds: this.config.backoffMaxSeconds,
          jitterSeconds: this.config.backoffJitterSeconds,
        }),
      );
      if (outcome === null) {
        this.log.warn("job.result_discarded", { ...ids, reason: "not owner" });
      } else {
        this.log.warn("job.failed", { ...ids, error: failure.message, outcome, retryable: failure.retryable });
      }
    } catch (err) {
      // The row is left alone: the lease expires and the reaper requeues it.
      this.log.error("job.report_error", { ...ids, error: messageOf(err) });
    }
  }

  // One try plus three retries after 200, 400 and 800 ms; the last error is thrown.
  private async withRetries<T>(fn: () => Promise<T>): Promise<T> {
    for (const delayMs of REPORT_RETRY_DELAYS_MS) {
      try {
        return await fn();
      } catch {
        await sleepFor(delayMs);
      }
    }
    return fn();
  }
}
