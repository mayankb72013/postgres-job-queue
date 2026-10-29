import { randomBytes } from "node:crypto";
import { hostname } from "node:os";
import { setTimeout as sleepFor } from "node:timers/promises";
import type pg from "pg";
import type { Config } from "./config.js";
import { PermanentError, type Handler } from "./handlers.js";
import type { Logger } from "./logger.js";
import { claim, complete, fail, heartbeat, reap, recordExecution, release, type JobRow } from "./queue.js";

// The random suffix keeps a restarted worker from looking like its previous process.
export function makeWorkerId(): string {
  return `${hostname()}-${process.pid}-${randomBytes(3).toString("hex")}`;
}

const CLAIM_BACKOFF_START_MS = 500;
const CLAIM_BACKOFF_MAX_MS = 30_000;
const REPORT_RETRY_DELAYS_MS = [200, 400, 800];
const REAP_BATCH_SIZE = 100;
const SHUTDOWN_RELEASE_REASON = "released on worker shutdown";

type Verdict =
  | { kind: "success" }
  | { kind: "error"; error: unknown }
  | { kind: "timeout" }
  | { kind: "lease_lost"; reason: string }
  | { kind: "shutdown" };

// Everything that can end the handler early funnels through `abort`; the first caller wins.
type JobRun = {
  controller: AbortController;
  aborted: Promise<Verdict>;
  abort: (verdict: Verdict) => void;
  graceTimer?: NodeJS.Timeout;
};

const jitter = (ms: number) => ms * (0.8 + Math.random() * 0.4);
const messageOf = (err: unknown) => (err instanceof Error ? err.message : String(err));

export class Worker {
  private stopping = false;
  private readonly wake = new AbortController();
  private current: JobRun | null = null;
  private reaperTimer: NodeJS.Timeout | undefined;

  constructor(
    private readonly pool: pg.Pool,
    private readonly config: Config,
    private readonly handlers: Record<string, Handler>,
    private readonly log: Logger,
    readonly workerId: string = makeWorkerId(),
  ) {}

  // Resolves once the loop has ended after stop(); closing the pool is the caller's job.
  async run(): Promise<void> {
    this.log.info("worker.started", { worker: this.workerId });
    this.scheduleReaper();
    let claimBackoffMs = CLAIM_BACKOFF_START_MS;
    while (!this.stopping) {
      // The stopwatch starts before the statement is sent, so it can only err on the safe side.
      const claimSentAt = performance.now();
      let job: JobRow | null;
      try {
        job = await claim(this.pool, this.workerId, this.config.leaseSeconds);
        claimBackoffMs = CLAIM_BACKOFF_START_MS;
      } catch (err) {
        this.log.error("claim.error", { worker: this.workerId, error: messageOf(err), retryInMs: claimBackoffMs });
        await this.idle(claimBackoffMs);
        claimBackoffMs = Math.min(claimBackoffMs * 2, CLAIM_BACKOFF_MAX_MS);
        continue;
      }
      if (!job) {
        await this.idle(jitter(this.config.pollIntervalMs));
        continue;
      }
      await this.execute(job, claimSentAt);
    }
    clearTimeout(this.reaperTimer);
    this.log.info("worker.stopped", { worker: this.workerId });
  }

  // Idempotent. An idle worker wakes at once; a busy one gets the grace period to finish.
  stop(): void {
    if (this.stopping) return;
    this.stopping = true;
    this.log.info("worker.stopping", { worker: this.workerId });
    this.wake.abort();
    clearTimeout(this.reaperTimer);
    if (this.current) this.armGraceTimer(this.current);
  }

  // Sleeps, but returns early when stop() is called.
  private async idle(ms: number): Promise<void> {
    try {
      await sleepFor(ms, undefined, { signal: this.wake.signal });
    } catch {
      // aborted by stop()
    }
  }

  private armGraceTimer(run: JobRun): void {
    run.graceTimer = setTimeout(() => run.abort({ kind: "shutdown" }), this.config.shutdownGraceSeconds * 1000);
  }

  private scheduleReaper(): void {
    this.reaperTimer = setTimeout(() => void this.reapOnce(), jitter(this.config.reaperIntervalSeconds * 1000));
  }

  // Every worker reaps; the next tick is scheduled only after this one ends, so ticks never overlap.
  private async reapOnce(): Promise<void> {
    try {
      const recovered = await reap(this.pool, REAP_BATCH_SIZE);
      if (recovered.length > 0) {
        this.log.warn("reaper.requeued", { worker: this.workerId, count: recovered.length, jobs: recovered });
      }
    } catch (err) {
      this.log.error("reaper.error", { worker: this.workerId, error: messageOf(err) });
    }
    if (!this.stopping) this.scheduleReaper();
  }

  // Two separate steps: an error while reporting must never change the handler's verdict.
  private async execute(job: JobRow, claimSentAt: number): Promise<void> {
    this.log.info("job.started", { job: job.id, worker: this.workerId, attempt: job.attempts });
    const run = this.newRun();
    this.current = run;
    if (this.stopping) this.armGraceTimer(run);
    const heartbeatTimer = this.startHeartbeat(job, run, claimSentAt);
    try {
      const verdict = await this.runHandler(job, run);
      clearTimeout(run.graceTimer);
      await this.report(job, verdict);
    } finally {
      clearInterval(heartbeatTimer);
      clearTimeout(run.graceTimer);
      this.current = null;
    }
  }

  private newRun(): JobRun {
    const controller = new AbortController();
    let settle!: (verdict: Verdict) => void;
    const aborted = new Promise<Verdict>((resolve) => {
      settle = resolve;
    });
    // Settle before aborting, so the verdict is the cause and not the handler's reaction to it.
    const abort = (verdict: Verdict) => {
      settle(verdict);
      controller.abort();
    };
    return { controller, aborted, abort };
  }

  // The heartbeat rule of spec 9.1: stopwatch first, one in flight, bookkeeping from the send time.
  private startHeartbeat(job: JobRow, run: JobRun, claimSentAt: number): NodeJS.Timeout {
    const leaseMs = this.config.leaseSeconds * 1000;
    const ids = { job: job.id, worker: this.workerId };
    let lastGoodAt = claimSentAt;
    let inFlight = false;
    let leaseLost = false;

    const loseLease = (reason: string) => {
      leaseLost = true;
      run.abort({ kind: "lease_lost", reason });
    };

    return setInterval(() => {
      if (leaseLost) return;
      if (performance.now() - lastGoodAt > leaseMs) {
        loseLease("no successful heartbeat for more than the lease");
        return;
      }
      if (inFlight) return;
      inFlight = true;
      const sentAt = performance.now();
      heartbeat(this.pool, job.id, this.workerId, this.config.leaseSeconds)
        .then(
          (owned) => {
            if (leaseLost) return;
            if (owned) lastGoodAt = sentAt;
            else loseLease("not owner");
          },
          (err: unknown) => this.log.warn("heartbeat.error", { ...ids, error: messageOf(err) }),
        )
        .finally(() => {
          inFlight = false;
        });
    }, this.config.heartbeatSeconds * 1000);
  }

  private async runHandler(job: JobRow, run: JobRun): Promise<Verdict> {
    const handler = this.handlers[job.type];
    if (!handler) {
      return { kind: "error", error: new PermanentError(`no handler registered for type "${job.type}"`) };
    }

    const timer = setTimeout(() => run.abort({ kind: "timeout" }), this.config.jobTimeoutSeconds * 1000);
    // The race lets the worker move on even when a handler ignores the abort signal.
    const finished = (async () =>
      handler(job.payload, {
        signal: run.controller.signal,
        jobId: job.id,
        workerId: this.workerId,
        recordExecution: () => recordExecution(this.pool, job.id, this.workerId),
      }))().then(
      (): Verdict => ({ kind: "success" }),
      (error: unknown): Verdict => ({ kind: "error", error }),
    );

    try {
      return await Promise.race([finished, run.aborted]);
    } finally {
      clearTimeout(timer);
    }
  }

  private async report(job: JobRow, verdict: Verdict): Promise<void> {
    const ids = { job: job.id, worker: this.workerId };
    try {
      if (verdict.kind === "lease_lost") {
        // Someone else owns the job now, so there is nothing to write.
        this.log.warn("job.aborted", { ...ids, reason: verdict.reason });
        return;
      }

      if (verdict.kind === "shutdown") {
        const owned = await this.withRetries(() => release(this.pool, job.id, this.workerId, SHUTDOWN_RELEASE_REASON));
        if (owned) this.log.warn("job.released", ids);
        else this.log.warn("job.result_discarded", { ...ids, reason: "not owner" });
        return;
      }

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
