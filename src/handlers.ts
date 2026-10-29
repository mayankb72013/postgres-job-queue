import { setTimeout as sleepFor } from "node:timers/promises";

// Throw this to fail a job without retrying it.
export class PermanentError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PermanentError";
  }
}

// The worker injects workerId and recordExecution, so handlers contain no SQL.
export type HandlerContext = {
  signal: AbortSignal;
  jobId: string;
  workerId: string;
  recordExecution: () => Promise<void>;
};

export type Handler = (payload: Record<string, unknown>, ctx: HandlerContext) => Promise<void>;

export const builtinHandlers: Record<string, Handler> = {
  echo: async () => {},

  sleep: async (payload, { signal }) => {
    const ms = payload.ms;
    if (typeof ms !== "number" || !Number.isFinite(ms) || ms < 0) {
      throw new PermanentError("sleep requires payload.ms to be a non-negative number");
    }
    await sleepFor(ms, undefined, { signal });
  },

  flaky: async (payload) => {
    const failRate = typeof payload.failRate === "number" ? payload.failRate : 0.5;
    if (Math.random() < failRate) throw new Error("flaky handler failed");
  },

  always_fail: async () => {
    throw new Error("always_fail handler always fails");
  },

  permanent_fail: async () => {
    throw new PermanentError("permanent_fail handler never succeeds");
  },
};

const durationMs = (payload: Record<string, unknown>) =>
  typeof payload.ms === "number" && payload.ms > 0 ? payload.ms : 0;

// Enabled only by TEST_HANDLERS=1, and kept out of builtinHandlers so the API never lists them.
export const testHandlers: Record<string, Handler> = {
  // Ignores the abort signal on purpose: it models a handler that cannot be stopped.
  record: async (payload, { recordExecution }) => {
    await recordExecution();
    const ms = durationMs(payload);
    if (ms > 0) await sleepFor(ms);
  },

  // Spins the event loop, so no timer (heartbeat included) can fire meanwhile.
  block: async (payload) => {
    const end = performance.now() + durationMs(payload);
    while (performance.now() < end) {
      // busy wait
    }
  },

  // A poison job: it kills the whole process, so nobody gets to report a failure.
  crash: async (_payload, { recordExecution }) => {
    await recordExecution();
    process.kill(process.pid, "SIGKILL");
    await new Promise(() => {});
  },
};
