import { setTimeout as sleepFor } from "node:timers/promises";

// Throw this to fail a job without retrying it.
export class PermanentError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PermanentError";
  }
}

export type HandlerContext = { signal: AbortSignal; jobId: string };

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
