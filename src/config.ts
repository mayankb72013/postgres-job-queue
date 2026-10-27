import { z } from "zod";

const positive = (fallback: number) => z.coerce.number().positive().default(fallback);
const nonNegative = (fallback: number) => z.coerce.number().min(0).default(fallback);

const envSchema = z
  .object({
    DATABASE_URL: z
      .string()
      .min(1)
      .default("postgres://jobqueue:jobqueue@localhost:5432/jobqueue"),
    PORT: z.coerce.number().int().min(0).max(65535).default(3000),
    LEASE_SECONDS: positive(30),
    HEARTBEAT_SECONDS: positive(10),
    POLL_INTERVAL_MS: positive(500),
    REAPER_INTERVAL_SECONDS: positive(10),
    DB_QUERY_TIMEOUT_SECONDS: positive(10),
    JOB_TIMEOUT_SECONDS: positive(60),
    SHUTDOWN_GRACE_SECONDS: nonNegative(20),
    BACKOFF_BASE_SECONDS: nonNegative(5),
    BACKOFF_MAX_SECONDS: nonNegative(300),
    BACKOFF_JITTER_SECONDS: nonNegative(5),
    PAYLOAD_LIMIT_BYTES: positive(65536),
    LOG_LEVEL: z.enum(["debug", "info", "warn", "error"]).default("info"),
  })
  .refine((env) => env.HEARTBEAT_SECONDS * 3 <= env.LEASE_SECONDS, {
    // Three missed heartbeats must fit inside one lease, or a healthy worker loses its jobs.
    message: "HEARTBEAT_SECONDS x 3 must be <= LEASE_SECONDS",
    path: ["HEARTBEAT_SECONDS"],
  });

export type Config = {
  databaseUrl: string;
  port: number;
  leaseSeconds: number;
  heartbeatSeconds: number;
  pollIntervalMs: number;
  reaperIntervalSeconds: number;
  dbQueryTimeoutSeconds: number;
  jobTimeoutSeconds: number;
  shutdownGraceSeconds: number;
  backoffBaseSeconds: number;
  backoffMaxSeconds: number;
  backoffJitterSeconds: number;
  payloadLimitBytes: number;
  logLevel: "debug" | "info" | "warn" | "error";
};

type Source = Record<string, string | number | undefined>;

// Overrides use the environment variable names and win over `env`.
export function loadConfig(overrides: Source = {}, env: Source = process.env): Config {
  const merged: Source = { ...env };
  for (const [name, value] of Object.entries(overrides)) {
    if (value !== undefined) merged[name] = value;
  }

  const result = envSchema.safeParse(merged);
  if (!result.success) {
    const problems = result.error.issues.map((i) => `${i.path.join(".") || "config"}: ${i.message}`);
    throw new Error(`Invalid configuration:\n  ${problems.join("\n  ")}`);
  }

  const c = result.data;
  return {
    databaseUrl: c.DATABASE_URL,
    port: c.PORT,
    leaseSeconds: c.LEASE_SECONDS,
    heartbeatSeconds: c.HEARTBEAT_SECONDS,
    pollIntervalMs: c.POLL_INTERVAL_MS,
    reaperIntervalSeconds: c.REAPER_INTERVAL_SECONDS,
    dbQueryTimeoutSeconds: c.DB_QUERY_TIMEOUT_SECONDS,
    jobTimeoutSeconds: c.JOB_TIMEOUT_SECONDS,
    shutdownGraceSeconds: c.SHUTDOWN_GRACE_SECONDS,
    backoffBaseSeconds: c.BACKOFF_BASE_SECONDS,
    backoffMaxSeconds: c.BACKOFF_MAX_SECONDS,
    backoffJitterSeconds: c.BACKOFF_JITTER_SECONDS,
    payloadLimitBytes: c.PAYLOAD_LIMIT_BYTES,
    logLevel: c.LOG_LEVEL,
  };
}
