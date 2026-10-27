import { describe, expect, it } from "vitest";
import { loadConfig } from "../src/config.js";

// An empty env makes the tests independent of the developer's shell and .env.
const load = (overrides: Record<string, string | number> = {}) => loadConfig(overrides, {});

describe("config", () => {
  it("C01 defaults load and equal the values in chapter 11", () => {
    expect(load()).toEqual({
      databaseUrl: "postgres://jobqueue:jobqueue@localhost:5432/jobqueue",
      port: 3000,
      leaseSeconds: 30,
      heartbeatSeconds: 10,
      pollIntervalMs: 500,
      reaperIntervalSeconds: 10,
      dbQueryTimeoutSeconds: 10,
      jobTimeoutSeconds: 60,
      shutdownGraceSeconds: 20,
      backoffBaseSeconds: 5,
      backoffMaxSeconds: 300,
      backoffJitterSeconds: 5,
      payloadLimitBytes: 65536,
      logLevel: "info",
    });
  });

  it("C02 HEARTBEAT 11 with LEASE 30 throws; HEARTBEAT 10 passes", () => {
    expect(() => load({ LEASE_SECONDS: 30, HEARTBEAT_SECONDS: 11 })).toThrow(/HEARTBEAT_SECONDS/);
    expect(load({ LEASE_SECONDS: 30, HEARTBEAT_SECONDS: 10 }).heartbeatSeconds).toBe(10);
  });

  it("C03 zero, negative, non-numeric values and an unknown LOG_LEVEL throw", () => {
    const strictlyPositive = [
      "LEASE_SECONDS",
      "POLL_INTERVAL_MS",
      "REAPER_INTERVAL_SECONDS",
      "DB_QUERY_TIMEOUT_SECONDS",
      "JOB_TIMEOUT_SECONDS",
      "PAYLOAD_LIMIT_BYTES",
    ];
    for (const name of strictlyPositive) {
      expect(() => load({ [name]: 0 }), `${name}=0`).toThrow(name);
    }

    const notNegative = [
      "SHUTDOWN_GRACE_SECONDS",
      "BACKOFF_BASE_SECONDS",
      "BACKOFF_MAX_SECONDS",
      "BACKOFF_JITTER_SECONDS",
    ];
    for (const name of notNegative) {
      expect(load({ [name]: 0 }), `${name}=0`).toBeDefined();
    }

    for (const name of [...strictlyPositive, ...notNegative, "PORT"]) {
      expect(() => load({ [name]: -1 }), `${name}=-1`).toThrow(name);
      expect(() => load({ [name]: "abc" }), `${name}=abc`).toThrow(name);
    }

    expect(() => load({ PORT: 3000.5 })).toThrow(/PORT/);
    expect(() => load({ LOG_LEVEL: "verbose" })).toThrow(/LOG_LEVEL/);
  });

  it("C04 a job timeout longer than the lease is allowed", () => {
    const config = load({ LEASE_SECONDS: 30, JOB_TIMEOUT_SECONDS: 600 });
    expect(config.jobTimeoutSeconds).toBe(600);
    expect(config.leaseSeconds).toBe(30);
  });
});
