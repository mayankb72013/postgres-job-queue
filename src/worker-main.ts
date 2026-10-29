import { setTimeout as sleepFor } from "node:timers/promises";
import { loadConfig } from "./config.js";
import { makePool } from "./db.js";
import { builtinHandlers, testHandlers } from "./handlers.js";
import { makeLogger } from "./logger.js";
import { Worker } from "./worker.js";

const POOL_CLOSE_LIMIT_MS = 5_000;

const config = loadConfig();
const log = makeLogger(config.logLevel);
const pool = makePool(config.databaseUrl, 5, config.dbQueryTimeoutSeconds * 1000);
const handlers = process.env.TEST_HANDLERS === "1" ? { ...builtinHandlers, ...testHandlers } : builtinHandlers;

const worker = new Worker(pool, config, handlers, log);

// The IPC message is the stop request that also works on Windows, where signals do not.
process.on("SIGTERM", () => worker.stop());
process.on("SIGINT", () => worker.stop());
process.on("message", (message) => {
  if (message === "stop") worker.stop();
});

await worker.run();

// A silent database must not be able to hang the shutdown.
await Promise.race([pool.end().catch(() => {}), sleepFor(POOL_CLOSE_LIMIT_MS)]);
process.exit(0);
