import { loadConfig } from "./config.js";
import { makePool } from "./db.js";
import { builtinHandlers } from "./handlers.js";
import { makeLogger } from "./logger.js";
import { Worker } from "./worker.js";

const config = loadConfig();
const log = makeLogger(config.logLevel);
const pool = makePool(config.databaseUrl, 5, config.dbQueryTimeoutSeconds * 1000);

await new Worker(pool, config, builtinHandlers, log).run();
