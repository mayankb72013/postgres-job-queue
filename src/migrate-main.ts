import { loadConfig } from "./config.js";
import { makePool, migrate } from "./db.js";
import { makeLogger } from "./logger.js";

const config = loadConfig();
const log = makeLogger(config.logLevel);
const pool = makePool(config.databaseUrl, 1, config.dbQueryTimeoutSeconds * 1000);

try {
  await migrate(pool);
  log.info("migrate.done");
} catch (err) {
  log.error("migrate.error", { error: err instanceof Error ? err.message : String(err) });
  process.exitCode = 1;
} finally {
  await pool.end();
}
