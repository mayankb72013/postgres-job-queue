import { makeApp } from "./api.js";
import { loadConfig } from "./config.js";
import { makePool, migrate } from "./db.js";
import { makeLogger } from "./logger.js";

const config = loadConfig();
const log = makeLogger(config.logLevel);
const pool = makePool(config.databaseUrl, 10, config.dbQueryTimeoutSeconds * 1000);

await migrate(pool);
makeApp(pool, config).listen(config.port, () => {
  log.info("api.started", { port: config.port });
});
