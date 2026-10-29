// X4: how many updates a job costs, and how many of them are HOT updates.
// Runs against the test database and empties it first.
//   node --env-file=.env --import tsx scripts/hot.ts
import { setTimeout as sleepFor } from "node:timers/promises";
import { pool, resetDb } from "../tests/helpers.js";
import { isClean, runLoad } from "./load.js";

const JOBS = 5_000;
const SETTLE_MS = 1_500;
const SETTLE_TRIES = 10;

type TableStats = { updates: number; hotUpdates: number };

async function readStats(): Promise<TableStats> {
  const result = await pool.query<{ n_tup_upd: number; n_tup_hot_upd: number }>(
    "SELECT n_tup_upd::float8, n_tup_hot_upd::float8 FROM pg_stat_user_tables WHERE relname = $1",
    ["jobs"],
  );
  const stats = result.rows[0];
  return { updates: stats?.n_tup_upd ?? 0, hotUpdates: stats?.n_tup_hot_upd ?? 0 };
}

// PostgreSQL reports these counters a moment after the work, so read until two reads agree.
async function settledStats(): Promise<TableStats> {
  let previous = await readStats();
  for (let i = 0; i < SETTLE_TRIES; i++) {
    await sleepFor(SETTLE_MS);
    const current = await readStats();
    if (current.updates === previous.updates && current.hotUpdates === previous.hotUpdates) return current;
    previous = current;
  }
  throw new Error("table statistics did not settle");
}

try {
  await resetDb();
  const before = await settledStats();
  const result = await runLoad({ type: "echo", jobs: JOBS, workers: 1 });
  const after = await settledStats();

  const updates = after.updates - before.updates;
  const hotUpdates = after.hotUpdates - before.hotUpdates;

  console.log(`${JOBS} echo jobs on 1 worker`);
  console.log("| Jobs | n_tup_upd | Updates per job | n_tup_hot_upd | HOT ratio |");
  console.log("|---|---|---|---|---|");
  console.log(
    `| ${JOBS.toLocaleString("en-US")} | ${updates} | ${(updates / JOBS).toFixed(2)} | ${hotUpdates} | ${updates > 0 ? ((hotUpdates / updates) * 100).toFixed(1) : "n/a"} % |`,
  );

  if (!isClean(result)) {
    console.error("\nINVARIANT VIOLATED: some job did not run exactly once; do not record these numbers.");
    process.exitCode = 1;
  }
} finally {
  await pool.end();
}
