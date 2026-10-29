// X2: throughput and duplicate executions. Runs against the test database and empties it first.
//   npm run bench                         full run: 5,000 echo jobs on 1, 2, 4 workers; 3,000 record jobs on 1, 4
//   npm run bench -- --echo 200 --record 100   quick check of the script itself (numbers are not results)
import { parseArgs } from "node:util";
import { pool, resetDb } from "../tests/helpers.js";
import { describeEnvironment, isClean, runLoad, type LoadResult, type LoadSpec } from "./load.js";

const { values } = parseArgs({ options: { echo: { type: "string" }, record: { type: "string" } } });

function count(raw: string | undefined, fallback: number, name: string): number {
  const n = raw === undefined ? fallback : Number(raw);
  if (!Number.isInteger(n) || n < 2) {
    console.error(`--${name} must be an integer of at least 2`);
    process.exit(1);
  }
  return n;
}

const echoJobs = count(values.echo, 5_000, "echo");
const recordJobs = count(values.record, 3_000, "record");

const plan: LoadSpec[] = [
  { type: "echo", jobs: echoJobs, workers: 1 },
  { type: "echo", jobs: echoJobs, workers: 2 },
  { type: "echo", jobs: echoJobs, workers: 4 },
  { type: "record", jobs: recordJobs, workers: 1 },
  { type: "record", jobs: recordJobs, workers: 4 },
];

const n = (value: number) => value.toLocaleString("en-US");

function row(r: LoadResult): string {
  const duplicates =
    r.executions === null
      ? "n/a"
      : `${n(r.executions - (r.distinctJobs ?? 0))} (${n(r.executions)} executions of ${n(r.distinctJobs ?? 0)} distinct jobs)`;
  return [
    r.type,
    n(r.jobs),
    r.workers,
    `${r.seconds.toFixed(1)} s`,
    `${n(Math.round(r.jobsPerSecond))} jobs/s`,
    n(r.badAttempts),
    duplicates,
  ].join(" | ");
}

console.log((await describeEnvironment()).join("\n"));
console.log("");

const results: LoadResult[] = [];
try {
  for (const spec of plan) {
    await resetDb();
    console.error(`running: ${spec.jobs} ${spec.type} jobs on ${spec.workers} worker(s) ...`);
    results.push(await runLoad(spec));
  }
} finally {
  await pool.end();
}

console.log("| Run | Jobs | Worker processes | Time | Throughput | Jobs with attempts <> 1 | Duplicate executions |");
console.log("|---|---|---|---|---|---|---|");
for (const r of results) console.log(`| ${row(r)} |`);

const unclean = results.filter((r) => !isClean(r));
if (unclean.length > 0) {
  console.error(`\nINVARIANT VIOLATED in ${unclean.length} run(s): do not record these numbers.`);
  process.exitCode = 1;
}
