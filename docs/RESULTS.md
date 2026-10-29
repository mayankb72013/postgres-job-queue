# Results

Measured with the scripts in `scripts/`. Every cell below stays empty until the experiment has been run on the machine described in the first table. Nothing here is estimated or copied from another machine.

All experiments run against the test database (`jobqueue_test`) and empty its `jobs` table first.

## Machine and versions

The bench script prints the CPU, RAM, OS, Node and PostgreSQL lines; copy them here.

| Item | Value |
|---|---|
| Machine | |
| CPU model | |
| Logical CPUs | |
| RAM | |
| OS | |
| Node | |
| PostgreSQL | |
| Where PostgreSQL runs | Docker container on the same machine, sharing its CPUs with the workers and the script |
| Date | |

## X2: throughput and duplicate executions

Command: `npm run bench`

Handlers do no real work (`echo`) or insert one row (`record`), so this measures the queue's own overhead.

How the numbers are taken:

- **Time** is the database clock from the first finished job to the last, so process start-up is not counted. Throughput is (jobs - 1) divided by that time.
- All jobs are inserted with one statement after every worker has started, so the workers begin together.
- The bench workers use `POLL_INTERVAL_MS=50` (only the wait on an empty queue) and the default log level. Every other setting comes from `.env`.
- **Duplicate executions** is `count(*) - count(DISTINCT job_id)` over the `executions` table, which only `record` fills.

| Run | Jobs | Worker processes | Time | Throughput | Jobs with `attempts <> 1` | Duplicate executions |
|---|---|---|---|---|---|---|
| `echo` | 5,000 | 1 | | | | n/a |
| `echo` | 5,000 | 2 | | | | n/a |
| `echo` | 5,000 | 4 | | | | n/a |
| `record` | 3,000 | 1 | | | | |
| `record` | 3,000 | 4 | | | | |

## X3: query plans

Command (Linux and macOS): `docker compose exec -T db psql -U jobqueue -d jobqueue_test < scripts/explain.sql`

Table of 220,050 rows: 200,000 succeeded, 20,000 queued and ready, 50 running (10 with an expired lease), after `VACUUM (ANALYZE)`.

| Query | Index used | Execution time | Buffers |
|---|---|---|---|
| claim | | | |
| reaper | | | |

| Relation | Size |
|---|---|
| `jobs` (table) | |
| `jobs_pkey` | |
| `jobs_idempotency_key_key` | |
| `idx_jobs_ready` | |
| `idx_jobs_lease` | |

## X4: update cost

Command: `node --env-file=.env --import tsx scripts/hot.ts`

5,000 `echo` jobs on one worker. The script reads `pg_stat_user_tables` before and after the run and reports the difference.

| Jobs | `n_tup_upd` | Updates per job | `n_tup_hot_upd` | HOT ratio |
|---|---|---|---|---|
| 5,000 | | | | |
