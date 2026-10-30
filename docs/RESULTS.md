# Results

Measured with the scripts in `scripts/`. Every number below comes from my own runs on the machine in the first table (29 to 30 October 2026). Nothing here is estimated or copied from another machine.

All experiments run against the test database (`jobqueue_test`) and empty its `jobs` table first.

## Machine and versions

Taken from the header that `npm run bench` prints.

| Item | Value |
|---|---|
| Machine | Laptop |
| CPU model | AMD Ryzen 5 5600H with Radeon Graphics |
| Logical CPUs | 12 |
| RAM | 22.8 GB |
| OS | Linux 7.0.9-arch1-1 |
| Node | v22.16.0 |
| PostgreSQL | 16.15 (Debian 16.15-1.pgdg13+2) |
| Where PostgreSQL runs | Docker container on the same machine, sharing its CPUs with the workers and the script |
| Date | 29 to 30 October 2026 |

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
| `echo` | 5,000 | 1 | 14.5 s | 345 jobs/s | 0 | n/a |
| `echo` | 5,000 | 2 | 8.4 s | 598 jobs/s | 0 | n/a |
| `echo` | 5,000 | 4 | 4.3 s | 1,152 jobs/s | 0 | n/a |
| `record` | 3,000 | 1 | 12.9 s | 232 jobs/s | 0 | 0 (3,000 executions of 3,000 distinct jobs) |
| `record` | 3,000 | 4 | 3.9 s | 767 jobs/s | 0 | 0 (3,000 executions of 3,000 distinct jobs) |

One run per row, on a laptop with the database on the same machine. Throughput rises almost linearly from one to four worker processes, and no job was executed twice.

## X3: query plans

Command (Linux and macOS): `docker compose exec -T db psql -U jobqueue -d jobqueue_test < scripts/explain.sql`

Table of 220,050 rows: 200,000 succeeded, 20,000 queued and ready, 50 running (10 with an expired lease), after `VACUUM (ANALYZE)`.

| Query | Index used | Execution time | Buffers |
|---|---|---|---|
| claim | `idx_jobs_ready` (Index Scan, then `LockRows` with `Limit 1`) | 0.449 ms | 24 shared hits |
| reaper (10 expired jobs) | `idx_jobs_lease` (Index Scan) | 0.220 ms | 194 shared hits |

| Relation | Size |
|---|---|
| `jobs` (table) | 23 MB |
| `jobs_pkey` | 4848 kB |
| `jobs_idempotency_key_key` | 1392 kB |
| `idx_jobs_ready` | 816 kB |
| `idx_jobs_lease` | 16 kB |

Single run on a warm cache (every buffer was a hit). The partial indexes hold only queued and running rows, so the 200,000 finished jobs do not slow the claim or the reaper. The planner estimated one row for the reaper where ten came back; this did not change the plan.

## X4: update cost

Command: `node --env-file=.env --import tsx scripts/hot.ts`

5,000 `echo` jobs on one worker. The script reads `pg_stat_user_tables` before and after the run and reports the difference.

| Jobs | `n_tup_upd` | Updates per job | `n_tup_hot_upd` | HOT ratio |
|---|---|---|---|---|
| 5,000 | 10000 | 2.00 | 0 | 0.0 % |

Every update writes a new row version and none is HOT, because `status` and `lease_expires_at` are indexed. This is a known limit; autovacuum matters at scale.

## X1: database outage (manual)

One worker, six 2 s sleep jobs, `docker compose stop db` while job 2 (id 8) was running, a 9 s outage, then `docker compose start db` (30 October 2026, times UTC).

| Time | Event |
|---|---|
| 16:06:31.2 | Job 8 starts |
| 16:06:31.98 | `pool.idle_client_error` (database stopped); the process stays up |
| 16:06:34.6 | `job.report_error`: the handler finished but the result could not be written (3 retries) |
| 16:06:34.6 to 16:06:38.1 | `claim.error` with retries after 500, 1000, 2000 and 4000 ms |
| 16:06:42.15 | Worker resumes claiming (job 10) about a second after the database returned |
| 16:07:07.08 | `reaper.requeued`: job 8's lease had expired |
| 16:07:07.13 | Job 8 runs again as attempt 2 and succeeds at 16:07:09.13 |
| end | Ctrl+C: `worker.stopping`, `worker.stopped`, exit code 0 |

Final state: six jobs `succeeded`; job 8 has `attempts` 2, the others 1. The worker never crashed.

Job 8's handler had already completed once when the database was unreachable, so its side effect ran twice. This is at-least-once delivery, and why handlers must be idempotent. The log lines for the connection errors show an empty `error` string; the cause is not logged.

## Test suite

84 tests in 4 files passed in three consecutive runs (about 56 s each), and `npm run typecheck` is clean.
