# Postgres Job Queue

A durable background-job queue built on PostgreSQL, Node.js and TypeScript.
Jobs survive crashes, retry with exponential backoff, and are never claimed by two workers at once.

## Quick start

Requires Node 22 and Docker.

    git clone <repository-url> && cd postgres-job-queue
    npm install
    cp .env.example .env
    npm run db:up        # PostgreSQL 16 in Docker
    npm run migrate      # creates the jobs table
    npm run api          # terminal 1
    npm run worker       # terminal 2 (start a few more terminals for more workers)
    curl -X POST localhost:3000/jobs -H "content-type: application/json" -d '{"type":"echo"}'

Then look at the result:

    curl localhost:3000/jobs/<id>      # the id is in the response of the POST
    curl localhost:3000/metrics

The npm scripts read `.env` through `node --env-file=.env`, so they behave the same on Linux and Windows.
`npm run demo:enqueue -- 20 sleep 3000` posts twenty three-second jobs through the running API.

## What it does

- **Submit** a job with `POST /jobs`: a type, a JSON payload, and optionally a priority, a delayed start (`runAt`), `maxAttempts` and an `idempotencyKey`.
- **Claim safely.** A worker takes one job at a time with `FOR UPDATE SKIP LOCKED`. Highest priority first, then the oldest.
- **Run and report.** A handler runs the job. Success marks it `succeeded`; failure schedules a retry or marks it `dead`.
- **Retry with backoff.** The delay grows exponentially, is capped, and gets random jitter. A `PermanentError` skips the retries.
- **Recover from crashes.** A running job holds a lease that a heartbeat renews. If the worker dies, every worker's reaper puts the job back. Fencing stops a stale worker from overwriting the result.
- **Delayed jobs**, **dead-letter and requeue**: a dead job keeps its last error, and `POST /jobs/:id/requeue` resets it.
- **See what is going on** with `GET /jobs/:id`, `GET /jobs` (filter and paginate), `GET /metrics` and `GET /health`.
- **Shut down politely.** On SIGTERM or Ctrl+C a worker stops claiming, finishes or hands back the current job, and exits 0.
- **Time out** a handler that runs too long.
- **Survive a silent database.** Client-side query timeouts, and a heartbeat that notices a lost lease without needing the database to answer.
- **Operate it**: JSON logs with job id and worker id, and configuration from environment variables, validated at start-up.

## How it works

```
 ┌────────┐   HTTP    ┌─────────────┐    SQL     ┌─────────────────────────────┐
 │ Client │ ────────► │ API process │ ─────────► │ PostgreSQL 16               │
 └────────┘ POST      │ (Express 5) │  INSERT    │   table  jobs               │
            /jobs     └─────────────┘            │   index  idx_jobs_ready     │
                                                 │   index  idx_jobs_lease     │
                                                 └──────▲───────────▲──────────┘
                                                        │ claim     │ heartbeat
                         ┌──────────────────────┐ ──────┘           │ complete / fail
                         │ Worker 1 … Worker N  │ ──────────────────┘ release / reap
                         │  loop · handler      │
                         │  heartbeat · reaper  │
                         └──────────────────────┘
```

The API validates requests and reads or writes rows; it never runs jobs. Each worker claims a job, runs its handler, keeps the lease alive and reports the result; it also runs the reaper in the background. PostgreSQL is the only shared state: the table is the queue.

Key ideas:

- **Atomic claim.** One statement chooses the next job with `FOR UPDATE SKIP LOCKED` and marks it `running`, sets the owner and the lease, and counts the attempt. Two workers cannot take the same row, and a worker never waits behind a locked row.
- **Leases and heartbeats.** The lease is a deadline in the database. A heartbeat pushes it forward while the handler runs.
- **The reaper.** Any worker's reaper moves jobs with an expired lease back to `queued`, or to `dead` when no attempts are left.
- **Fencing.** `complete`, `fail`, `heartbeat` and `release` all say `WHERE locked_by = me AND status = 'running'`. A worker that lost its job gets zero rows and discards its result.
- **Backoff with jitter.** The retry delay is computed inside the SQL statement, from the database clock: `min(BACKOFF_BASE x 2^attempts, BACKOFF_MAX)` plus 0 to `BACKOFF_JITTER` seconds. With the defaults, the first four retries wait about 10, 20, 40 and 80 seconds plus jitter. Jitter keeps many failed jobs from retrying at the same instant.
- **Dead-letter and requeue.** Out of attempts, or a permanent error: the job is `dead` and keeps its last error until someone requeues it.
- **Constraints in the database.** Three `CHECK` constraints make impossible states unrepresentable (a running job without an owner, a finished job without `finished_at`, attempts out of range). Two partial indexes keep the claim and the reaper small however many finished jobs pile up.
- **One clock.** Every time decision uses `now()` inside SQL. The worker uses only a monotonic stopwatch for its own timing, so clock jumps and disagreeing machines do not matter.

States of a job: `queued` → `running` → `succeeded`, or back to `queued` (retry, reaper, release), or `dead`. `requeue` takes `dead` back to `queued`.

## Guarantees (and what it does not promise)

**At-least-once delivery.** A job that was claimed will end up `succeeded` or `dead`, but it can run more than once, so handlers must be idempotent. Exactly-once is not possible: a worker can finish the work and die before it can say so, and from outside that looks the same as dying before the work.

What the queue does promise:

| Promise | How |
|---|---|
| A job is never claimed by two workers at the same time | Atomic claim with `SKIP LOCKED`; checked with 8 concurrent claimers over 100 jobs, and with 4 worker processes over 3,000 jobs |
| A stale worker cannot overwrite the result of the worker that took over | Every state change is guarded by ownership |
| A job whose worker died is run again | Lease, reaper |
| A job that kills every worker ends up `dead` | `attempts` is counted at claim time, not at completion |
| The same idempotency key and request creates one job | Unique index and `ON CONFLICT DO NOTHING`; 50 simultaneous submissions give one row |
| Time never depends on a worker's clock | All time decisions use the database clock |

What it does not promise:

- **Exactly-once execution.** See above.
- **Strict ordering.** Priority first, then oldest, but under concurrency a worker skips a row another worker has locked and takes the next best one.
- **No side effects from a stale worker.** Fencing protects the job record, not the outside world. A stalled worker whose handler was already running may still send its email or call its API. Handlers must tolerate that.
- **No starvation protection.** A constant stream of high-priority jobs can keep low-priority jobs waiting.

How it fails, and what happens:

| Failure | What happens |
|---|---|
| Worker killed (`kill -9`, out-of-memory kill, power loss) | Heartbeats stop, the lease expires, another worker's reaper requeues the job, and it runs again |
| Worker stalled, not dead (blocked event loop, long pause) | The lease expires and another worker takes the job. When the stalled worker wakes up it notices (`job.aborted`) and drops its result; if it still reports, fencing refuses it (`job.result_discarded`) |
| Silent network failure between one worker and the database | Queries time out on the client (`DB_QUERY_TIMEOUT_SECONDS`); the heartbeat notices it has had no good heartbeat for a lease and stops the handler without asking the database; another worker takes over |
| Database restarts or is unreachable for several seconds | The worker stays up, logs `claim.error` and retries with backoff, and the jobs finish after the database returns (experiment X1 below) |
| Handler succeeded but the result could not be written | The report is retried three times, then logged as `job.report_error`; the lease expires and the job runs again |
| The reply to a claim is lost though the database claimed the job | The job sits `running` with nobody working until the lease expires, then it is requeued; one attempt is lost |
| Poison job that kills the worker every time | `dead` after `maxAttempts` crashes |
| Handler ignores the abort signal after a timeout | The worker moves on; the handler keeps running in the background |

After a worker dies, its job is back in the queue between about `LEASE_SECONDS - HEARTBEAT_SECONDS` and `LEASE_SECONDS + 1.2 x REAPER_INTERVAL_SECONDS` seconds later (20 to 42 seconds with the defaults, from the formula, plus up to one poll before it is claimed again). Shorter leases recover faster but need heartbeats that are reliably on time.

## API

| Method and path | Purpose | Success | Errors |
|---|---|---|---|
| `POST /jobs` | Create a job | `201` + `Location` header (new) · `200` (same idempotency key and same request) | `400` validation or bad JSON · `409` key reused for a different request · `413` body too large · `503` database down |
| `GET /jobs/:id` | Fetch one job | `200` | `400` malformed id · `404` |
| `GET /jobs` | List, newest first. Query: `status`, `limit` (1-100, default 20), `beforeId` | `200` `{ jobs, nextBeforeId }` | `400` |
| `POST /jobs/:id/requeue` | Reset a **dead** job to queued | `200` with the job | `400` · `404` · `409` (not dead) |
| `GET /metrics` | Counts per status and queue lag | `200` | `503` |
| `GET /health` | Can the API reach the database? | `200 {"status":"ok"}` | `503` |

Create a job (only `type` is required):

```json
{
  "type": "echo",
  "payload": { "to": "a@example.com" },
  "priority": 0,
  "runAt": "2026-11-01T09:00:00+05:30",
  "maxAttempts": 5,
  "idempotencyKey": "welcome-email-user-42"
}
```

Responses use camelCase (`runAt`, `maxAttempts`, `lastError`, `lockedBy`, `leaseExpiresAt`, `idempotencyKey`, `createdAt`, `updatedAt`, `finishedAt`), and the id is a **string**, because a database `bigint` can exceed what a JavaScript number holds exactly.

Job types: `echo` (does nothing), `sleep` (`payload.ms`), `flaky` (fails with probability `payload.failRate`, default 0.5), `always_fail`, `permanent_fail`. A job type with no handler in the worker that claims it goes `dead` at once, with a clear message.

Validation, all enforced by zod:

| Field | Rule |
|---|---|
| `type` | required; one of the job types above |
| `payload` | a JSON **object**; at most 64 KB for the whole body; must not contain the NUL character `\u0000` |
| `priority` | integer from -100 to 100 |
| `runAt` | ISO 8601 **with a timezone** (`Z` or `+05:30`); a timestamp without a zone is rejected, because the server would otherwise guess |
| `maxAttempts` | integer from 1 to 20 |
| `idempotencyKey` | 1 to 200 characters |
| `:id` in the URL | digits only, at most 18 of them |
| `limit` | integer 1 to 100 |
| `status` filter | `queued`, `running`, `succeeded` or `dead` |
| any other field | rejected with `400`, so a typo such as `maxAttemps` cannot silently fall back to a default; numbers must be JSON numbers (`"priority": "5"` is rejected) |

Two submissions with the same `idempotencyKey` count as the same request when type and payload are equal (key order inside objects does not matter, array order does). The original job wins and is returned with `200`; priority, `runAt` and `maxAttempts` are not part of the comparison.

`GET /jobs` pages with `beforeId`: pass the `nextBeforeId` of the previous page. It is the id of the last job when the page is full, otherwise `null`.

`GET /metrics`:

```json
{ "counts": { "queued": 12, "running": 3, "succeeded": 4821, "dead": 2 }, "oldestReadyAgeSeconds": 0.8 }
```

All four statuses are always present (zero when empty). `oldestReadyAgeSeconds` is the queue lag in seconds: how long the longest-waiting job that is **due now** has been waiting, 0 when nothing is ready. A delayed job that is not due yet does not count.

Every error has the same shape, and unknown routes return it too, with `404`:

```json
{ "error": { "code": "VALIDATION_ERROR", "message": "invalid request body", "details": { } } }
```

Codes: `VALIDATION_ERROR`, `INVALID_JSON`, `UNSTORABLE_PAYLOAD`, `IDEMPOTENCY_CONFLICT` (`details.existingJobId`), `PAYLOAD_TOO_LARGE`, `NOT_FOUND`, `NOT_DEAD`, `DB_UNAVAILABLE`, `INTERNAL`. A database that is down or silent is `503 DB_UNAVAILABLE`, not `500`.

## Configuration

Environment variables, read from `.env` by the npm scripts and validated at start-up; an invalid value stops the process at once with a clear message. `.env.example` lists every variable with its default.

| Variable | Default | Meaning | Rule |
|---|---|---|---|
| `DATABASE_URL` | `postgres://jobqueue:jobqueue@localhost:5432/jobqueue` | Connection string | non-empty |
| `PORT` | 3000 | API port | integer, 0 to 65535 |
| `LEASE_SECONDS` | 30 | How long a claim lasts without a heartbeat | > 0 |
| `HEARTBEAT_SECONDS` | 10 | Heartbeat interval | > 0; `x 3 <= LEASE_SECONDS` |
| `POLL_INTERVAL_MS` | 500 | Wait when the queue is empty (±20 % jitter) | > 0 |
| `REAPER_INTERVAL_SECONDS` | 10 | How often each worker runs the reaper (±20 % jitter) | > 0 |
| `DB_QUERY_TIMEOUT_SECONDS` | 10 | Client-side limit on any single database call | > 0 |
| `JOB_TIMEOUT_SECONDS` | 60 | Longest a handler may run; independent of the lease | > 0 |
| `SHUTDOWN_GRACE_SECONDS` | 20 | Time allowed to finish the current job on SIGTERM | >= 0 |
| `BACKOFF_BASE_SECONDS` | 5 | First retry delay base | >= 0 |
| `BACKOFF_MAX_SECONDS` | 300 | Cap on the delay | >= 0 |
| `BACKOFF_JITTER_SECONDS` | 5 | Random extra delay, 0 to this | >= 0 |
| `PAYLOAD_LIMIT_BYTES` | 65536 | Largest request body | > 0 |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` | enum |

The lease and the job timeout are separate settings. The heartbeat renews the lease for as long as the handler runs, and the timeout is a guard against a handler that runs too long.

`TEST_HANDLERS=1` adds the handlers the tests need (`record`, `block`, `crash`) to a worker. They are not accepted by the API and must not be set in normal use.

Logs are one JSON object per line on stdout, with `ts`, `level` and `event`. Anything that signals a problem or a protection firing is `warn` or `error` (`job.result_discarded`, `job.aborted`, `job.released`, `heartbeat.error`, `claim.error`, `reaper.requeued`, `reaper.error`, `job.report_error`, `pool.idle_client_error`).

Handy SQL when a job looks stuck:

```sql
-- running but the lease expired (reaper has not run, or all workers are down)
SELECT id, locked_by, lease_expires_at FROM jobs WHERE status='running' AND lease_expires_at < now();
-- queued but not due (delayed, or waiting out a backoff)
SELECT id, attempts, run_at - now() AS due_in FROM jobs WHERE status='queued' AND run_at > now();
-- dead jobs and why
SELECT id, type, attempts, last_error, finished_at FROM jobs WHERE status='dead' ORDER BY finished_at DESC LIMIT 20;
```

## Testing

    npm test             # needs the database from `npm run db:up`
    npm run typecheck

`npm test` runs 84 tests against a real PostgreSQL (queue 32, API 33, worker 15, configuration 4); none uses a mocked database. The tests use a separate `jobqueue_test` database and empty it before each test. The test files run one after another because they share it.

The risky behaviour is tested, not just the happy path:

- 100 jobs and 8 concurrent claimers: every job claimed exactly once; a claim skips a row another transaction has locked.
- A worker process killed with `kill -9` in the middle of a job: another worker runs the job again.
- A blocked event loop (a zombie worker): the job is taken over, and the late result is discarded.
- A database that goes silent, using a TCP proxy that can freeze traffic: the worker stops its own handler, another worker takes over, and the API answers `503` instead of hanging.
- A reply to a claim that is lost, and a poison job that kills its worker: the job is recovered, and the poison job ends `dead`.
- Graceful shutdown: finish the job, or release it and give the attempt back; exit code 0.
- The constraints, idempotency under 50 simultaneous submissions, 300 expired jobs under 10 concurrent reapers, and every validation rule of the API.

Mutation checks: for the four tests that prove the silent-database and poison-job protections (W10, W11, W12 and A18) I temporarily broke the matching line of code, watched the test fail, and reverted. The committed tests are unchanged.

The suite passed in three consecutive runs (about 56 s each). One test sends a real SIGTERM and is skipped on Windows; the other shutdown tests use a portable `stop` message.

## Results

Measured by me with the scripts in `scripts/`, on a laptop (AMD Ryzen 5 5600H, 12 logical CPUs, 22.8 GB RAM, Linux 7.0.9-arch1-1), PostgreSQL 16.15 in Docker on the same machine, Node v22.16.0, on 29 to 30 October 2026. The database shares the CPUs with the workers and the script. The full tables, commands and caveats are in [docs/RESULTS.md](docs/RESULTS.md).

**Throughput and duplicates** (`npm run bench`; the handlers do no real work, so this is the queue's own overhead; one run per row):

| Run | Jobs | Worker processes | Time | Throughput | Jobs with `attempts <> 1` | Duplicate executions |
|---|---|---|---|---|---|---|
| `echo` | 5,000 | 1 | 14.5 s | 345 jobs/s | 0 | n/a |
| `echo` | 5,000 | 2 | 8.4 s | 598 jobs/s | 0 | n/a |
| `echo` | 5,000 | 4 | 4.3 s | 1,152 jobs/s | 0 | n/a |
| `record` | 3,000 | 1 | 12.9 s | 232 jobs/s | 0 | 0 |
| `record` | 3,000 | 4 | 3.9 s | 767 jobs/s | 0 | 0 |

No job was executed twice: `record` made 3,000 executions of 3,000 distinct jobs in both runs.

**Query plans** (220,050 rows, 200,000 of them finished, warm cache, single run): the claim used `idx_jobs_ready` and took 0.449 ms; the reaper, with 10 expired jobs, used `idx_jobs_lease` and took 0.220 ms.

**Update cost**: 5,000 jobs caused 10,000 updates (2.00 per job) and none was a HOT update, because `status` and `lease_expires_at` are indexed.

**Database outage** (manual experiment X1: one worker, six 2 s jobs, database stopped for 9 s): the worker never crashed and exited with code 0 on Ctrl+C; all six jobs ended `succeeded`. The job that was running when the database stopped had finished its work but could not report it, so its lease expired and it ran again as attempt 2.

## Design decisions and trade-offs

The full list, with what was rejected and why, is in [docs/DECISIONS.md](docs/DECISIONS.md).

- **PostgreSQL is the queue.** One system to run, and a job can be created in the same transaction as the business data. The price is a throughput ceiling and bloat management.
- **Plain SQL with `pg`, no ORM.** The locking SQL is the project, and all of it lives in `src/queue.ts`.
- **Every state change is one statement, no explicit transactions.** A single `UPDATE` is atomic, and ownership in its `WHERE` clause is the fence.
- **The database clock decides everything.** Backoff, leases and queue lag use `now()` in SQL; workers use only a monotonic stopwatch.
- **Rules live in the database.** `CHECK` constraints and partial indexes, so a bug in the code cannot create an impossible job.
- **Attempts are counted at claim time**, so a job that kills its worker still uses up its attempts. A graceful `release` gives the attempt back.
- **Handler and report are two steps.** A database error while reporting never turns a successful run into a failure.
- **Reject, do not guess.** Unknown fields, timestamps without a zone and numbers sent as strings are errors.

## Limitations and next steps

What is not built, and what is known to be limited. None of this is hidden in the code; each item has an honest answer.

**Not built**

| Missing | Next step |
|---|---|
| Recurring (cron) schedules | A `schedules` table with `next_run_at` and a leader-elected scheduler that inserts jobs |
| Authentication and multi-tenancy | Local development tool. Put it behind an auth gateway and add tenant ids to the table; anyone who can reach the API can enqueue and requeue |
| Several named queues and per-type routing | One queue today; the `type` column is the hook for routing |
| Job results and progress | Jobs have side effects, not return values; results would be a separate table |
| Retention (deleting old finished jobs) | The table grows without bound. A batched `DELETE` of old `succeeded` and `dead` rows, run on a schedule. Deleting a job also deletes its idempotency key, so a key is remembered only as long as its job |
| Rate limits and concurrency limits per type | A counter table or advisory locks |
| Cancelling a running job | A cancel flag the worker checks through the heartbeat |
| Prometheus metrics | `/metrics` is JSON counts; the Prometheus format is a small adapter |
| A dashboard UI | The API plus SQL is enough to operate it |
| A migration framework | One idempotent schema applied by a script; adopt a tool when the schema starts changing |
| Concurrency inside a worker | A worker runs one job at a time. For I/O-heavy handlers, run several jobs per worker, which needs one abort controller per job and careful heartbeat handling |

**Known limitations**

- **At-least-once, not exactly-once.** Handlers must be idempotent.
- **Starvation.** A constant stream of high-priority jobs can make low-priority jobs wait indefinitely. Fixes: priority aging, or separate queues per priority.
- **Priority order is best-effort** with many workers.
- **A handler that ignores its abort signal** cannot be stopped. After the timeout the worker moves on, and the handler keeps running in the background and may still cause side effects.
- **A handler that blocks the event loop** longer than the lease starves the heartbeat; the job is run by someone else, and fencing keeps the record consistent but not the handler's side effects. CPU-heavy work belongs in worker threads or another process.
- **A lost claim reply costs one attempt.** The job waits for the lease to expire.
- **Idempotency lookup window.** If retention deleted the key's row between a conflict and the lookup, the API answers `500` once and a retry succeeds. Fix: retry the insert once.
- **Every update is a new row version, and none is HOT** (measured: 2.00 updates per job, 0 % HOT). Dead row versions accumulate until autovacuum removes them. In production, make autovacuum more aggressive on `jobs` and watch `n_dead_tup`; a long transaction anywhere in the same database blocks vacuum and slows the queue. I did not load-test vacuum behaviour.
- **Two read queries scan the whole table.** `GET /metrics` groups all rows by status, and `GET /jobs?status=dead` can walk the primary key backwards when dead rows are old. Both grow with the table, which is another reason retention matters. I did not measure these two myself. The first fixes would be a partial index on dead jobs, counting the small statuses through the partial indexes, and not counting `succeeded` exactly.
- **Throughput has a ceiling.** Each job costs at least two writes (claim and complete) plus one per heartbeat, each writing a row version, index entries and WAL. One well-sized server handles thousands of jobs per second, not millions. My numbers above are from one laptop with the database and the workers sharing its CPUs.
- **Connections.** Each worker holds up to 5 database connections and the API up to 10; the total must stay under PostgreSQL's `max_connections`. A pooler such as PgBouncer in transaction mode works, because every statement is self-contained.
- **Security.** No authentication. Payloads are stored in plain text and are not written to the logs. `last_error` may contain text from a handler (up to 2,000 characters). One database role does everything locally.
- **Polling.** Idle workers poll about twice a second. `LISTEN/NOTIFY` could wake them at once, with polling kept as a fallback because notifications are not durable.

**When I would move away from PostgreSQL:** tens of thousands of jobs per second sustained (Redis-based queues, RabbitMQ, SQS), a replayable log with many independent consumers (Kafka), or very low-latency delivery (no polling). PostgreSQL is the right answer when the job must be created in the same transaction as the business data, at small and medium scale.

## Project layout

```
postgres-job-queue/
├── docker-compose.yml          # PostgreSQL 16 only (healthcheck, volume, init script)
├── db/init.sql                 # CREATE DATABASE jobqueue_test;
├── .env.example                # every variable with its default
├── package.json
├── tsconfig.json               # strict, ES2022, NodeNext
├── vitest.config.ts            # test files run one after another, 30 s timeouts
├── src/
│   ├── config.ts               # zod env schema + the HEARTBEAT x 3 <= LEASE rule
│   ├── logger.ts               # JSON-lines logger
│   ├── db.ts                   # pool factory (with pool.on('error')), schema, migrate()
│   ├── queue.ts                # all SQL: enqueue, getJob, listJobs, metrics, claim, heartbeat,
│   │                           #   complete, fail, release, reap, requeueDead, ping, recordExecution
│   ├── handlers.ts             # PermanentError, built-in handlers, test-only handlers
│   ├── worker.ts               # Worker class: loop, execute, heartbeat, reaper, shutdown
│   ├── worker-main.ts          # process entry: SIGTERM / SIGINT / IPC 'stop' -> worker.stop()
│   ├── api.ts                  # Express app: routes, zod schemas, error middleware
│   ├── api-main.ts             # process entry (applies the schema at start-up)
│   └── migrate-main.ts         # applies the schema; used by `npm run migrate`
├── tests/
│   ├── helpers.ts              # test pool, resetDb(), startWorker(), stopWorker(), waitFor()
│   ├── proxy.ts                # TCP proxy that can freeze or drop traffic
│   ├── config.test.ts
│   ├── queue.test.ts
│   ├── api.test.ts
│   └── worker.test.ts          # spawns real worker processes
├── scripts/
│   ├── bench.ts                # throughput and duplicate executions (`npm run bench`)
│   ├── load.ts                 # shared by bench.ts and hot.ts
│   ├── hot.ts                  # update cost and HOT ratio
│   ├── explain.sql             # query plans for the claim and the reaper
│   └── enqueue.ts              # posts N jobs through the API (`npm run demo:enqueue`)
├── README.md
└── docs/
    ├── DECISIONS.md            # technology choices and deviations, with reasons
    └── RESULTS.md              # my measurements, with machine and versions
```
