# Decisions

Why the project looks the way it does: the technology choices, the design rules that matter most, and every place where the code differs from the original plan. Measured numbers are in [RESULTS.md](RESULTS.md); the README has the overview.

## 1. Technology choices

| Choice | Why | Rejected | If challenged |
|---|---|---|---|
| **Node.js 22 + TypeScript (strict)** | Async I/O suits workers that mostly wait on networks; types catch SQL-result mistakes early | Python, Go | "Node has one thread, so a CPU-heavy handler can starve the heartbeat. I tested that case and documented it." |
| **Express 5** | Errors from async handlers reach the error middleware automatically | Fastify, NestJS (more to learn, no benefit here) | "The framework is not the point of this project." |
| **zod 4** | Validates at runtime and gives the TypeScript type from the same schema | Hand-written checks, ajv | "TypeScript types vanish at runtime; request bodies are untrusted input." |
| **pg (node-postgres), plain SQL** | The locking SQL is the project; an ORM would hide it | Prisma, TypeORM, Knex | "I wanted to see exactly what the database does for each step." |
| **PostgreSQL 16** | `SKIP LOCKED`, partial indexes, `CHECK` constraints, JSONB, `make_interval` | MySQL 8 (has `SKIP LOCKED`, fewer of the features used here), SQLite (single writer) | "`SKIP LOCKED` exists since 9.5; 16 is just the current stable." |
| **Docker Compose (database only)** | One command gives everyone the same database | Installing PostgreSQL locally | "Reproducible: a reviewer needs Docker and Node, nothing else." |
| **vitest, real database** | Fast and TypeScript-native; the integration tests hit the real database | Jest, mocking the database | "A mock would not catch a wrong `WHERE` clause or a lock-related bug." |
| **tsx** | Run TypeScript directly in development | ts-node, compiling first | "Dev convenience; production would compile." |
| **No Redis, Kafka or broker** | One system to run; transactions and durability come with the database | Redis, RabbitMQ, SQS | "The README says when I would move away from PostgreSQL." |
| **Own tiny JSON logger** | No dependency; one JSON object per line is easy to grep | pino, winston | "In real code I would use pino; this is about what to log, not the library." |
| **One idempotent SQL file as the migration** | Enough for one table | Flyway, Knex migrations, Prisma Migrate | "I would adopt a migration tool when the schema starts changing." |

Versions in use: Node v22.16.0, PostgreSQL 16.15, Express 5.3.0, pg 8.23.1, zod 4.6.7, vitest 5.0.5, supertest 7.3.1, tsx 4.23.15, TypeScript 7.0.2. The stack is fixed: no other runtime dependency was added.

## 2. Design decisions that carry the project

- **All SQL lives in `src/queue.ts`** (plus the schema in `src/db.ts`). `worker.ts` and `api.ts` call functions and contain no SQL, so one file is the database contract.
- **Every value is a bound parameter.** No SQL is ever built by joining strings.
- **Every state change is one statement, with no explicit transaction.** A single `UPDATE` is atomic, so there is no gap in which two workers can disagree. This also leaves no lock order to get wrong, except in the reaper, which locks in `ORDER BY id` so concurrent reapers cannot deadlock.
- **Ownership in the `WHERE` clause is the fence.** `complete`, `fail`, `heartbeat` and `release` match only `locked_by = me AND status = 'running'`. The guard is ownership, not the clock: a late heartbeat still works until the reaper has actually taken the job.
- **The claim is a CTE with `FOR UPDATE SKIP LOCKED`.** Choosing and marking happen in one statement; a claimer moves on to the next row instead of waiting behind a locked one.
- **Attempts are counted at claim time.** A job that kills its worker never completes, so counting at completion would let it crash workers forever. A graceful `release` gives the attempt back, because a deploy is not the job's fault.
- **The database clock decides every time question** (`now()` inside SQL: leases, backoff, queue lag, due time). The worker uses only `performance.now()` for its own stopwatch, so machines with different clocks and clock jumps have no effect.
- **Backoff is computed inside the `fail` statement:** `LEAST(base x 2^attempts, max) + random() x jitter` seconds. Because `attempts` already includes the current attempt, the first retry waits `base x 2`, not `base`.
- **The handler and the report are two separate steps.** An error while reporting never changes the verdict of the run. The report gets one try plus three retries (after 200, 400 and 800 ms); if all fail, the lease expires and the job runs again.
- **The heartbeat checks the worker's stopwatch before it calls the database,** and measures from the moment the request was sent. A hung query or a blocked event loop therefore cannot keep a stale worker believing it still owns a job.
- **The job timeout and the lease are independent.** The heartbeat renews the lease as long as the handler runs; the timeout (an `AbortController` plus `Promise.race`) guards against a handler that runs too long, and the worker moves on even when the handler ignores the signal.
- **Rules in the database:** three `CHECK` constraints and two partial indexes. `running_has_owner` is spelled out in both directions (running needs an owner and a lease; anything else must have neither), because a one-directional version let a queued job carry a stray owner.
- **Idempotency is decided by the unique index,** through `ON CONFLICT DO NOTHING`. If nothing was inserted, a lookup compares type and payload with `jsonb` equality. Same request: `200` with the original job. Different request: `409`.
- **Ids are strings in the API,** because a `bigint` can exceed JavaScript's safe integer range.
- **Pagination is keyset (`id < beforeId`),** not `OFFSET`, so deep pages stay fast and nothing is skipped or repeated when new jobs arrive.
- **Status codes:** `201` versus `200` on create tells the client whether its request created something; a repeated idempotent request is not an error. `409` is for a key reused with a different request. `503`, not `500`, when the database is down or silent, because it means "try again later". Database errors are classified by code and by message, since a silent database makes the driver throw errors that have a message and no code.
- **Reject, do not guess.** Unknown fields, timestamps without a timezone, and numbers sent as strings are `400`.
- **The pool has an `error` listener, a client-side `query_timeout` and keep-alive.** Without the listener a dropped idle connection kills the process; without the timeout a silent database hangs every call.
- **Problems are logged at `warn` or `error`.** Fencing events are exactly the ones to see when the log level is raised.
- **READ COMMITTED is enough.** Each statement is self-contained, and row locks, not snapshot isolation, decide who wins.

## 3. Differences from the original plan

These are choices made while building. Each one is a deliberate change or a clarification, not an accident.

1. **`ping()` and `recordExecution()` are in `src/queue.ts`.** The health check needs a trivial query, and the test-only `record` and `crash` handlers need to write a row per execution. To keep the rule that handlers contain no SQL, the handler context carries `workerId` and a `recordExecution` callback that the worker injects, and the SQL sits next to all the other SQL. The plan said handlers know nothing about the queue; they still do not import it, but the context now carries one injected callback. `recordExecution` writes to an `executions` table that exists only in the test database (the tests create it), so the production schema is unchanged.
2. **Test T02 is built in step 4, not step 3.** It checks the defaults of a new job, and that needs `enqueue`. Step 3 covers T01, T18, T19 and T20 only. T02 was not weakened.
3. **Worker tests clean up with `child.kill('SIGKILL')` in step 10.** There was no `stop()` yet. From step 11 the tests stop workers through the portable IPC `stop` message, and `SIGKILL` remains the cleanup of last resort.
4. **Mutation checks are temporary.** For W10, W11, W12 and A18, each test was shown to fail by applying the matching mutation to the code, and the mutation was then reverted. The committed tests are unchanged.
5. **No `passWithNoTests` in `vitest.config.ts`.** It was only useful while no tests existed; a suite that finds no tests must fail.
6. **The quick start includes `cp .env.example .env`,** and the scripts keep `node --env-file=.env`. The `.env` file is git-ignored, so a fresh clone needs the copy.
7. **Configuration rules.** `BACKOFF_BASE_SECONDS`, `BACKOFF_MAX_SECONDS`, `BACKOFF_JITTER_SECONDS` and `SHUTDOWN_GRACE_SECONDS` accept 0 (a base of 0 makes the delay just the jitter, which the tests use). `makePool` takes an optional connection timeout that defaults to 5,000 ms. There is no rule that depends on `NODE_ENV`, so `DATABASE_URL` is not enforced as required in production: it falls back to the local development URL.
8. **API details.**
   - `/metrics` always returns all four statuses, with 0 for an empty one, and `oldestReadyAgeSeconds` is in seconds.
   - `nextBeforeId` is the id of the last job when the page is full, and `null` otherwise.
   - The NUL character is rejected in two layers: a pre-scan of the payload (strings and keys, any depth) gives a clear `400`, and the database error `22P05` is mapped to the same `UNSTORABLE_PAYLOAD` as a safety net.
9. **Connection pool sizes.** A worker's pool holds up to 5 connections and the API's up to 10. The total is workers x 5 plus 10, and it must stay under PostgreSQL's `max_connections`.
10. **Test count.** The suite has 84 tests (queue 32, API 33, worker 15, configuration 4). The queue file has three more than the 29 planned ids: a non-owner `fail` changes nothing, a status filter for listing, and the queue-lag rule for delayed jobs.
11. **Test-only handlers are not accepted by the API.** `record`, `block` and `crash` exist only in a worker started with `TEST_HANDLERS=1`; the API's list of job types leaves them out.

## 4. Limits taken on purpose

These are in the README with their next steps; the short version:

- No authentication, no retention, one queue, one job per worker at a time, no cancellation of a running job.
- At-least-once delivery only; fencing protects the job record, not a stale handler's side effects.
- Every update writes a new row version and none is HOT (2.00 updates per job, 0 % HOT in the measurement), so autovacuum matters at scale.
- The throughput numbers in RESULTS.md come from one laptop with the database and workers sharing its CPUs; they say nothing about other hardware.
