-- X3: query plans on a large table. Empties and refills the TEST database (jobqueue_test) only.
-- Run `npm test` once first so the schema exists, then:
--   Linux/macOS: docker compose exec -T db psql -U jobqueue -d jobqueue_test < scripts/explain.sql
--   PowerShell:  Get-Content scripts/explain.sql | docker compose exec -T db psql -U jobqueue -d jobqueue_test
\set ON_ERROR_STOP on
\pset pager off

DO $$
BEGIN
  IF current_database() <> 'jobqueue_test' THEN
    RAISE EXCEPTION 'refusing to run on database %: use jobqueue_test', current_database();
  END IF;
  IF to_regclass('public.jobs') IS NULL THEN
    RAISE EXCEPTION 'the jobs table does not exist: run npm test once first';
  END IF;
END $$;

TRUNCATE jobs RESTART IDENTITY;

-- 200,000 succeeded, 20,000 queued (ready now), 50 running (10 of them with an expired lease).
-- Random order, so the statuses are mixed in the table the way real traffic mixes them.
INSERT INTO jobs (type, status, priority, run_at, attempts, locked_by, lease_expires_at,
                  finished_at, created_at, updated_at)
SELECT 'echo', s,
       CASE WHEN s = 'queued' THEN g % 5 ELSE 0 END,
       now() - g * interval '1 second',
       CASE WHEN s = 'queued' THEN 0 ELSE 1 END,
       CASE WHEN s = 'running' THEN 'explain-worker-' || g END,
       CASE WHEN s = 'running'
            THEN CASE WHEN g % 5 = 0 THEN now() - interval '1 minute' ELSE now() + interval '30 seconds' END
       END,
       CASE WHEN s = 'succeeded' THEN now() - g * interval '1 second' END,
       now() - g * interval '1 second',
       now() - g * interval '1 second'
FROM (
  SELECT g, CASE WHEN g <= 200000 THEN 'succeeded' WHEN g <= 220000 THEN 'queued' ELSE 'running' END AS s
  FROM generate_series(1, 220050) AS g
  ORDER BY random()
) AS t;

VACUUM (ANALYZE) jobs;

SELECT status, count(*) FROM jobs GROUP BY status ORDER BY status;

-- EXPLAIN ANALYZE really runs the statement, so each plan is rolled back.
-- The two statements are copied from claim() and reap() in src/queue.ts, with literal values.
\echo
\echo '=== claim ==='
BEGIN;
EXPLAIN (ANALYZE, BUFFERS)
WITH next AS (
  SELECT id FROM jobs
  WHERE status = 'queued' AND run_at <= now()
  ORDER BY priority DESC, run_at
  FOR UPDATE SKIP LOCKED
  LIMIT 1
)
UPDATE jobs
SET status = 'running',
    locked_by = 'explain-claimer',
    lease_expires_at = now() + make_interval(secs => 30),
    attempts = attempts + 1,
    updated_at = now()
FROM next
WHERE jobs.id = next.id
RETURNING jobs.*;
ROLLBACK;

\echo
\echo '=== reaper ==='
BEGIN;
EXPLAIN (ANALYZE, BUFFERS)
WITH expired AS (
  SELECT id FROM jobs
  WHERE status = 'running' AND lease_expires_at < now()
  ORDER BY id
  FOR UPDATE SKIP LOCKED
  LIMIT 100
)
UPDATE jobs j
SET status = CASE WHEN j.attempts >= j.max_attempts THEN 'dead' ELSE 'queued' END,
    locked_by = NULL, lease_expires_at = NULL,
    last_error = 'lease expired (worker presumed dead)',
    run_at = now(),
    finished_at = CASE WHEN j.attempts >= j.max_attempts THEN now() END,
    updated_at = now()
FROM expired
WHERE j.id = expired.id
RETURNING j.id, j.status;
ROLLBACK;

\echo
\echo '=== sizes ==='
SELECT 'jobs (table)' AS relation, pg_relation_size('jobs') AS bytes,
       pg_size_pretty(pg_relation_size('jobs')) AS size
UNION ALL
SELECT indexname, pg_relation_size(quote_ident(indexname)::regclass),
       pg_size_pretty(pg_relation_size(quote_ident(indexname)::regclass))
FROM pg_indexes
WHERE schemaname = 'public' AND tablename = 'jobs'
ORDER BY relation;

TRUNCATE jobs RESTART IDENTITY;
