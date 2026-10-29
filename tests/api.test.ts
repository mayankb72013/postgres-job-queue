import request from "supertest";
import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { makeApp } from "../src/api.js";
import { loadConfig } from "../src/config.js";
import { makePool } from "../src/db.js";
import { TEST_DB_PORT, TEST_URL, pool, resetDb, row, urlViaPort } from "./helpers.js";
import { startProxy } from "./proxy.js";

const config = loadConfig({ DATABASE_URL: TEST_URL, PAYLOAD_LIMIT_BYTES: 2000, LOG_LEVEL: "error" });
const app = makeApp(pool, config);

beforeEach(resetDb);
afterAll(() => pool.end());

function expectError(res: request.Response, status: number, code: string) {
  expect(res.status).toBe(status);
  expect(res.body.error.code).toBe(code);
  expect(typeof res.body.error.message).toBe("string");
}

describe("POST /jobs", () => {
  it("A01 creates a job with defaults, a Location header and a string id", async () => {
    const res = await request(app).post("/jobs").send({ type: "echo" });
    expect(res.status).toBe(201);
    expect(typeof res.body.id).toBe("string");
    expect(res.headers.location).toBe(`/jobs/${res.body.id}`);
    expect(res.body).toMatchObject({
      type: "echo",
      payload: {},
      status: "queued",
      priority: 0,
      attempts: 0,
      maxAttempts: 5,
      lastError: null,
      lockedBy: null,
      idempotencyKey: null,
      finishedAt: null,
    });
  });

  it("A02 the same key and body twice gives 201 then 200 with the same id", async () => {
    const body = { type: "echo", payload: { a: 1 }, idempotencyKey: "k1" };
    const first = await request(app).post("/jobs").send(body);
    const second = await request(app).post("/jobs").send(body);
    expect(first.status).toBe(201);
    expect(second.status).toBe(200);
    expect(second.body.id).toBe(first.body.id);
  });

  it("A03 the same key with a different payload is a 409 naming the first job", async () => {
    const first = await request(app)
      .post("/jobs")
      .send({ type: "echo", payload: { a: 1 }, idempotencyKey: "k1" });
    const second = await request(app)
      .post("/jobs")
      .send({ type: "echo", payload: { a: 2 }, idempotencyKey: "k1" });
    expectError(second, 409, "IDEMPOTENCY_CONFLICT");
    expect(second.body.error.details.existingJobId).toBe(first.body.id);
  });

  const invalidBodies: [string, unknown][] = [
    ["unknown type", { type: "nope" }],
    ["missing type", { payload: {} }],
    ["priority 101", { type: "echo", priority: 101 }],
    ["priority 1.5", { type: "echo", priority: 1.5 }],
    ["priority as a string", { type: "echo", priority: "5" }],
    ["maxAttempts 0", { type: "echo", maxAttempts: 0 }],
    ["maxAttempts 21", { type: "echo", maxAttempts: 21 }],
    ["payload an array", { type: "echo", payload: [1] }],
    ["payload a string", { type: "echo", payload: "x" }],
    ["runAt without timezone", { type: "echo", runAt: "2026-11-01T09:00:00" }],
    ["runAt date only", { type: "echo", runAt: "2026-11-01" }],
    ["runAt tomorrow", { type: "echo", runAt: "tomorrow" }],
    ["empty key", { type: "echo", idempotencyKey: "" }],
    ["201-character key", { type: "echo", idempotencyKey: "k".repeat(201) }],
    ["unknown field maxAttemps", { type: "echo", maxAttemps: 3 }],
  ];

  it.each(invalidBodies)("A04 rejects %s", async (_name, body) => {
    const res = await request(app).post("/jobs").send(body as object);
    expectError(res, 400, "VALIDATION_ERROR");
  });

  it("A05 malformed JSON is INVALID_JSON", async () => {
    const res = await request(app)
      .post("/jobs")
      .set("Content-Type", "application/json")
      .send('{"type": "echo",');
    expectError(res, 400, "INVALID_JSON");
  });

  it("A06 no body, or the wrong content type, is a 400", async () => {
    const empty = await request(app).post("/jobs");
    expectError(empty, 400, "VALIDATION_ERROR");
    const wrongType = await request(app)
      .post("/jobs")
      .set("Content-Type", "text/plain")
      .send('{"type":"echo"}');
    expectError(wrongType, 400, "VALIDATION_ERROR");
  });

  it("A07 a body above the limit is a 413", async () => {
    const res = await request(app)
      .post("/jobs")
      .send({ type: "echo", payload: { big: "x".repeat(3000) } });
    expectError(res, 413, "PAYLOAD_TOO_LARGE");
  });

  it("A08 runAt with +05:30 is stored as the same instant", async () => {
    const res = await request(app)
      .post("/jobs")
      .send({ type: "echo", runAt: "2030-11-01T09:00:00+05:30" });
    expect(res.status).toBe(201);
    const stored = await row(res.body.id);
    expect(stored.run_at.toISOString()).toBe("2030-11-01T03:30:00.000Z");
  });

  it("A09 unicode and nested payloads come back unchanged", async () => {
    const payload = { name: "Zoë 日本語 🚀", nested: { list: [1, { deep: ["é", null, true] }] } };
    const res = await request(app).post("/jobs").send({ type: "echo", payload });
    expect(res.status).toBe(201);
    expect(res.body.payload).toEqual(payload);
    const fetched = await request(app).get(`/jobs/${res.body.id}`);
    expect(fetched.body.payload).toEqual(payload);
  });

  it("A09b a NUL character in the payload is UNSTORABLE_PAYLOAD", async () => {
    const res = await request(app)
      .post("/jobs")
      .set("Content-Type", "application/json")
      .send('{"type":"echo","payload":{"a":"x\\u0000y"}}');
    expectError(res, 400, "UNSTORABLE_PAYLOAD");
    const count = await pool.query("SELECT count(*)::int AS n FROM jobs");
    expect(count.rows[0].n).toBe(0);
  });
});

describe("GET /jobs/:id", () => {
  it("A10 bad ids are 400 and an unknown id is 404", async () => {
    expectError(await request(app).get("/jobs/abc"), 400, "VALIDATION_ERROR");
    expectError(await request(app).get("/jobs/99999999999999999999"), 400, "VALIDATION_ERROR");
    expectError(await request(app).get("/jobs/123456"), 404, "NOT_FOUND");
  });
});

describe("GET /jobs", () => {
  it("A11 bad list parameters are 400", async () => {
    for (const query of ["limit=0", "limit=101", "limit=abc", "status=paused"]) {
      expectError(await request(app).get(`/jobs?${query}`), 400, "VALIDATION_ERROR");
    }
  });

  it("A12 filters by status and pages through every row once", async () => {
    for (let i = 0; i < 7; i++) await request(app).post("/jobs").send({ type: "echo" });
    await pool.query(
      "UPDATE jobs SET status = 'dead', finished_at = now() WHERE id IN (2, 5)",
    );

    const dead = await request(app).get("/jobs?status=dead");
    expect(dead.status).toBe(200);
    expect(dead.body.jobs.map((j: { id: string }) => j.id)).toEqual(["5", "2"]);
    expect(dead.body.nextBeforeId).toBeNull();

    const seen: string[] = [];
    let beforeId: string | null = null;
    let pages = 0;
    do {
      const url: string = `/jobs?limit=3${beforeId ? `&beforeId=${beforeId}` : ""}`;
      const page = await request(app).get(url);
      expect(page.status).toBe(200);
      seen.push(...page.body.jobs.map((j: { id: string }) => j.id));
      beforeId = page.body.nextBeforeId;
      pages++;
    } while (beforeId && pages < 10);
    expect(seen).toEqual(["7", "6", "5", "4", "3", "2", "1"]);
  });
});

describe("POST /jobs/:id/requeue", () => {
  it("A13 404 for a missing id, 409 NOT_DEAD for a queued job, 200 for a dead one", async () => {
    expectError(await request(app).post("/jobs/999/requeue"), 404, "NOT_FOUND");

    const created = await request(app).post("/jobs").send({ type: "echo" });
    expectError(await request(app).post(`/jobs/${created.body.id}/requeue`), 409, "NOT_DEAD");

    await pool.query(
      "UPDATE jobs SET status = 'dead', attempts = 3, last_error = 'boom', finished_at = now() WHERE id = $1",
      [created.body.id],
    );
    const res = await request(app).post(`/jobs/${created.body.id}/requeue`);
    expect(res.status).toBe(200);
    expect(res.body).toMatchObject({
      id: created.body.id,
      status: "queued",
      attempts: 0,
      lastError: null,
      finishedAt: null,
    });
  });
});

describe("operational endpoints", () => {
  it("A14 /metrics has counts for every status and the queue lag", async () => {
    await request(app).post("/jobs").send({ type: "echo" });
    const res = await request(app).get("/metrics");
    expect(res.status).toBe(200);
    expect(res.body.counts).toEqual({ queued: 1, running: 0, succeeded: 0, dead: 0 });
    expect(typeof res.body.oldestReadyAgeSeconds).toBe("number");
  });

  it("A15 /health is ok and an unknown route is a JSON 404", async () => {
    const health = await request(app).get("/health");
    expect(health.status).toBe(200);
    expect(health.body).toEqual({ status: "ok" });

    const missing = await request(app).get("/nope");
    expectError(missing, 404, "NOT_FOUND");
    expect(missing.headers["content-type"]).toMatch(/json/);
  });

  it("A16 a closed database port gives 503 on /health and DB_UNAVAILABLE on POST /jobs", async () => {
    const url = new URL(TEST_URL);
    url.port = "1";
    const deadPool = makePool(url.toString(), 2, 2_000, 2_000);
    try {
      const deadApp = makeApp(deadPool, config);
      expect((await request(deadApp).get("/health")).status).toBe(503);
      const res = await request(deadApp).post("/jobs").send({ type: "echo" });
      expectError(res, 503, "DB_UNAVAILABLE");
    } finally {
      await deadPool.end();
    }
  });

  it("A18 a silent database gives 503 DB_UNAVAILABLE within 5 s, not a 500 and not a hang", async () => {
    const proxy = await startProxy(TEST_DB_PORT);
    const silentPool = makePool(urlViaPort(proxy.port), 2, 800, 800);
    try {
      const silentApp = makeApp(silentPool, config);
      expect((await request(silentApp).get("/health")).status).toBe(200);

      proxy.freeze();
      const healthStart = performance.now();
      const health = await request(silentApp).get("/health");
      expect(performance.now() - healthStart).toBeLessThan(5_000);
      expectError(health, 503, "DB_UNAVAILABLE");

      const createStart = performance.now();
      const created = await request(silentApp).post("/jobs").send({ type: "echo" });
      expect(performance.now() - createStart).toBeLessThan(5_000);
      expectError(created, 503, "DB_UNAVAILABLE");
    } finally {
      proxy.heal();
      await silentPool.end();
      await proxy.close();
    }
  });
});

describe("injection", () => {
  it("A17 SQL-looking text is stored verbatim and a SQL-looking type is rejected", async () => {
    const evil = "'); DROP TABLE jobs; --";
    const stored = await request(app)
      .post("/jobs")
      .send({ type: "echo", payload: { note: evil }, idempotencyKey: evil });
    expect(stored.status).toBe(201);
    expect(stored.body.payload.note).toBe(evil);
    expect(stored.body.idempotencyKey).toBe(evil);

    expectError(await request(app).post("/jobs").send({ type: evil }), 400, "VALIDATION_ERROR");

    const count = await pool.query("SELECT count(*)::int AS n FROM jobs");
    expect(count.rows[0].n).toBe(1);
  });
});
