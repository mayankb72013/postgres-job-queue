import express, { type ErrorRequestHandler, type Request } from "express";
import type pg from "pg";
import { z } from "zod";
import type { Config } from "./config.js";
import { makeLogger } from "./logger.js";
import {
  IdempotencyConflict,
  enqueue,
  getJob,
  listJobs,
  metrics,
  ping,
  requeueDead,
} from "./queue.js";

// The types a client may submit. The test-only handlers (record, block, crash) are left out on purpose.
const KNOWN_TYPES = ["echo", "sleep", "flaky", "always_fail", "permanent_fail"] as const;
const STATUSES = ["queued", "running", "succeeded", "dead"] as const;

class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
    readonly details: Record<string, unknown> = {},
  ) {
    super(message);
  }
}

const idSchema = z.string().regex(/^\d{1,18}$/, "id must be 1 to 18 digits");

const createSchema = z.strictObject({
  type: z.enum(KNOWN_TYPES),
  payload: z.record(z.string(), z.unknown()).default({}),
  priority: z.number().int().min(-100).max(100).default(0),
  // `offset: true` accepts Z and +05:30; a timestamp without a zone is still rejected.
  runAt: z.iso.datetime({ offset: true }).optional(),
  maxAttempts: z.number().int().min(1).max(20).default(5),
  idempotencyKey: z.string().min(1).max(200).optional(),
});

const listSchema = z.strictObject({
  status: z.enum(STATUSES).optional(),
  limit: z
    .string()
    .regex(/^\d+$/)
    .transform(Number)
    .pipe(z.number().int().min(1).max(100))
    .default(20),
  beforeId: idSchema.optional(),
});

function parse<T extends z.ZodType>(schema: T, value: unknown, what: string): z.output<T> {
  const result = schema.safeParse(value);
  if (result.success) return result.data;
  const issues = result.error.issues.map((i) => ({ path: i.path.join("."), message: i.message }));
  throw new ApiError(400, "VALIDATION_ERROR", `invalid ${what}`, { issues });
}

// jsonb cannot store U+0000, in strings or in keys. Catch it here for a clear 400.
function containsNul(value: unknown): boolean {
  if (typeof value === "string") return value.includes("\u0000");
  if (Array.isArray(value)) return value.some(containsNul);
  if (value !== null && typeof value === "object") {
    return Object.entries(value).some(([k, v]) => k.includes("\u0000") || containsNul(v));
  }
  return false;
}

const UNREACHABLE_CODES = new Set([
  "ECONNREFUSED",
  "ECONNRESET",
  "ENOTFOUND",
  "ETIMEDOUT",
  "EPIPE",
  "EAI_AGAIN",
  "57P01", // admin_shutdown
  "57P02", // crash_shutdown
  "57P03", // cannot_connect_now
  "53300", // too_many_connections
]);

// The driver reports a silent or dropped connection with a message and no code.
const UNREACHABLE_MESSAGES = [
  /query read timeout/i,
  /connection terminated/i,
  /timeout exceeded when trying to connect/i,
  /connection ended unexpectedly/i,
  /the database system is (starting up|shutting down)/i,
];

function errorField(err: unknown, field: "code" | "message" | "type"): string {
  if (typeof err !== "object" || err === null) return "";
  const value = (err as Record<string, unknown>)[field];
  return typeof value === "string" ? value : "";
}

// 08xxx is the SQLSTATE class "connection exception".
function isDbUnavailable(err: unknown): boolean {
  const code = errorField(err, "code");
  if (UNREACHABLE_CODES.has(code) || code.startsWith("08")) return true;
  const message = errorField(err, "message");
  return UNREACHABLE_MESSAGES.some((re) => re.test(message));
}

// 22P05: the database refused a \u0000 that slipped past the pre-scan.
function isUnstorable(err: unknown): boolean {
  return errorField(err, "code") === "22P05";
}

export function makeApp(pool: pg.Pool, config: Config): express.Express {
  const log = makeLogger(config.logLevel);
  const app = express();
  app.disable("x-powered-by");
  app.use(express.json({ limit: config.payloadLimitBytes }));

  app.post("/jobs", async (req, res) => {
    const body = parse(createSchema, req.body, "request body");
    if (containsNul(body.payload)) {
      throw new ApiError(400, "UNSTORABLE_PAYLOAD", "payload contains the NUL character \\u0000");
    }
    const { job, created } = await enqueue(pool, body);
    if (created) res.location(`/jobs/${job.id}`);
    res.status(created ? 201 : 200).json(job);
  });

  app.get("/jobs/:id", async (req, res) => {
    const id = parse(idSchema, req.params.id, "job id");
    const job = await getJob(pool, id);
    if (!job) throw new ApiError(404, "NOT_FOUND", `job ${id} not found`);
    res.json(job);
  });

  app.get("/jobs", async (req: Request, res) => {
    const query = parse(listSchema, req.query, "query string");
    const jobs = await listJobs(pool, query);
    // A full page may have more behind it; a short page is the end.
    const last = jobs[jobs.length - 1];
    const nextBeforeId = last && jobs.length === query.limit ? last.id : null;
    res.json({ jobs, nextBeforeId });
  });

  app.post("/jobs/:id/requeue", async (req, res) => {
    const id = parse(idSchema, req.params.id, "job id");
    const outcome = await requeueDead(pool, id);
    if (outcome === "not_found") throw new ApiError(404, "NOT_FOUND", `job ${id} not found`);
    if (outcome === "not_dead") throw new ApiError(409, "NOT_DEAD", `job ${id} is not dead`);
    const job = await getJob(pool, id);
    if (!job) throw new ApiError(404, "NOT_FOUND", `job ${id} not found`);
    res.json(job);
  });

  app.get("/metrics", async (_req, res) => {
    res.json(await metrics(pool));
  });

  app.get("/health", async (_req, res) => {
    await ping(pool);
    res.json({ status: "ok" });
  });

  app.use((_req, _res, next) => {
    next(new ApiError(404, "NOT_FOUND", "route not found"));
  });

  // Express recognises error middleware by its four parameters; `err` is typed by Express.
  const onError: ErrorRequestHandler = (err, _req, res, _next) => {
    let status = 500;
    let code = "INTERNAL";
    let message = "internal server error";
    let details: Record<string, unknown> = {};

    if (err instanceof ApiError) {
      ({ status, code, message, details } = err);
    } else if (err instanceof IdempotencyConflict) {
      status = 409;
      code = "IDEMPOTENCY_CONFLICT";
      message = err.message;
      details = { existingJobId: err.existingJobId };
    } else if (errorField(err, "type") === "entity.parse.failed") {
      status = 400;
      code = "INVALID_JSON";
      message = "request body is not valid JSON";
    } else if (errorField(err, "type") === "entity.too.large") {
      status = 413;
      code = "PAYLOAD_TOO_LARGE";
      message = `request body is larger than ${config.payloadLimitBytes} bytes`;
    } else if (isUnstorable(err)) {
      status = 400;
      code = "UNSTORABLE_PAYLOAD";
      message = "payload contains a character the database cannot store";
    } else if (isDbUnavailable(err)) {
      status = 503;
      code = "DB_UNAVAILABLE";
      message = "database unavailable, try again later";
      log.error("api.db_unavailable", { error: errorField(err, "message") || errorField(err, "code") });
    } else {
      log.error("api.internal_error", { error: errorField(err, "message") });
    }

    res.status(status).json({ error: { code, message, details } });
  };
  app.use(onError);

  return app;
}
