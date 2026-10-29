import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { migrate } from "../src/db.js";
import { pool, resetDb } from "./helpers.js";

beforeEach(resetDb);
afterAll(() => pool.end());

async function insertQueued(): Promise<string> {
  const result = await pool.query("INSERT INTO jobs (type) VALUES ('echo') RETURNING id");
  return result.rows[0].id;
}

// 23514 is check_violation; `constraint` names the rule that rejected the change.
function rejectedBy(sql: string, params: unknown[], constraint: string) {
  return expect(pool.query(sql, params)).rejects.toMatchObject({ code: "23514", constraint });
}

describe("schema", () => {
  it("T01 migrate twice does not fail", async () => {
    await migrate(pool);
    await migrate(pool);
  });

  it("T18 a running job needs an owner, and a queued job cannot have one", async () => {
    const id = await insertQueued();
    await rejectedBy("UPDATE jobs SET status = 'running' WHERE id = $1", [id], "running_has_owner");
    await rejectedBy("UPDATE jobs SET locked_by = 'w' WHERE id = $1", [id], "running_has_owner");
  });

  it("T19 finished_at is set exactly when the job is succeeded or dead", async () => {
    const id = await insertQueued();
    await rejectedBy("UPDATE jobs SET status = 'dead' WHERE id = $1", [id], "finished_iff_final");
    await rejectedBy("UPDATE jobs SET finished_at = now() WHERE id = $1", [id], "finished_iff_final");
  });

  it("T20 attempts stay within 0..max_attempts and status is one of four values", async () => {
    const id = await insertQueued();
    await rejectedBy(
      "UPDATE jobs SET max_attempts = 3, attempts = 4 WHERE id = $1",
      [id],
      "attempts_in_range",
    );
    await rejectedBy("UPDATE jobs SET attempts = -1 WHERE id = $1", [id], "attempts_in_range");
    await rejectedBy("UPDATE jobs SET status = 'paused' WHERE id = $1", [id], "jobs_status_check");
  });
});
