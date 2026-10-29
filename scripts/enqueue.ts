// Posts N jobs through the running API (used by the demo).
//   npm run demo:enqueue -- <count> [type] [ms]      e.g. npm run demo:enqueue -- 20 sleep 3000
const CONCURRENCY = 10;

const [countArg, type = "echo", msArg] = process.argv.slice(2);
const count = Number(countArg);
const ms = msArg === undefined ? undefined : Number(msArg);

if (!Number.isInteger(count) || count < 1 || (ms !== undefined && (!Number.isFinite(ms) || ms < 0))) {
  console.error("usage: npm run demo:enqueue -- <count> [type] [ms]");
  process.exit(1);
}

const url = `http://localhost:${process.env.PORT ?? 3000}/jobs`;
const body = JSON.stringify({ type, payload: ms === undefined ? {} : { ms } });

async function post(): Promise<void> {
  const response = await fetch(url, { method: "POST", headers: { "content-type": "application/json" }, body });
  if (response.status !== 201) {
    throw new Error(`POST /jobs returned ${response.status}: ${await response.text()}`);
  }
}

try {
  for (let sent = 0; sent < count; sent += CONCURRENCY) {
    const batch = Math.min(CONCURRENCY, count - sent);
    await Promise.all(Array.from({ length: batch }, post));
  }
} catch (err) {
  // fetch reports an unreachable server as a TypeError; anything else is an HTTP error from the API.
  if (err instanceof TypeError) console.error(`cannot reach ${url}: is "npm run api" running?`);
  else console.error(err instanceof Error ? err.message : String(err));
  process.exit(1);
}

console.log(`enqueued ${count} ${type} job(s) through ${url}`);
