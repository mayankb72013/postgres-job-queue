export type LogLevel = "debug" | "info" | "warn" | "error";

const order: Record<LogLevel, number> = { debug: 10, info: 20, warn: 30, error: 40 };

export type LogFields = Record<string, unknown>;
export type LogMethod = (event: string, fields?: LogFields) => void;
export type Logger = Record<LogLevel, LogMethod>;

export function makeLogger(minLevel: LogLevel = "info"): Logger {
  const emit = (level: LogLevel): LogMethod => (event, fields = {}) => {
    if (order[level] < order[minLevel]) return;
    // Spread first, then reassign, so a caller cannot overwrite the three fixed keys.
    const line = { ts: "", level, event, ...fields };
    line.ts = new Date().toISOString();
    line.level = level;
    line.event = event;
    process.stdout.write(JSON.stringify(line) + "\n");
  };
  return { debug: emit("debug"), info: emit("info"), warn: emit("warn"), error: emit("error") };
}
