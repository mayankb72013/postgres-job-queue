import net from "node:net";
import { TEST_URL } from "./helpers.js";

export type Proxy = {
  port: number;
  // Hold all traffic in both directions, like a cable pulled or a firewall dropping packets.
  freeze(): void;
  // Requests still reach the database, but its answers vanish (a one-way failure).
  dropResponses(): void;
  // Back to normal; whatever was held is delivered, in order.
  heal(): void;
  close(): Promise<void>;
};

type Mode = "pass" | "freeze" | "drop";

// One direction of one connection. A null entry stands for the end of the stream.
class Pipe {
  private held: (Buffer | null)[] = [];

  constructor(private readonly out: net.Socket) {}

  push(chunk: Buffer | null, mode: Mode, dropWhenDropping: boolean): void {
    if (mode === "freeze") this.held.push(chunk);
    else if (mode === "drop" && dropWhenDropping) return;
    else this.send(chunk);
  }

  flush(): void {
    for (const chunk of this.held.splice(0)) this.send(chunk);
  }

  private send(chunk: Buffer | null): void {
    if (this.out.destroyed) return;
    if (chunk === null) this.out.end();
    else this.out.write(chunk);
  }
}

export async function startProxy(targetPort: number): Promise<Proxy> {
  const targetHost = new URL(TEST_URL).hostname;
  let mode: Mode = "pass";
  const sockets = new Set<net.Socket>();
  const pipes = new Set<Pipe>();

  const server = net.createServer((client) => {
    const upstream = net.connect(targetPort, targetHost);
    sockets.add(client);
    sockets.add(upstream);
    // A destroyed peer is expected in these tests; it must not become an unhandled error.
    client.on("error", () => upstream.destroy());
    upstream.on("error", () => client.destroy());
    client.on("close", () => sockets.delete(client));
    upstream.on("close", () => sockets.delete(upstream));

    const up = new Pipe(upstream);
    const down = new Pipe(client);
    pipes.add(up);
    pipes.add(down);
    client.on("data", (chunk) => up.push(chunk, mode, false));
    client.on("end", () => up.push(null, mode, false));
    upstream.on("data", (chunk) => down.push(chunk, mode, true));
    upstream.on("end", () => down.push(null, mode, false));
  });

  await new Promise<void>((resolve) => server.listen(0, resolve));
  const { port } = server.address() as net.AddressInfo;

  return {
    port,
    freeze: () => {
      mode = "freeze";
    },
    dropResponses: () => {
      mode = "drop";
    },
    heal: () => {
      mode = "pass";
      for (const pipe of pipes) pipe.flush();
    },
    close: async () => {
      for (const socket of sockets) socket.destroy();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}
