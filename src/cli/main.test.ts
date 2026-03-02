import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { createServer } from "node:net";
import { setTimeout as delay } from "node:timers/promises";
import { describe, test } from "node:test";

async function getFreePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => resolve());
  });

  const address = server.address();
  if (!address || typeof address === "string") {
    await new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
    throw new Error("Failed to allocate a free TCP port");
  }

  const { port } = address;
  await new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
  return port;
}

async function waitForHealthy(port: number, child: { exitCode: number | null }, timeoutMs: number): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    if (child.exitCode !== null) {
      return false;
    }

    try {
      const response = await fetch(`http://127.0.0.1:${port}/healthz`);
      if (response.ok) {
        return true;
      }
    } catch {
      // Ignore connection-refused until the server is up.
    }

    await delay(150);
  }

  return false;
}

describe("CLI server runtime", () => {
  test(
    "server command remains alive until SIGTERM and exits gracefully",
    { timeout: 30_000 },
    async () => {
      const port = await getFreePort();
      const npmCommand = process.platform === "win32" ? "npm.cmd" : "npm";
      const child = spawn(npmCommand, ["run", "start"], {
        cwd: process.cwd(),
        env: {
          ...process.env,
          PORT: String(port),
          LOG_LEVEL: "error"
        },
        stdio: ["ignore", "pipe", "pipe"]
      });

      let logs = "";
      child.stdout.on("data", (chunk: Buffer) => {
        logs += chunk.toString("utf8");
      });
      child.stderr.on("data", (chunk: Buffer) => {
        logs += chunk.toString("utf8");
      });

      try {
        const healthy = await waitForHealthy(port, child, 10_000);
        assert.equal(healthy, true, `server never became healthy or exited early\n${logs}`);

        await delay(2_000);
        assert.equal(child.exitCode, null, `server exited unexpectedly after startup\n${logs}`);

        child.kill("SIGTERM");
        const [exitCode, exitSignal] = (await once(child, "exit")) as [number | null, NodeJS.Signals | null];
        assert.equal(exitSignal, null, `expected graceful exit, got signal ${String(exitSignal)}\n${logs}`);
        assert.equal(exitCode, 0, `expected zero exit code on SIGTERM shutdown\n${logs}`);
      } finally {
        if (child.exitCode === null) {
          child.kill("SIGTERM");
          await once(child, "exit");
        }
      }
    }
  );
});
