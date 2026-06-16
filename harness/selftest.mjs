// Harness self-test: mock workers + the real run.js CLI against a live daemon.
import { spawn } from "node:child_process";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";

const BASE = "http://127.0.0.1:4123";
let fail = 0;
const assert = (c, m) => { console.log(`${c ? "✅" : "❌"} ${m}`); if (!c) fail++; };
const txt = (r) => r.content.map((x) => x.text).join("\n");
async function connect(name) { const c = new Client({ name, version: "1.0.0" }); await c.connect(new StreamableHTTPClientTransport(new URL(`${BASE}/mcp`))); return c; }

let running = true;
async function worker(name) {
  const c = await connect(name);
  await c.callTool({ name: "register", arguments: { name } });
  await c.callTool({ name: "join", arguments: { channel: "#protocol" } });
  while (running) {
    const res = txt(await c.callTool({ name: "claim_task", arguments: {} }));
    if (res.startsWith("{")) {
      const t = JSON.parse(res);
      await new Promise((r) => setTimeout(r, 100)); // pretend to work
      await c.callTool({ name: "report_task", arguments: { id: t.id, status: "done", result: `${name} handled "${t.title}"` } });
    } else {
      await c.callTool({ name: "wait", arguments: { timeout_seconds: 2 } });
    }
  }
  await c.close();
}

// Start the crew.
const crew = ["docs", "firmware", "app"].map(worker);
await new Promise((r) => setTimeout(r, 400));

// Supervisor control endpoints.
const bc = await (await fetch(`${BASE}/api/control/broadcast`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ text: "hello crew" }) })).json();
assert(bc.ok && bc.recipients.length === 3, "POST /api/control/broadcast reaches 3 agents");

// Run the real workflow runner CLI and wait for it to finish.
const code = await new Promise((resolve) => {
  const p = spawn("node", ["harness/run.js", "examples/workflow.json", "--timeout", "30"], { stdio: "inherit" });
  p.on("exit", resolve);
});
assert(code === 0, "run.js exits 0 (all tasks done)");

const state = await (await fetch(`${BASE}/api/state`)).json();
assert(state.task_stats.total === 4, "board has 4 tasks");
assert(state.task_stats.done === 4, "all 4 tasks done");
const review = state.tasks.find((t) => t.title.startsWith("Cross-check"));
assert(review && review.status === "done", "dependent 'review' task completed after its deps");

running = false;
await Promise.allSettled(crew);
console.log(fail ? `\n${fail} FAILED` : "\nALL PASSED");
process.exit(fail ? 1 : 0);
