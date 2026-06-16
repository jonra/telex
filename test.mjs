// End-to-end smoke test: two+ MCP clients, threads, guide, plus API & SSE.
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";

const BASE = process.env.TELEX_URL || "http://127.0.0.1:4123";
const ENDPOINT = `${BASE}/mcp`;
let failures = 0;
const assert = (cond, msg) => { console.log(`${cond ? "✅" : "❌"} ${msg}`); if (!cond) failures++; };

async function connect(name) {
  const c = new Client({ name, version: "1.0.0" });
  await c.connect(new StreamableHTTPClientTransport(new URL(ENDPOINT)));
  return c;
}
const txt = (r) => r.content.map((x) => x.text).join("\n");
const call = (c, name, args = {}) => c.callTool({ name, arguments: args });

const a = await connect("firmware");
const b = await connect("app");

assert(txt(await call(a, "telex_guide")).includes("register"), "telex_guide returns the protocol");
await call(a, "register", { name: "firmware" });
assert(txt(await call(b, "register", { name: "app" })).includes("firmware"), "register reports other agents");

// Blocking wait wakes on delivery, with a thread.
const waiting = call(b, "wait", { timeout_seconds: 10 }).then(txt);
await new Promise((r) => setTimeout(r, 150));
await call(a, "send", { to: "app", text: "carry vehicleType?", thread: "ble" });
const got = await waiting;
assert(got.includes("vehicleType") && got.includes('"thread": "ble"'), "wait() received threaded direct message");

// Thread-filtered inbox.
await call(a, "send", { to: "app", text: "in thread X", thread: "x" });
await call(a, "send", { to: "app", text: "no thread" });
const xOnly = txt(await call(b, "inbox", { thread: "x" }));
assert(xOnly.includes("in thread X") && !xOnly.includes("no thread"), "inbox({thread}) filters by thread");
assert(txt(await call(b, "inbox")).includes("no thread"), "remaining message still queued");

// Channel broadcast.
await call(a, "join", { channel: "protocol" });
await call(b, "join", { channel: "protocol" });
const c = await connect("docs"); await call(c, "register", { name: "docs" });
assert(txt(await call(c, "send", { to: "#protocol", text: "spec updated" })).includes("firmware"), "channel broadcast routes to members");

// who() exposes threads.
assert(txt(await call(a, "who")).includes("\"threads\""), "who() lists threads");

// REST API.
const state = await (await fetch(`${BASE}/api/state`)).json();
assert(state.agents.length === 3, "GET /api/state lists 3 agents");
assert(state.stats.total_messages >= 4, "GET /api/state counts messages");
assert(state.threads.some((t) => t.name === "ble"), "GET /api/state lists thread 'ble'");

// SSE: confirm a live message event arrives.
const sse = await fetch(`${BASE}/api/events`);
const reader = sse.body.getReader();
const dec = new TextDecoder();
let sawMessage = false;
const sseDone = (async () => {
  const deadline = Date.now() + 3000;
  let buf = "";
  while (Date.now() < deadline) {
    const { value, done } = await reader.read();
    if (done) break;
    buf += dec.decode(value, { stream: true });
    if (buf.includes('"type":"message"')) { sawMessage = true; break; }
  }
})();
await new Promise((r) => setTimeout(r, 200));
await call(a, "send", { to: "app", text: "sse ping" });
await sseDone;
reader.cancel();
assert(sawMessage, "GET /api/events streams live message events");

await a.close(); await b.close(); await c.close();
console.log(failures ? `\n${failures} FAILED` : "\nALL PASSED");
process.exit(failures ? 1 : 0);
