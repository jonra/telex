// Pattern playground — runs four classic multi-agent coordination patterns on
// telex with mock agents, narrating and asserting each. Start the daemon first:
//   node server.js  (then)  node harness/patterns.mjs
//
// Patterns: 1) Stigmergy (ant swarm)  2) Master-Worker (hierarchy)
//           3) Pipeline (DAG)         4) Contract-Net (auction / cost-bidding)
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";

const BASE = process.env.TELEX_URL || "http://127.0.0.1:4123";
let fail = 0;
const ok = (c, m) => { console.log(`   ${c ? "✅" : "❌"} ${m}`); if (!c) fail++; };
const hdr = (n, t) => console.log(`\n━━ ${n} · ${t} ━━`);
const txt = (r) => r.content.map((x) => x.text).join("\n");
const call = (c, n, a = {}) => c.callTool({ name: n, arguments: a });
async function agent(name, meta = {}) {
  const c = new Client({ name, version: "1.0.0" });
  await c.connect(new StreamableHTTPClientTransport(new URL(`${BASE}/mcp`)));
  await call(c, "register", { name, ...meta });
  return c;
}
const reset = () => fetch(`${BASE}/api/control/reset`, { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" });

// 1 ─ STIGMERGY: ants reinforce competing paths; the strongest (net of inhibit) wins.
async function stigmergy() {
  hdr("1", "Stigmergy — ant swarm picks a path by pheromone strength");
  await reset();
  const ants = await Promise.all(["ant1", "ant2", "ant3"].map((n) => agent(n)));
  // ants 1&2 favour path-a; ant3 explores path-b; ant1 also inhibits path-b (dead end).
  await call(ants[0], "emit_signal", { topic: "path-a", strength: 2 });
  await call(ants[1], "emit_signal", { topic: "path-a", strength: 2 });
  await call(ants[2], "emit_signal", { topic: "path-b", strength: 1 });
  await call(ants[0], "emit_signal", { topic: "path-b", strength: 2, kind: "inhibit" });
  // a forager reads the substrate and commits to the strongest path past threshold θ=2.
  const sigs = JSON.parse(txt(await call(ants[1], "read_signals", {})));
  const top = sigs[0];
  console.log(`   substrate: ${sigs.map((s) => `${s.topic}=${s.net}`).join("  ")}`);
  ok(top.topic === "path-a", "strongest path is path-a (reinforced)");
  ok(sigs.find((s) => s.topic === "path-b").net < top.net, "path-b suppressed by inhibition");
  ok(top.net >= 2, "winner crosses activation threshold θ=2 → forager commits");
  await Promise.all(ants.map((a) => a.close()));
}

// 2 ─ MASTER-WORKER: a lead assigns role-targeted tasks; workers claim & report.
async function masterWorker() {
  hdr("2", "Master-Worker — lead delegates, workers report");
  await reset();
  const lead = await agent("lead");
  const workers = await Promise.all(["w1", "w2", "w3"].map((n) => agent(n)));
  for (const w of ["w1", "w2", "w3"]) await call(lead, "post_task", { title: `job for ${w}`, role: w });
  for (const w of workers) {
    const t = JSON.parse(txt(await call(w, "claim_task", {})));
    await call(w, "report_task", { id: t.id, status: "done", result: "ok" });
  }
  const state = await (await fetch(`${BASE}/api/state`)).json();
  ok(state.task_stats.done === 3, "all 3 delegated tasks done");
  ok(state.tasks.every((t) => t.owner === t.role), "each task done by its assigned worker");
  await Promise.all([lead, ...workers].map((a) => a.close()));
}

// 3 ─ PIPELINE: a dependency chain forces ordered execution (DAG scheduler).
async function pipeline() {
  hdr("3", "Pipeline — dependency chain runs in order");
  await reset();
  const lead = await agent("plead");
  const worker = await agent("pipe");
  const idOf = (r) => Number(txt(r).match(/#(\d+)/)[1]);
  const a = idOf(await call(lead, "post_task", { title: "A extract" }));
  const b = idOf(await call(lead, "post_task", { title: "B transform", deps: [a] }));
  idOf(await call(lead, "post_task", { title: "C load", deps: [b] }));
  const order = [];
  for (let i = 0; i < 3; i++) {
    const res = txt(await call(worker, "claim_task", {}));
    ok(res.startsWith("{"), `step ${i + 1}: a task was claimable`);
    const t = JSON.parse(res);
    order.push(t.title[0]);
    await call(worker, "report_task", { id: t.id, status: "done", result: "ok" });
  }
  ok(order.join("") === "ABC", `executed in dependency order: ${order.join(" → ")}`);
  await Promise.all([lead, worker].map((x) => x.close()));
}

// 4 ─ CONTRACT-NET: auctioneer announces; workers bid by cost; cheapest wins.
async function contractNet() {
  hdr("4", "Contract-Net — auction routes work to the cheapest bidder");
  await reset();
  const auctioneer = await agent("auctioneer");
  // three workers on different subscriptions / cost tiers.
  const bidders = {
    cheap: await agent("cheap", { account: "max-personal", cost: "low" }),
    mid: await agent("mid", { account: "team", cost: "medium" }),
    pricey: await agent("pricey", { account: "api", cost: "high" }),
  };
  const idOf = (r) => Number(txt(r).match(/#(\d+)/)[1]);
  const taskId = idOf(await call(auctioneer, "post_task", { title: "render-job" }));
  // bid strength = inverse cost (cheaper bids harder). topic carries the bidder name.
  const bidStrength = { cheap: 3, mid: 2, pricey: 1 };
  for (const [name, c] of Object.entries(bidders)) {
    await call(c, "emit_signal", { topic: `bid:render:${name}`, strength: bidStrength[name] });
  }
  // auctioneer reads bids and awards to the strongest (cheapest) bidder.
  const bids = JSON.parse(txt(await call(auctioneer, "read_signals", {}))).filter((s) => s.topic.startsWith("bid:render:"));
  const winner = bids[0].topic.split(":").pop();
  console.log(`   bids: ${bids.map((b) => `${b.topic.split(":").pop()}=${b.net}`).join("  ")}`);
  ok(winner === "cheap", `awarded to cheapest bidder: ${winner}`);
  await call(auctioneer, "send", { to: winner, text: `You won; claim #${taskId}.` });
  const t = JSON.parse(txt(await call(bidders[winner], "claim_task", { id: taskId })));
  await call(bidders[winner], "report_task", { id: t.id, status: "done", result: "rendered" });
  const state = await (await fetch(`${BASE}/api/state`)).json();
  ok(state.tasks[0].owner === "cheap" && state.tasks[0].status === "done", "winner executed the task");
  await Promise.all([auctioneer, ...Object.values(bidders)].map((a) => a.close()));
}

await stigmergy();
await masterWorker();
await pipeline();
await contractNet();
await reset();
console.log(fail ? `\n${fail} CHECK(S) FAILED` : "\nALL PATTERNS PASSED");
process.exit(fail ? 1 : 0);
