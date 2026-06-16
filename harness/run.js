#!/usr/bin/env node
// telex workflow runner — seeds a workflow of tasks onto the telex board and
// monitors them to completion. The actual work is done by worker agents
// (Claude Code sessions) that claim_task / report_task; this process is the
// "conductor": it posts the tasks (honouring dependencies), kicks the workers
// off, and prints progress until everything is done.
//
// Usage:  node harness/run.js <workflow.json> [--base http://127.0.0.1:4123] [--timeout 600]
//
// SPDX-License-Identifier: MIT

import fs from "node:fs";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";

const args = process.argv.slice(2);
const file = args.find((a) => !a.startsWith("--"));
const opt = (k, d) => { const i = args.indexOf(k); return i >= 0 ? args[i + 1] : d; };
const BASE = opt("--base", process.env.TELEX_URL || "http://127.0.0.1:4123");
const TIMEOUT = Number(opt("--timeout", 600)) * 1000;

if (!file) { console.error("usage: run.js <workflow.json> [--base URL] [--timeout sec]"); process.exit(1); }
const wf = JSON.parse(fs.readFileSync(file, "utf8"));
const txt = (r) => r.content.map((x) => x.text).join("\n");

function topoOrder(tasks) {
  const byId = new Map(tasks.map((t) => [t.id, t]));
  const done = new Set(), order = [];
  let guard = 0;
  while (order.length < tasks.length) {
    if (guard++ > tasks.length + 2) throw new Error("cycle or missing dependency in workflow");
    for (const t of tasks) {
      if (done.has(t.id)) continue;
      if ((t.deps || []).every((d) => done.has(d))) { order.push(t); done.add(t.id); }
    }
  }
  return order;
}

const conductor = new Client({ name: "telex-conductor", version: "1.0.0" });
await conductor.connect(new StreamableHTTPClientTransport(new URL(`${BASE}/mcp`)));
await conductor.callTool({ name: "register", arguments: { name: wf.name ? `conductor:${wf.name}` : "conductor" } });

console.log(`\n▸ seeding workflow "${wf.name || file}" (${wf.tasks.length} tasks) on ${BASE}\n`);
const idMap = new Map(); // local id -> server task id
for (const t of topoOrder(wf.tasks)) {
  const deps = (t.deps || []).map((d) => idMap.get(d)).filter((x) => x != null);
  const res = txt(await conductor.callTool({
    name: "post_task",
    arguments: { title: t.title, detail: t.detail, role: t.role, deps },
  }));
  const sid = Number(res.match(/#(\d+)/)?.[1]);
  idMap.set(t.id, sid);
  console.log(`  posted #${sid}  ${t.title}${t.role ? `  → ${t.role}` : ""}${deps.length ? `  (after ${deps.map((d) => "#" + d).join(",")})` : ""}`);
}

if (wf.kickoff) {
  await conductor.callTool({ name: "send", arguments: { to: "*", text: wf.kickoff } });
  console.log(`\n📣 kickoff broadcast sent`);
}

console.log(`\n⏳ waiting for workers to complete tasks (timeout ${TIMEOUT / 1000}s)…\n`);
const total = idMap.size;
const seen = new Map(); // sid -> status
const deadline = Date.now() + TIMEOUT;

while (Date.now() < deadline) {
  const state = await (await fetch(`${BASE}/api/state`)).json();
  for (const t of state.tasks) {
    if (!idMap.has([...idMap.keys()].find((k) => idMap.get(k) === t.id))) continue;
    if (seen.get(t.id) !== t.status) {
      seen.set(t.id, t.status);
      if (["claimed", "done", "failed"].includes(t.status))
        console.log(`  #${t.id} ${t.status.padEnd(8)} ${t.title}${t.owner ? `  [${t.owner}]` : ""}`);
    }
  }
  const terminal = [...idMap.values()].filter((sid) => ["done", "failed"].includes(seen.get(sid)));
  if (terminal.length >= total) break;
  await new Promise((r) => setTimeout(r, 1000));
}

const state = await (await fetch(`${BASE}/api/state`)).json();
const mine = state.tasks.filter((t) => [...idMap.values()].includes(t.id));
const done = mine.filter((t) => t.status === "done").length;
const failed = mine.filter((t) => t.status === "failed");
console.log(`\n━━ workflow "${wf.name || file}" complete ━━`);
console.log(`   ${done}/${total} done${failed.length ? `, ${failed.length} failed` : ""}`);
for (const t of mine) console.log(`   #${t.id} ${t.status.padEnd(8)} ${t.title}${t.result ? ` — ${String(t.result).slice(0, 80)}` : ""}`);
await conductor.close();
process.exit(failed.length ? 1 : 0);
