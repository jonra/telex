#!/usr/bin/env node
// telex session launcher — spawns one Claude Code worker session per role from
// a roles config. Each session is told to connect to telex, register under its
// role name, and run the worker loop (claim_task → do work → report_task). The
// crew then executes whatever the workflow runner posts to the board — with no
// extra API calls beyond the sessions themselves.
//
// Usage:
//   node harness/launch.js [roles.json] [--dry-run] [--base URL] [--only a,b]
//
//   --dry-run   print the command, prompt, and MCP config for each worker, but
//               don't spawn anything. Always start here to see what will run.
//
// Requires the `claude` CLI on PATH (override with CLAUDE_BIN). Logs are written
// to .telex-logs/<name>.log; PIDs to .telex-logs/<name>.pid.
//
// SPDX-License-Identifier: MIT

import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";

const args = process.argv.slice(2);
const file = args.find((a) => !a.startsWith("--")) || "examples/roles.json";
const opt = (k, d) => { const i = args.indexOf(k); return i >= 0 ? args[i + 1] : d; };
const DRY = args.includes("--dry-run");
const cfg = JSON.parse(fs.readFileSync(file, "utf8"));
const BASE = opt("--base", cfg.base || process.env.TELEX_URL || "http://127.0.0.1:4123");
const CHANNEL = cfg.channel || "#work";
const CLAUDE = process.env.CLAUDE_BIN || "claude";
const only = opt("--only", "")?.split(",").filter(Boolean);

const workerPrompt = (w) => `You are the "${w.name}" worker on a telex multi-agent network.
${w.prompt || ""}

Connect and work autonomously:
1. Call telex_guide to learn the protocol.
2. register as "${w.name}", then join("${CHANNEL}").
3. Worker loop:
   - Call claim_task(). If you receive a task, carry out its instructions —
     collaborate with other agents via send() (use the task's thread if given)
     when you need input — then call report_task({ id, status: "done", result }).
   - If claim_task() returns none, call wait({ timeout_seconds: 60 }) and handle
     any messages, then try claim_task() again.
   - Stop only when the board has no open tasks for you and a wait() has timed out
     with nothing pending.
Keep messages concise and post meaningful progress to ${CHANNEL}.`;

const mcpConfig = JSON.stringify({ mcpServers: { telex: { type: "http", url: `${BASE}/mcp` } } });

const workers = cfg.workers.filter((w) => !only?.length || only.includes(w.name));
if (!workers.length) { console.error("no workers selected"); process.exit(1); }

const logDir = path.resolve(".telex-logs");
if (!DRY) fs.mkdirSync(logDir, { recursive: true });

console.log(`\ntelex launcher · ${workers.length} worker(s) · base ${BASE}${DRY ? "  [DRY RUN]" : ""}\n`);

for (const w of workers) {
  const cfgPath = DRY
    ? `<tmp>/telex-${w.name}.json`
    : path.join(os.tmpdir(), `telex-${w.name}-${process.pid}.json`);
  if (!DRY) fs.writeFileSync(cfgPath, mcpConfig);

  const claudeArgs = [
    "--mcp-config", cfgPath,
    "--permission-mode", "acceptEdits",
    "--allowedTools", "mcp__telex__register,mcp__telex__send,mcp__telex__wait,mcp__telex__inbox,mcp__telex__join,mcp__telex__who,mcp__telex__telex_guide,mcp__telex__tasks,mcp__telex__claim_task,mcp__telex__report_task,mcp__telex__barrier",
    ...(w.claudeArgs || []),
    "-p", workerPrompt(w),
  ];

  if (DRY) {
    console.log(`── ${w.name} ──`);
    console.log(`$ ${CLAUDE} ${claudeArgs.map((a) => (a.includes("\n") || a.includes(" ") ? JSON.stringify(a) : a)).join(" ")}`);
    console.log(`  mcp-config: ${mcpConfig}\n`);
    continue;
  }

  const out = fs.openSync(path.join(logDir, `${w.name}.log`), "a");
  const child = spawn(CLAUDE, claudeArgs, { detached: true, stdio: ["ignore", out, out] });
  fs.writeFileSync(path.join(logDir, `${w.name}.pid`), String(child.pid));
  child.unref();
  console.log(`  ▸ ${w.name.padEnd(12)} pid ${child.pid}  → .telex-logs/${w.name}.log`);
}

if (DRY) {
  console.log("Dry run only — nothing spawned. Re-run without --dry-run to launch.");
} else {
  console.log(`\nWorkers launched (detached). Tail logs with:  tail -f .telex-logs/*.log`);
  console.log(`Stop them with:  for f in .telex-logs/*.pid; do kill $(cat "$f"); done`);
}
