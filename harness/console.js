#!/usr/bin/env node
// telex operator console — a human "command desk" for working with sessions at
// a higher level. You register as an operator on the bus, direct-message any
// individual session, broadcast, post tasks, tag sessions with their model /
// subscription / cost, and watch replies stream in live.
//
// Usage:
//   node harness/console.js [--name operator] [--base http://127.0.0.1:4123] [--watch]
//
//   --watch   also print every message on the bus, not just ones addressed to you.
//
// Commands (type at the > prompt):
//   @name text        DM a session by name
//   #channel text     send to a channel
//   * text            broadcast to everyone
//   /to name          set a default target; then bare text goes to it
//   /who              list sessions with their model/account/cost
//   /tag name k=v ..  tag a session (e.g. /tag firmware model=opus account=max cost=high)
//   /task title | role   post a task to the board (role optional)
//   /help   /quit
//
// SPDX-License-Identifier: MIT

import readline from "node:readline";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";

const args = process.argv.slice(2);
const opt = (k, d) => { const i = args.indexOf(k); return i >= 0 ? args[i + 1] : d; };
const BASE = opt("--base", process.env.TELEX_URL || "http://127.0.0.1:4123");
const NAME = opt("--name", "operator");
const WATCH = args.includes("--watch");

const C = { dim: "\x1b[2m", b: "\x1b[1m", cyan: "\x1b[36m", green: "\x1b[32m", amber: "\x1b[33m", red: "\x1b[31m", reset: "\x1b[0m" };
const txt = (r) => r.content.map((x) => x.text).join("\n");

const client = new Client({ name: `telex-console:${NAME}`, version: "1.0.0" });
await client.connect(new StreamableHTTPClientTransport(new URL(`${BASE}/mcp`)));
await client.callTool({ name: "register", arguments: { name: NAME, note: "human operator console" } });

let target = null;
const rl = readline.createInterface({ input: process.stdin, output: process.stdout, prompt: prompt() });
function prompt() { return `${C.cyan}telex${C.reset}${target ? ` ${C.amber}@${target}${C.reset}` : ""}> `; }
function line(s) { process.stdout.write("\r\x1b[K" + s + "\n"); rl.prompt(true); }

console.log(`${C.b}telex operator console${C.reset} — connected to ${BASE} as "${NAME}"${WATCH ? " (watching all traffic)" : ""}`);
console.log(`${C.dim}Type /help for commands. Messages addressed to you appear below.${C.reset}\n`);

// Live incoming via SSE.
(async function stream() {
  for (;;) {
    try {
      const res = await fetch(`${BASE}/api/events`, { headers: { Accept: "text/event-stream" } });
      const reader = res.body.getReader(); const dec = new TextDecoder(); let buf = "";
      for (;;) {
        const { value, done } = await reader.read(); if (done) break;
        buf += dec.decode(value, { stream: true });
        let i; while ((i = buf.indexOf("\n\n")) >= 0) {
          const chunk = buf.slice(0, i); buf = buf.slice(i + 2);
          const data = chunk.split("\n").filter((l) => l.startsWith("data:")).map((l) => l.slice(5).trim()).join("");
          if (!data) continue;
          let ev; try { ev = JSON.parse(data); } catch { continue; }
          if (ev.type !== "message") continue;
          const toMe = ev.to === NAME || (ev.recipients || []).includes(NAME);
          if (ev.from === NAME) continue;
          if (toMe || WATCH) {
            const tag = ev.thread ? ` ${C.amber}[${ev.thread}]${C.reset}` : "";
            const arrow = toMe ? `${C.green}→ you${C.reset}` : `${C.dim}→ ${ev.to}${C.reset}`;
            line(`${C.cyan}${ev.from}${C.reset} ${arrow}${tag}: ${ev.text}`);
          }
        }
      }
    } catch { /* reconnect */ }
    await new Promise((r) => setTimeout(r, 1500));
  }
})();

async function handle(input) {
  const s = input.trim();
  if (!s) return;
  try {
    if (s === "/help") {
      console.log("  @name text | #channel text | * text | /to name | /who | /tag name k=v | /task title | role | /quit");
    } else if (s === "/quit" || s === "/exit") {
      await client.close(); process.exit(0);
    } else if (s === "/who") {
      const who = JSON.parse(txt(await client.callTool({ name: "who", arguments: {} })));
      for (const a of who.agents) {
        const m = a.meta || {};
        const bits = [m.model, m.account, m.cost && `$${m.cost}`].filter(Boolean).join(" · ");
        console.log(`  ${C.cyan}${a.name}${C.reset}${bits ? `  ${C.dim}${bits}${C.reset}` : ""}${a.waiting ? "  (waiting)" : ""}`);
      }
    } else if (s.startsWith("/to ")) {
      target = s.slice(4).trim() || null; rl.setPrompt(prompt());
    } else if (s.startsWith("/tag ")) {
      const [, name, ...kv] = s.split(/\s+/);
      const meta = Object.fromEntries(kv.map((p) => p.split("=")).filter((x) => x.length === 2));
      const r = await fetch(`${BASE}/api/control/tag`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ agent: name, meta }) });
      console.log((await r.json()).ok ? `  tagged ${name}` : `  failed`);
    } else if (s.startsWith("/task ")) {
      const [title, role] = s.slice(6).split("|").map((x) => x.trim());
      await client.callTool({ name: "post_task", arguments: { title, role: role || undefined } });
      console.log(`  posted task "${title}"${role ? ` → ${role}` : ""}`);
    } else if (s.startsWith("@")) {
      const sp = s.indexOf(" "); const to = s.slice(1, sp); const text = s.slice(sp + 1);
      await client.callTool({ name: "send", arguments: { to, text } });
    } else if (s.startsWith("#")) {
      const sp = s.indexOf(" "); const to = s.slice(0, sp); const text = s.slice(sp + 1);
      await client.callTool({ name: "send", arguments: { to, text } });
    } else if (s.startsWith("*")) {
      await client.callTool({ name: "send", arguments: { to: "*", text: s.slice(1).trim() } });
    } else if (target) {
      await client.callTool({ name: "send", arguments: { to: target, text: s } });
    } else {
      console.log(`  ${C.dim}no target — use @name, set /to name, or * to broadcast${C.reset}`);
    }
  } catch (e) { console.log(`  ${C.red}error: ${e.message}${C.reset}`); }
}

rl.prompt();
rl.on("line", async (l) => { await handle(l); rl.prompt(); });
rl.on("close", () => process.exit(0));
