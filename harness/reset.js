#!/usr/bin/env node
// telex reset — wipe the daemon's state (agents, channels, tasks, messages)
// without restarting it. Pass --keep-agents to leave the roster in place.
//
// Usage:  node harness/reset.js [--keep-agents] [--base http://127.0.0.1:4123]
//
// SPDX-License-Identifier: MIT

const args = process.argv.slice(2);
const opt = (k, d) => { const i = args.indexOf(k); return i >= 0 ? args[i + 1] : d; };
const BASE = opt("--base", process.env.TELEX_URL || `http://${process.env.TELEX_HOST || "127.0.0.1"}:${process.env.TELEX_PORT || 4123}`);
const keepAgents = args.includes("--keep-agents");

try {
  const r = await fetch(`${BASE}/api/control/reset`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ keepAgents }),
  });
  const o = await r.json();
  console.log(o.ok ? `telex reset${keepAgents ? " (kept agents)" : ""} on ${BASE}` : `reset failed: ${JSON.stringify(o)}`);
} catch (e) {
  console.error(`could not reach telex at ${BASE} — is the daemon running?  (${e.message})`);
  process.exit(1);
}
