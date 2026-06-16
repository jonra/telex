#!/usr/bin/env node
// telex-top — a tiny no-dependency terminal UI for watching a telex daemon.
// Shows connected agents (with live LED activity), channels, threads, traffic
// stats, and a scrolling feed of event "blips" as messages fire.
//
// Usage:  node tui.js            (defaults to http://127.0.0.1:4123)
//         TELEX_URL=http://host:port node tui.js
//
// SPDX-License-Identifier: MIT

const BASE = process.env.TELEX_URL || `http://${process.env.TELEX_HOST || "127.0.0.1"}:${process.env.TELEX_PORT || 4123}`;

const C = {
  reset: "\x1b[0m", dim: "\x1b[2m", bold: "\x1b[1m",
  green: "\x1b[32m", amber: "\x1b[33m", blue: "\x1b[34m", cyan: "\x1b[36m",
  red: "\x1b[31m", gray: "\x1b[90m", white: "\x1b[97m",
  bgBlip: "\x1b[42m\x1b[30m",
};
const ESC = {
  altOn: "\x1b[?1049h", altOff: "\x1b[?1049l",
  clear: "\x1b[2J", home: "\x1b[H", hideCur: "\x1b[?25l", showCur: "\x1b[?25h",
};

let state = { agents: [], channels: [], threads: [], stats: { total_messages: 0, connections: 0 }, uptime_seconds: 0, now: "" };
let connected = false;
const feed = [];               // recent event lines
const hot = new Map();         // agent -> timestamp until which its LED is "hot"
const HOT_MS = 700;

function out(s) { process.stdout.write(s); }
function pad(s, n) { s = String(s); return s.length > n ? s.slice(0, n - 1) + "…" : s.padEnd(n); }
function fmtUptime(s) { const h = (s / 3600) | 0, m = ((s % 3600) / 60) | 0, x = s % 60; return (h ? h + "h" : "") + (m ? m + "m" : "") + x + "s"; }
function nowHHMMSS(ts) { return new Date(ts || Date.now()).toLocaleTimeString([], { hour12: false }); }

function led(agent, waiting) {
  const isHot = (hot.get(agent) || 0) > Date.now();
  if (isHot) return C.bgBlip + "◉" + C.reset;
  if (waiting) return C.amber + "◍" + C.reset;
  return C.green + "●" + C.reset;
}

function render() {
  const W = process.stdout.columns || 80;
  const H = process.stdout.rows || 24;
  let lines = [];

  const status = connected ? C.green + "● live" + C.reset : C.red + "● offline" + C.reset;
  lines.push(`${C.bold}${C.white} TELEX-TOP${C.reset}  ${C.dim}${BASE}${C.reset}   ${status}`);
  lines.push(
    `${C.gray} agents ${C.reset}${C.bold}${state.agents.length}${C.reset}  ` +
    `${C.gray}channels ${C.reset}${C.bold}${state.channels.length}${C.reset}  ` +
    `${C.gray}threads ${C.reset}${C.bold}${state.threads.length}${C.reset}  ` +
    `${C.gray}messages ${C.reset}${C.bold}${state.stats.total_messages}${C.reset}  ` +
    `${C.gray}conns ${C.reset}${C.bold}${state.stats.connections}${C.reset}  ` +
    `${C.gray}up ${C.reset}${fmtUptime(state.uptime_seconds)}`
  );
  lines.push(C.gray + "─".repeat(W) + C.reset);

  lines.push(C.bold + " AGENTS" + C.reset);
  if (!state.agents.length) lines.push(C.dim + "   (none connected)" + C.reset);
  for (const a of state.agents) {
    lines.push(`  ${led(a.name, a.waiting)} ${C.cyan}${pad(a.name, 18)}${C.reset} ` +
      `${C.dim}${a.waiting ? "waiting" : "idle " + a.idle_seconds + "s"}${a.pending ? "  " + a.pending + " queued" : ""}${C.reset}`);
  }

  if (state.channels.length) {
    lines.push("");
    lines.push(C.bold + " CHANNELS" + C.reset);
    for (const c of state.channels)
      lines.push(`   ${C.green}${pad(c.name, 16)}${C.reset} ${C.dim}${c.members.join(", ")}${C.reset}`);
  }
  if (state.threads.length) {
    lines.push("");
    lines.push(C.bold + " THREADS" + C.reset);
    for (const t of state.threads)
      lines.push(`   ${C.amber}${pad(t.name, 16)}${C.reset} ${C.dim}${t.count} msg · ${t.participants.join(", ")}${C.reset}`);
  }

  lines.push("");
  lines.push(C.bold + " FEED" + C.reset + C.dim + "  (newest first)" + C.reset);
  const room = H - lines.length - 2;
  for (const f of feed.slice(0, Math.max(0, room))) lines.push("  " + f);

  // Compose frame, clamp to height, draw footer.
  let frame = ESC.home + ESC.clear;
  frame += lines.slice(0, H - 1).map((l) => l + "\x1b[K").join("\r\n");
  frame += "\r\n" + C.dim + " q quit · live SSE feed · polls /api/state" + C.reset + "\x1b[K";
  out(frame);
}

function pushFeed(ev) {
  let line;
  if (ev.type === "message") {
    const thr = ev.thread ? ` ${C.amber}[${ev.thread}]${C.reset}` : "";
    const dst = ev.channel ? C.green + ev.to : C.white + ev.to;
    line = `${C.gray}${nowHHMMSS(ev.ts)}${C.reset} ${C.cyan}${ev.from}${C.reset} ${C.dim}→${C.reset} ${dst}${C.reset}${thr} ${C.dim}·${C.reset} ${pad(ev.text, 60)}`;
    hot.set(ev.from, Date.now() + HOT_MS);
    (ev.recipients || []).forEach((r) => hot.set(r, Date.now() + HOT_MS));
  } else if (ev.type === "join") {
    line = `${C.gray}${nowHHMMSS(ev.ts)}${C.reset} ${C.green}+ joined${C.reset} ${C.cyan}${ev.agent}${C.reset}`;
  } else if (ev.type === "join_channel") {
    line = `${C.gray}${nowHHMMSS(ev.ts)}${C.reset} ${C.cyan}${ev.agent}${C.reset} ${C.dim}joined${C.reset} ${C.green}${ev.channel}${C.reset}`;
  } else return;
  feed.unshift(line);
  if (feed.length > 200) feed.pop();
}

async function poll() {
  try {
    const r = await fetch(`${BASE}/api/state?limit=1`);
    state = await r.json();
    connected = true;
  } catch { connected = false; }
}

async function streamEvents() {
  for (;;) {
    try {
      const res = await fetch(`${BASE}/api/events`, { headers: { Accept: "text/event-stream" } });
      connected = true;
      const reader = res.body.getReader();
      const dec = new TextDecoder();
      let buf = "";
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        buf += dec.decode(value, { stream: true });
        let i;
        while ((i = buf.indexOf("\n\n")) >= 0) {
          const chunk = buf.slice(0, i); buf = buf.slice(i + 2);
          const data = chunk.split("\n").filter((l) => l.startsWith("data:")).map((l) => l.slice(5).trim()).join("");
          if (!data) continue;
          try { const ev = JSON.parse(data); if (ev.type && ev.type !== "hello") { pushFeed(ev); poll(); } } catch {}
        }
      }
    } catch { connected = false; }
    await new Promise((r) => setTimeout(r, 2000)); // reconnect backoff
  }
}

function shutdown() {
  out(ESC.showCur + ESC.altOff);
  if (process.stdin.isTTY) process.stdin.setRawMode(false);
  process.exit(0);
}

// Boot.
out(ESC.altOn + ESC.hideCur + ESC.clear);
if (process.stdin.isTTY) {
  process.stdin.setRawMode(true);
  process.stdin.resume();
  process.stdin.on("data", (d) => { if (d[0] === 0x71 || d[0] === 0x03) shutdown(); }); // q or Ctrl-C
}
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
process.stdout.on("resize", render);

poll().then(render);
setInterval(poll, 3000);
setInterval(render, 150); // smooth LED fade + feed updates
streamEvents();
