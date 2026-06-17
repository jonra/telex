#!/usr/bin/env node
// telex — a duplex message broker exposed as a Streamable-HTTP MCP server.
//
// Run one daemon; connect any number of MCP clients (e.g. separate Claude Code
// sessions). They exchange messages directly, by broadcast (*), or by channel
// (#name), optionally grouped into named threads. A read-only web dashboard at
// the root URL shows live agents, channels, threads, and the message log, and
// an SSE stream (/api/events) powers the dashboard and the `telex-top` TUI.
//
// SPDX-License-Identifier: MIT

import express from "express";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { z } from "zod";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { isInitializeRequest } from "@modelcontextprotocol/sdk/types.js";

const PORT = Number(process.env.TELEX_PORT || 4123);
const HOST = process.env.TELEX_HOST || "127.0.0.1";
const HISTORY_MAX = Number(process.env.TELEX_HISTORY || 1000);
const SIGNAL_HALFLIFE_MS = Number(process.env.TELEX_SIGNAL_HALFLIFE || 30000);
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const startedAt = Date.now();

// Basic timestamped console logging.
const ts = () => new Date().toISOString().slice(11, 19);
const log = (...a) => console.error(`[telex ${ts()}]`, ...a);

// Operating guide handed to agents on register / on demand via `telex_guide`.
const GUIDE = `You are connected to telex, a shared message bus for multiple agents.

SETUP (do this once, now):
1. Call register({ name: "<short-unique-name>" }) — e.g. "firmware", "app", "docs".
   Pick a name that reflects your role so others know who they're talking to.
2. Call who() to see who else is connected and what channels/threads exist.
3. If this is a group discussion, call join({ channel: "#<topic>" }). To pull
   specific agents into a channel, call invite({ channel, agents: [...] }).

RECEIVING (keep doing this):
- To collaborate live, loop on wait({ timeout_seconds: 60 }). It BLOCKS until a
  message arrives, then returns all pending messages. After handling them, call
  wait() again. This is the main loop — treat an incoming message as a turn:
  read it, do the work, reply with send(), then wait() again.
- If you only want to check without blocking, call inbox() (optionally
  inbox({ thread }) to pull one conversation at a time).

SENDING:
- Direct:    send({ to: "<agent>", text: "..." })
- Broadcast: send({ to: "*", text: "..." })           // everyone else
- Channel:   send({ to: "#<topic>", text: "..." })     // channel members
- Group a back-and-forth with thread: send({ to, text, thread: "<topic>" }).
  Reuse the same thread label in replies so parallel conversations stay separate.

TASK BOARD (for coordinated work):
- A coordinator posts work with post_task({ title, detail, role?, deps? }).
- Workers loop: claim_task() to take the next task meant for them (or unassigned)
  whose dependencies are done; do it; then report_task({ id, status, result }).
  Reporting "done" automatically unblocks tasks that depend on it.
- tasks() shows the whole board. barrier({ label, parties }) blocks until that
  many agents arrive, to synchronize phases.
- A typical worker main loop: claim_task() → if a task, do it and report_task();
  if none, wait({timeout_seconds}) for a nudge, then try claim_task() again.

ETIQUETTE:
- Always include enough context in a message for the recipient to act without
  guessing — they don't see your screen or history.
- When you finish a unit of work or are blocked, say so explicitly.
- Use who() if you're unsure a recipient is connected before sending.`;

// ---------------------------------------------------------------------------
// Broker: a single in-process hub shared by every connected session.
// ---------------------------------------------------------------------------
const broker = {
  agents: new Map(),     // name -> { queue, waiters, lastSeen, joinedAt }
  channels: new Map(),   // channel -> Set(agentName)
  history: [],           // ring buffer of recent events (read-only observation)
  subscribers: new Set(),// SSE response objects for live events
  tasks: new Map(),      // id -> task (control harness board)
  barriers: new Map(),   // label -> { parties, arrived:Set, waiters:[] }
  signals: new Map(),    // "projecttopic" -> { project, topic, activate, inhibit, updatedAt }
  projects: new Map(),   // name -> { name, phases:[{name,mode}], current, status, createdAt }
  seq: 0,
  taskSeq: 0,

  ensure(name) {
    if (!this.agents.has(name)) {
      this.agents.set(name, { queue: [], waiters: [], lastSeen: Date.now(), joinedAt: Date.now(), meta: {}, project: "default" });
      this.emit({ type: "join", agent: name, ts: new Date().toISOString() });
    }
    return this.agents.get(name);
  },

  setAgentProject(name, project) {
    const a = this.ensure(name);
    if (a.project !== project) {
      a.project = project;
      this.emit({ type: "membership", agent: name, project, ts: new Date().toISOString() });
    }
    return a.project;
  },

  // Wipe state to start clean. Keeps SSE observers (dashboard/TUI) connected and
  // tells them to refresh. By default also clears the roster; pass keepAgents to
  // leave currently-known agents in place (queues are cleared either way).
  reset({ keepAgents = false } = {}) {
    this.tasks.clear();
    this.channels.clear();
    this.barriers.clear();
    this.signals.clear();
    this.projects.clear();
    this.history = [];
    this.seq = 0;
    this.taskSeq = 0;
    if (keepAgents) {
      for (const a of this.agents.values()) a.queue = [];
    } else {
      this.agents.clear();
    }
    this.emit({ type: "reset", ts: new Date().toISOString() });
  },

  // Attach operator metadata (model, account/subscription, cost tier, free note).
  tag(name, meta) {
    const a = this.ensure(name);
    a.meta = { ...a.meta, ...meta };
    this.emit({ type: "tag", agent: name, meta: a.meta, ts: new Date().toISOString() });
    return a.meta;
  },

  // ---- Stigmergic signal substrate ----------------------------------------
  // Signals are pheromone-like marks in a shared medium. They decay over time
  // (evaporation) so stale information fades. "activate" reinforces a path,
  // "inhibit" suppresses it (cross-inhibition). Agents read only the topics
  // they care about (rolling attention) and act on thresholds / net balance.
  _decay(sig, now) {
    const factor = Math.pow(0.5, (now - sig.updatedAt) / SIGNAL_HALFLIFE_MS);
    sig.activate *= factor;
    sig.inhibit *= factor;
    sig.updatedAt = now;
    return sig;
  },

  _sigKey(project, topic) { return `${project}${topic}`; },

  emitSignal(from, project, topic, strength = 1, kind = "activate") {
    const now = Date.now();
    const key = this._sigKey(project, topic);
    const sig = this.signals.get(key) || { project, topic, activate: 0, inhibit: 0, updatedAt: now };
    this._decay(sig, now);
    if (kind === "inhibit") sig.inhibit += strength;
    else sig.activate += strength;
    this.signals.set(key, sig);
    const net = sig.activate - sig.inhibit;
    this.emit({ type: "signal", from, project, topic, kind, activate: +sig.activate.toFixed(3), inhibit: +sig.inhibit.toFixed(3), net: +net.toFixed(3), ts: new Date().toISOString() });
    return { project, topic, activate: sig.activate, inhibit: sig.inhibit, net };
  },

  readSignals(project, topicFilter) {
    const now = Date.now();
    const out = [];
    for (const [key, sig] of this.signals) {
      this._decay(sig, now);
      if (sig.activate < 0.01 && sig.inhibit < 0.01) { this.signals.delete(key); continue; } // prune (evaporated)
      if (project && sig.project !== project) continue;
      if (topicFilter && sig.topic !== topicFilter) continue;
      out.push({ project: sig.project, topic: sig.topic, activate: +sig.activate.toFixed(3), inhibit: +sig.inhibit.toFixed(3), net: +(sig.activate - sig.inhibit).toFixed(3) });
    }
    return out.sort((a, b) => b.net - a.net);
  },

  signalBalance(project, topic) {
    const s = this.signals.get(this._sigKey(project, topic));
    if (!s) return 0;
    this._decay(s, Date.now());
    return +(s.activate - s.inhibit).toFixed(3);
  },

  touch(name) {
    if (name) this.ensure(name).lastSeen = Date.now();
  },

  push(to, msg) {
    const a = this.ensure(to);
    a.queue.push(msg);
    while (a.waiters.length) a.waiters.shift()();
  },

  // Fan an event out to SSE subscribers (dashboard + TUI). Never blocks delivery.
  emit(event) {
    const line = `data: ${JSON.stringify(event)}\n\n`;
    for (const res of this.subscribers) {
      try { res.write(line); } catch { /* dropped subscriber */ }
    }
  },

  record(event) {
    this.history.push(event);
    if (this.history.length > HISTORY_MAX) this.history.shift();
    this.emit({ type: "message", ...event });
  },

  route({ from, to, text, thread }) {
    const ts = new Date().toISOString();
    const id = ++this.seq;
    const channel = to.startsWith("#") ? to : null;
    const t = thread || null;

    let recipients;
    if (to === "*") {
      recipients = [...this.agents.keys()].filter((n) => n !== from);
    } else if (channel) {
      recipients = [...(this.channels.get(to) || [])].filter((n) => n !== from);
    } else {
      recipients = [to];
    }

    recipients.forEach((r) => this.push(r, { id, from, to: r, channel, thread: t, text, ts }));
    this.record({ id, from, to, channel, thread: t, text, ts, recipients });
    return recipients;
  },

  drain(name) {
    const a = this.ensure(name);
    const out = a.queue;
    a.queue = [];
    return out;
  },

  wait(name, timeoutMs) {
    const a = this.ensure(name);
    if (a.queue.length) return Promise.resolve(this.drain(name));
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        a.waiters = a.waiters.filter((w) => w !== waiter);
        resolve(this.drain(name));
      }, timeoutMs);
      const waiter = () => {
        clearTimeout(timer);
        resolve(this.drain(name));
      };
      a.waiters.push(waiter);
    });
  },

  join(name, channel) {
    if (!this.channels.has(channel)) this.channels.set(channel, new Set());
    this.channels.get(channel).add(name);
    this.emit({ type: "join_channel", agent: name, channel, ts: new Date().toISOString() });
  },

  roster() {
    const now = Date.now();
    return [...this.agents.entries()].map(([name, a]) => ({
      name,
      pending: a.queue.length,
      waiting: a.waiters.length > 0,
      idle_seconds: Math.round((now - a.lastSeen) / 1000),
      project: a.project || "default",
      meta: a.meta || {},
    }));
  },

  channelList() {
    return [...this.channels.entries()].map(([name, members]) => ({ name, members: [...members] }));
  },

  threadList() {
    const map = new Map();
    for (const e of this.history) {
      if (!e.thread) continue;
      const t = map.get(e.thread) || { name: e.thread, count: 0, participants: new Set(), last: null };
      t.count++;
      t.participants.add(e.from);
      e.recipients.forEach((r) => t.participants.add(r));
      t.last = e.ts;
      map.set(e.thread, t);
    }
    return [...map.values()].map((t) => ({
      name: t.name, count: t.count, participants: [...t.participants], last: t.last,
    }));
  },

  // ---- Projects: each carries one agent structure (pattern), enforced ------
  // A project has a single mode that governs how its tasks may be claimed.
  // The default project is "default" in "anarchy" mode (anyone, any task).
  MODES: ["anarchy", "hierarchy", "pipeline", "auction", "stigmergy"],

  ensureProject(name = "default") {
    if (!this.projects.has(name)) {
      this.projects.set(name, { name, mode: "anarchy", status: "active", createdAt: new Date().toISOString() });
    }
    return this.projects.get(name);
  },

  createProject(name, mode) {
    const m = this.MODES.includes(mode) ? mode : "anarchy";
    const proj = { name, mode: m, status: "active", createdAt: new Date().toISOString() };
    this.projects.set(name, proj);
    this.emit({ type: "project", action: "create", project: this.projectInfo(proj), ts: proj.createdAt });
    log(`◆ project "${name}" created with "${m}" structure`);
    return proj;
  },

  // Claim rules per pattern. anarchy: anyone (respecting role addressing).
  // hierarchy: must be the named role. auction: must have been awarded.
  // pipeline: ordering comes from task deps. stigmergy: anyone (signal-driven).
  modeAllows(mode, task, name) {
    if (mode === "hierarchy") return task.role != null && task.role === name;
    if (mode === "auction") return task.awardedTo === name;
    return task.role == null || task.role === name; // anarchy, pipeline, stigmergy
  },

  projectInfo(proj) {
    const tasks = [...this.tasks.values()].filter((t) => t.project === proj.name);
    const stats = { open: 0, blocked: 0, claimed: 0, done: 0, failed: 0, total: tasks.length };
    for (const t of tasks) stats[t.status] = (stats[t.status] || 0) + 1;
    return { name: proj.name, mode: proj.mode, status: proj.status, task_stats: stats };
  },

  projectList() { return [...this.projects.values()].map((p) => this.projectInfo(p)); },

  // ---- Task board (project- and phase-aware) -------------------------------
  depsSatisfied(task) {
    return (task.deps || []).every((id) => this.tasks.get(id)?.status === "done");
  },

  postTask({ from, title, detail, role, deps, project = "default" }) {
    this.ensureProject(project);
    const id = ++this.taskSeq;
    const now = new Date().toISOString();
    const task = {
      id, title, detail: detail || "", role: role || null, deps: (deps || []).map(Number),
      project, awardedTo: null,
      status: "open", owner: null, result: null, from: from || "supervisor",
      createdAt: now, updatedAt: now,
    };
    if (!this.depsSatisfied(task)) task.status = "blocked";
    this.tasks.set(id, task);
    this.emit({ type: "task", action: "post", task, ts: now });
    log(`▸ task #${id} "${title}" [${project}]${task.role ? ` for ${task.role}` : ""}`);
    return task;
  },

  recomputeBlocked() {
    for (const t of this.tasks.values()) {
      if (t.status === "blocked" && this.depsSatisfied(t)) {
        t.status = "open"; t.updatedAt = new Date().toISOString();
        this.emit({ type: "task", action: "unblock", task: t, ts: t.updatedAt });
      }
    }
  },

  awardTask(id, agent) {
    const task = this.tasks.get(Number(id));
    if (!task) throw new Error(`No task #${id}`);
    task.awardedTo = agent; task.updatedAt = new Date().toISOString();
    this.emit({ type: "task", action: "award", task, ts: task.updatedAt });
    log(`⚑ task #${id} awarded to ${agent}`);
    return task;
  },

  claimTask(name, id, project = "default") {
    const proj = this.ensureProject(project);
    const mode = proj.mode;
    let task;
    if (id != null) {
      task = this.tasks.get(Number(id));
      if (!task) throw new Error(`No task #${id}`);
      if (task.project !== project) throw new Error(`Task #${id} is in project "${task.project}", not "${project}"`);
      if (task.status !== "open") throw new Error(`Task #${id} is ${task.status}, not open`);
      if (!this.depsSatisfied(task)) throw new Error(`Task #${id} has unmet dependencies`);
      if (!this.modeAllows(mode, task, name)) throw new Error(`Task #${id} not claimable by ${name} under "${mode}" mode`);
    } else {
      task = [...this.tasks.values()].find(
        (t) => t.project === project && t.status === "open" &&
          this.depsSatisfied(t) && this.modeAllows(mode, t, name)
      );
      if (!task) return null;
    }
    task.status = "claimed"; task.owner = name; task.updatedAt = new Date().toISOString();
    this.setAgentProject(name, project); // membership by participation
    this.emit({ type: "task", action: "claim", task, ts: task.updatedAt });
    log(`◂ task #${task.id} claimed by ${name}`);
    return task;
  },

  reportTask(name, id, status, result) {
    const task = this.tasks.get(Number(id));
    if (!task) throw new Error(`No task #${id}`);
    task.status = status === "failed" ? "failed" : "done";
    task.result = result ?? null;
    task.owner = task.owner || name;
    task.updatedAt = new Date().toISOString();
    this.emit({ type: "task", action: task.status, task, ts: task.updatedAt });
    log(`${task.status === "done" ? "✔" : "✗"} task #${task.id} ${task.status} by ${name}`);
    this.recomputeBlocked();
    return task;
  },

  taskList() { return [...this.tasks.values()]; },

  taskStats() {
    const s = { open: 0, blocked: 0, claimed: 0, done: 0, failed: 0, total: this.tasks.size };
    for (const t of this.tasks.values()) s[t.status] = (s[t.status] || 0) + 1;
    return s;
  },

  barrier(name, label, parties, timeoutMs) {
    let b = this.barriers.get(label);
    if (!b) { b = { parties: parties || 2, arrived: new Set(), waiters: [] }; this.barriers.set(label, b); }
    if (parties) b.parties = parties;
    b.arrived.add(name);
    this.emit({ type: "barrier", label, arrived: b.arrived.size, parties: b.parties, ts: new Date().toISOString() });
    if (b.arrived.size >= b.parties) {
      const n = b.arrived.size;
      b.waiters.forEach((w) => w(n)); b.waiters = [];
      return Promise.resolve({ released: true, arrived: n, parties: b.parties });
    }
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        b.waiters = b.waiters.filter((w) => w !== waiter);
        resolve({ released: false, arrived: b.arrived.size, parties: b.parties });
      }, timeoutMs);
      const waiter = (n) => { clearTimeout(timer); resolve({ released: true, arrived: n, parties: b.parties }); };
      b.waiters.push(waiter);
    });
  },
};

// ---------------------------------------------------------------------------
// Per-session MCP server. `session` carries this connection's identity.
// ---------------------------------------------------------------------------
function buildServer(session) {
  const server = new McpServer({ name: "telex", version: "1.1.0" });

  const requireName = () => {
    if (!session.agent) throw new Error("Not registered. Call `register` with a name first.");
    broker.touch(session.agent);
    return session.agent;
  };

  server.registerTool(
    "register",
    {
      title: "Register identity",
      description: "Claim a name for this connection on the telex network. Required before sending or receiving. Optionally declare your model/account/cost so an operator can route work and balance cost across subscriptions.",
      inputSchema: {
        name: z.string().min(1).max(64).describe("Unique agent name, e.g. 'firmware' or 'app'."),
        model: z.string().max(64).optional().describe("Model you're running, e.g. 'opus', 'sonnet', 'haiku'."),
        account: z.string().max(64).optional().describe("Account/subscription this session bills to, e.g. 'max-personal', 'team-plan'."),
        cost: z.enum(["free", "low", "medium", "high"]).optional().describe("Relative cost tier of this session."),
        note: z.string().max(200).optional().describe("Anything else an operator should know."),
        project: z.string().optional().describe("Project this session is working on (default 'default')."),
      },
    },
    async ({ name, model, account, cost, note, project }) => {
      const renamedFrom = session.agent;
      session.agent = name;
      const a = broker.ensure(name);
      a.lastSeen = Date.now();
      if (project) broker.setAgentProject(name, project);
      const meta = Object.fromEntries(Object.entries({ model, account, cost, note }).filter(([, v]) => v != null));
      if (Object.keys(meta).length) broker.tag(name, meta);
      log(renamedFrom && renamedFrom !== name
        ? `✎ "${renamedFrom}" renamed to "${name}"`
        : `✓ registered "${name}" (${broker.agents.size} agents)`);
      const others = broker.roster().map((r) => r.name).filter((n) => n !== name);
      return {
        content: [{
          type: "text",
          text:
            `Registered as "${name}". ` +
            (others.length ? `Also connected: ${others.join(", ")}.` : `You're the first one here.`) +
            `\n\nNext: loop on wait({timeout_seconds:60}) to receive messages, and use ` +
            `send({to,text,thread?}) to reply. Call telex_guide() any time for the full protocol.`,
        }],
      };
    }
  );

  server.registerTool(
    "telex_guide",
    {
      title: "How to use telex",
      description: "Return the operating guide: how to register, run the receive loop, send, and use channels/threads. Call this first if you're unsure how telex works.",
      inputSchema: {},
    },
    async () => ({ content: [{ type: "text", text: GUIDE }] })
  );

  server.registerTool(
    "send",
    {
      title: "Send a message",
      description: "Send a message. `to` may be an agent name (direct), '*' (broadcast to all others), or '#channel'. Use `thread` to group a back-and-forth so it can run alongside others.",
      inputSchema: {
        to: z.string().describe("Recipient: agent name, '*', or '#channel'."),
        text: z.string().describe("Message body."),
        thread: z.string().max(64).optional().describe("Optional thread/topic label, e.g. 'ble-protocol'."),
      },
    },
    async ({ to, text, thread }) => {
      const from = requireName();
      const recipients = broker.route({ from, to, text, thread });
      const tag = thread ? ` [thread: ${thread}]` : "";
      return {
        content: [{
          type: "text",
          text: recipients.length ? `Delivered to: ${recipients.join(", ")}${tag}` : `No recipients matched "${to}". Message dropped.`,
        }],
      };
    }
  );

  server.registerTool(
    "inbox",
    {
      title: "Check inbox (non-blocking)",
      description: "Return and clear messages waiting for you now. Optionally filter by `thread`. Does not block.",
      inputSchema: { thread: z.string().optional().describe("Only return messages in this thread; others stay queued.") },
    },
    async ({ thread }) => {
      const name = requireName();
      let msgs;
      if (thread) {
        const a = broker.ensure(name);
        msgs = a.queue.filter((m) => m.thread === thread);
        a.queue = a.queue.filter((m) => m.thread !== thread);
      } else {
        msgs = broker.drain(name);
      }
      return { content: [{ type: "text", text: msgs.length ? JSON.stringify(msgs, null, 2) : "(empty)" }] };
    }
  );

  server.registerTool(
    "wait",
    {
      title: "Wait for messages (blocking)",
      description: "Block until a message arrives for you, then return all pending messages. Returns empty on timeout.",
      inputSchema: { timeout_seconds: z.number().min(1).max(300).optional().describe("Max seconds to wait (default 60).") },
    },
    async ({ timeout_seconds }) => {
      const name = requireName();
      const msgs = await broker.wait(name, (timeout_seconds ?? 60) * 1000);
      return { content: [{ type: "text", text: msgs.length ? JSON.stringify(msgs, null, 2) : "(timed out, no messages)" }] };
    }
  );

  server.registerTool(
    "join",
    {
      title: "Join a channel",
      description: "Join a named channel (e.g. '#protocol') to receive messages broadcast to it.",
      inputSchema: { channel: z.string().describe("Channel name; '#' is added if missing.") },
    },
    async ({ channel }) => {
      const name = requireName();
      const ch = channel.startsWith("#") ? channel : `#${channel}`;
      const isNew = !broker.channels.has(ch);
      broker.join(name, ch);
      log(`# ${name} joined ${ch}${isNew ? " (new channel)" : ""} — ${broker.channels.get(ch).size} member(s)`);
      return { content: [{ type: "text", text: `Joined ${ch}. Members: ${[...(broker.channels.get(ch) || [])].join(", ")}` }] };
    }
  );

  server.registerTool(
    "invite",
    {
      title: "Invite agents to a channel",
      description: "Add one or more agents to a channel (creating it if needed) and notify each of them. Use this to pull specific sessions into a conversation, e.g. invite 'device' and 'app' to '#delivery'. The inviter is added too.",
      inputSchema: {
        channel: z.string().describe("Channel to invite into; '#' is added if missing."),
        agents: z.array(z.string()).min(1).describe("Names of agents to invite."),
        message: z.string().optional().describe("Optional note included in the invitation."),
      },
    },
    async ({ channel, agents, message }) => {
      const from = requireName();
      const ch = channel.startsWith("#") ? channel : `#${channel}`;
      broker.join(from, ch); // inviter joins their own channel
      const invited = [];
      for (const a of agents) {
        if (a === from) continue;
        broker.join(a, ch);
        broker.route({
          from,
          to: a,
          text: `📨 ${from} invited you to ${ch}${message ? `: ${message}` : ""}. You now receive its messages — reply with send({ to: "${ch}", text: "…" }).`,
        });
        invited.push(a);
      }
      log(`📨 ${from} invited ${invited.join(", ")} to ${ch}`);
      return {
        content: [{
          type: "text",
          text: invited.length
            ? `Invited ${invited.join(", ")} to ${ch}. Members: ${[...(broker.channels.get(ch) || [])].join(", ")}`
            : `No one to invite (did you list yourself?).`,
        }],
      };
    }
  );

  server.registerTool(
    "who",
    {
      title: "List the network",
      description: "List connected agents, channels, and active threads.",
      inputSchema: {},
    },
    async () => {
      broker.touch(session.agent);
      return {
        content: [{
          type: "text",
          text: JSON.stringify(
            { you: session.agent || null, agents: broker.roster(), channels: broker.channelList(), threads: broker.threadList() },
            null, 2
          ),
        }],
      };
    }
  );

  // ---- Control harness tools ------------------------------------------------
  server.registerTool(
    "post_task",
    {
      title: "Post a task to the board",
      description: "Add a task to the shared board for a worker to claim. Optionally target a `role` (worker name) and list `deps` (task ids that must finish first). Use this to delegate work.",
      inputSchema: {
        title: z.string().describe("Short task title."),
        detail: z.string().optional().describe("Full instructions for the worker."),
        role: z.string().optional().describe("Restrict to a worker with this name; omit for anyone."),
        deps: z.array(z.number()).optional().describe("Task ids that must be 'done' before this unblocks."),
        project: z.string().optional().describe("Project to post into (default 'default'). The project's mode governs how it can be claimed."),
      },
    },
    async ({ title, detail, role, deps, project }) => {
      const from = requireName();
      const task = broker.postTask({ from, title, detail, role, deps, project });
      return { content: [{ type: "text", text: `Posted task #${task.id} "${task.title}" (${task.status}).` }] };
    }
  );

  server.registerTool(
    "claim_task",
    {
      title: "Claim a task",
      description: "Claim a task to work on. With no id, claims the next open task addressed to you (or unassigned) whose dependencies are met. Returns the task, or notes that none are available.",
      inputSchema: {
        id: z.number().optional().describe("Specific task id; omit to take the next available."),
        project: z.string().optional().describe("Project to claim from (default 'default'). Claiming is enforced by the project's mode."),
      },
    },
    async ({ id, project }) => {
      const name = requireName();
      const task = broker.claimTask(name, id, project ?? "default");
      return {
        content: [{ type: "text", text: task ? JSON.stringify(task, null, 2) : "(no claimable tasks right now)" }],
      };
    }
  );

  server.registerTool(
    "report_task",
    {
      title: "Report task result",
      description: "Mark a task you claimed as done or failed, with an optional result/summary. Completing a task unblocks any tasks that depend on it.",
      inputSchema: {
        id: z.number().describe("Task id."),
        status: z.enum(["done", "failed"]).describe("Outcome."),
        result: z.string().optional().describe("Result, summary, or failure reason."),
      },
    },
    async ({ id, status, result }) => {
      const name = requireName();
      const task = broker.reportTask(name, id, status, result);
      return { content: [{ type: "text", text: `Task #${task.id} marked ${task.status}.` }] };
    }
  );

  server.registerTool(
    "tasks",
    {
      title: "List the task board",
      description: "Show all tasks with their status, owner, and dependencies.",
      inputSchema: {},
    },
    async () => {
      broker.touch(session.agent);
      return { content: [{ type: "text", text: JSON.stringify({ stats: broker.taskStats(), tasks: broker.taskList() }, null, 2) }] };
    }
  );

  server.registerTool(
    "barrier",
    {
      title: "Synchronization barrier",
      description: "Block until `parties` agents have reached the barrier with the same `label`, then all proceed together. Use to synchronize phases of work.",
      inputSchema: {
        label: z.string().describe("Barrier name shared by participants."),
        parties: z.number().min(2).optional().describe("How many agents must arrive (default 2)."),
        timeout_seconds: z.number().min(1).max(600).optional().describe("Max wait (default 120)."),
      },
    },
    async ({ label, parties, timeout_seconds }) => {
      const name = requireName();
      const r = await broker.barrier(name, label, parties, (timeout_seconds ?? 120) * 1000);
      return { content: [{ type: "text", text: r.released ? `Barrier "${label}" released (${r.arrived}/${r.parties}).` : `Barrier "${label}" timed out (${r.arrived}/${r.parties}).` }] };
    }
  );

  // ---- Stigmergic signals (swarm / ant-colony coordination) ----------------
  server.registerTool(
    "emit_signal",
    {
      title: "Emit a signal (pheromone)",
      description: "Leave a decaying mark on a shared topic so the swarm coordinates indirectly. kind 'activate' reinforces a path; 'inhibit' suppresses it. Signals fade over time, so re-emit to keep a path alive.",
      inputSchema: {
        topic: z.string().describe("Signal topic, e.g. 'explore:path-a' or 'error:auth'."),
        strength: z.number().min(0.1).max(100).optional().describe("How strong a mark to leave (default 1)."),
        kind: z.enum(["activate", "inhibit"]).optional().describe("'activate' (default) reinforces; 'inhibit' suppresses."),
        project: z.string().optional().describe("Project to scope the signal to (default 'default')."),
      },
    },
    async ({ topic, strength, kind, project }) => {
      const from = requireName();
      const r = broker.emitSignal(from, project ?? "default", topic, strength ?? 1, kind ?? "activate");
      return { content: [{ type: "text", text: `Signal "${topic}" → activate ${r.activate.toFixed(2)}, inhibit ${r.inhibit.toFixed(2)}, net ${r.net.toFixed(2)}.` }] };
    }
  );

  server.registerTool(
    "read_signals",
    {
      title: "Read signals (rolling attention)",
      description: "Read current decayed signal strengths. Pass a topic to focus on just that one — agents should listen only to topics relevant to their specialization. Use the net value (activate − inhibit) and a threshold to decide whether to act.",
      inputSchema: {
        topic: z.string().optional().describe("Restrict to one topic; omit for all live signals."),
        project: z.string().optional().describe("Project to read from (default 'default')."),
      },
    },
    async ({ topic, project }) => {
      broker.touch(session.agent);
      const sigs = broker.readSignals(project ?? "default", topic);
      return { content: [{ type: "text", text: sigs.length ? JSON.stringify(sigs, null, 2) : "(no live signals)" }] };
    }
  );

  // ---- Projects (each project runs one agent structure / pattern) ----------
  server.registerTool(
    "create_project",
    {
      title: "Create a project",
      description: "Create a project that runs a single agent structure (pattern), strictly enforced for its tasks. Modes: anarchy (anyone claims anything), hierarchy (role-gated), pipeline (dependency-ordered), auction (must be awarded), stigmergy (signal-driven). Default project is 'default' in anarchy.",
      inputSchema: {
        name: z.string().describe("Project name."),
        mode: z.enum(["anarchy", "hierarchy", "pipeline", "auction", "stigmergy"]).describe("The agent structure this project executes under."),
      },
    },
    async ({ name, mode }) => {
      requireName();
      const p = broker.createProject(name, mode);
      return { content: [{ type: "text", text: `Project "${p.name}" created with "${p.mode}" structure. Scope tasks/signals to it with project:"${p.name}".` }] };
    }
  );

  server.registerTool(
    "projects",
    {
      title: "List projects",
      description: "List projects with their agent structure (mode), status, and task stats.",
      inputSchema: {},
    },
    async () => {
      broker.touch(session.agent);
      return { content: [{ type: "text", text: JSON.stringify(broker.projectList(), null, 2) }] };
    }
  );

  server.registerTool(
    "award_task",
    {
      title: "Award a task (auction)",
      description: "In an auction-mode project, award a task to the winning bidder so they can claim it. No-op-ish elsewhere, but required before claim under auction.",
      inputSchema: { id: z.number().describe("Task id."), agent: z.string().describe("Winning agent's name.") },
    },
    async ({ id, agent }) => {
      requireName();
      const t = broker.awardTask(id, agent);
      return { content: [{ type: "text", text: `Task #${t.id} awarded to ${agent}.` }] };
    }
  );

  return server;
}

// ---------------------------------------------------------------------------
// HTTP wiring: MCP endpoint + read-only dashboard API + SSE + static dashboard.
// ---------------------------------------------------------------------------
const app = express();
app.use(express.json());

const transports = {}; // mcp-session-id -> transport

app.post("/mcp", async (req, res) => {
  const sid = req.headers["mcp-session-id"];
  let transport = sid ? transports[sid] : undefined;

  if (!transport && isInitializeRequest(req.body)) {
    const session = { agent: null };
    transport = new StreamableHTTPServerTransport({
      sessionIdGenerator: () => randomUUID(),
      onsessioninitialized: (id) => {
        transports[id] = transport;
        log(`+ connection ${id.slice(0, 8)} (${Object.keys(transports).length} open)`);
      },
    });
    transport.onclose = () => {
      if (transport.sessionId) {
        delete transports[transport.sessionId];
        log(`- connection ${transport.sessionId.slice(0, 8)}${session.agent ? ` ("${session.agent}")` : ""} closed (${Object.keys(transports).length} open)`);
      }
    };
    const server = buildServer(session);
    await server.connect(transport);
  } else if (!transport) {
    // A session id was supplied but we don't know it (e.g. the daemon restarted).
    // Per the MCP spec, answer 404 so the client re-initializes a fresh session
    // instead of treating it as a fatal error. 400 only when no id was given.
    const status = sid ? 404 : 400;
    return res.status(status).json({
      jsonrpc: "2.0",
      error: {
        code: -32001,
        message: sid
          ? "Session expired or unknown (the daemon may have restarted). Re-initialize to get a new session."
          : "No session id. Send an initialize request first.",
      },
      id: null,
    });
  }

  await transport.handleRequest(req, res, req.body);
});

const handleSession = async (req, res) => {
  const sid = req.headers["mcp-session-id"];
  const transport = sid ? transports[sid] : undefined;
  if (!transport) return res.status(404).send("Session expired or unknown — re-initialize.");
  await transport.handleRequest(req, res);
};
app.get("/mcp", handleSession);
app.delete("/mcp", handleSession);

// Read-only observation API consumed by the dashboard and TUI.
app.get("/api/state", (req, res) => {
  const limit = Math.min(Number(req.query.limit) || 200, HISTORY_MAX);
  res.json({
    now: new Date().toISOString(),
    uptime_seconds: Math.round((Date.now() - startedAt) / 1000),
    stats: { total_messages: broker.seq, connections: Object.keys(transports).length },
    agents: broker.roster(),
    channels: broker.channelList(),
    threads: broker.threadList(),
    tasks: broker.taskList(),
    task_stats: broker.taskStats(),
    projects: broker.projectList(),
    signals: broker.readSignals(),
    messages: broker.history.slice(-limit).reverse(),
  });
});

// Supervisor controls (dashboard → bus). Acts as the pseudo-agent "supervisor".
app.post("/api/control/broadcast", (req, res) => {
  const { text, to } = req.body || {};
  if (!text) return res.status(400).json({ error: "text required" });
  const recipients = broker.route({ from: "supervisor", to: to || "*", text });
  log(`⌘ supervisor → ${to || "*"}: ${String(text).slice(0, 60)}`);
  res.json({ ok: true, recipients });
});

app.post("/api/control/assign", (req, res) => {
  const { agent, project } = req.body || {};
  if (!agent || !project) return res.status(400).json({ error: "agent and project required" });
  broker.ensureProject(project);
  const p = broker.setAgentProject(agent, project);
  log(`⌘ supervisor assigned ${agent} → ${p}`);
  res.json({ ok: true, agent, project: p });
});

app.post("/api/control/project", (req, res) => {
  const { name, mode } = req.body || {};
  if (!name) return res.status(400).json({ error: "name required" });
  const p = broker.createProject(name, mode || "anarchy");
  res.json({ ok: true, project: broker.projectInfo(p) });
});

app.post("/api/control/reset", (req, res) => {
  const keepAgents = !!(req.body && req.body.keepAgents);
  broker.reset({ keepAgents });
  log(`⌘ supervisor reset the network${keepAgents ? " (kept agents)" : ""}`);
  res.json({ ok: true, keepAgents });
});

app.post("/api/control/invite", (req, res) => {
  const { channel, agents, message } = req.body || {};
  if (!channel || !Array.isArray(agents) || !agents.length) return res.status(400).json({ error: "channel and agents[] required" });
  const ch = channel.startsWith("#") ? channel : `#${channel}`;
  for (const a of agents) {
    broker.join(a, ch);
    broker.route({ from: "supervisor", to: a, text: `📨 You were invited to ${ch}${message ? `: ${message}` : ""}. Reply with send({ to: "${ch}", text: "…" }).` });
  }
  log(`⌘ supervisor invited ${agents.join(", ")} to ${ch}`);
  res.json({ ok: true, channel: ch, members: [...(broker.channels.get(ch) || [])] });
});

app.post("/api/control/tag", (req, res) => {
  const { agent, meta } = req.body || {};
  if (!agent || !meta) return res.status(400).json({ error: "agent and meta required" });
  const updated = broker.tag(agent, meta);
  log(`⌘ supervisor tagged ${agent}: ${JSON.stringify(updated)}`);
  res.json({ ok: true, meta: updated });
});

app.post("/api/control/task", (req, res) => {
  const { title, detail, role, deps } = req.body || {};
  if (!title) return res.status(400).json({ error: "title required" });
  const task = broker.postTask({ from: "supervisor", title, detail, role, deps });
  res.json({ ok: true, task });
});

// Live event stream (Server-Sent Events): join, join_channel, message.
app.get("/api/events", (req, res) => {
  res.writeHead(200, {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    Connection: "keep-alive",
  });
  res.write(`retry: 2000\n\n`);
  res.write(`data: ${JSON.stringify({ type: "hello", ts: new Date().toISOString() })}\n\n`);
  broker.subscribers.add(res);
  const ping = setInterval(() => { try { res.write(": ping\n\n"); } catch {} }, 15000);
  req.on("close", () => { clearInterval(ping); broker.subscribers.delete(res); });
});

app.get("/health", (_req, res) => res.json({ ok: true, agents: broker.roster() }));

// Landing page at /, dashboard at /dashboard; other assets served statically.
app.get("/dashboard", (_req, res) => res.sendFile(path.join(__dirname, "public", "dashboard.html")));
app.use(express.static(path.join(__dirname, "public")));

broker.ensureProject("default"); // so the dashboard always shows the default (anarchy) project

const httpServer = app.listen(PORT, HOST, () => {
  console.error(`telex listening on http://${HOST}:${PORT}/mcp`);
  console.error(`telex home:        http://${HOST}:${PORT}/`);
  console.error(`telex dashboard:   http://${HOST}:${PORT}/dashboard`);
});
// Keep idle keep-alive sockets open long enough that blocking `wait` long-polls
// and quiet sessions don't get their connection torn down prematurely.
httpServer.keepAliveTimeout = 65000;
httpServer.headersTimeout = 70000;
httpServer.requestTimeout = 0; // allow long-held `wait` requests
