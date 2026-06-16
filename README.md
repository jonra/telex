# telex

> A duplex message broker, exposed as an MCP server, that lets any number of
> agents talk to each other in real time — with a live web dashboard and a
> terminal UI.

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Node](https://img.shields.io/badge/node-%E2%89%A518-339933.svg)](https://nodejs.org)
[![MCP](https://img.shields.io/badge/protocol-MCP-8a63d2.svg)](https://modelcontextprotocol.io)

Run one `telex` daemon, point any number of MCP clients at it (for example two
separate Claude Code sessions), and they can message each other directly, by
broadcast, or over named channels — optionally organised into threads so several
conversations can run at once. A read-only dashboard and a terminal UI let you
watch the whole network live.

```
   Claude Code A ─┐                        ┌─ web dashboard  (http://…/)
   Claude Code B ─┼──▶  telex daemon  ◀──┤
   any MCP client─┘     (broker + SSE)     └─ telex-top TUI
```

## Contents
- [Quick start](#quick-start)
- [Connect agents](#connect-agents)
- [Bootstrap an agent](#bootstrap-an-agent) ← copy-paste prompt
- [Tools](#tools)
- [Control harness](#control-harness)
- [Dashboard](#dashboard)
- [Terminal UI](#terminal-ui-telex-top)
- [HTTP API](#http-api)
- [Configuration](#configuration)
- [How it works](#how-it-works)

## Quick start

```bash
npm install
node server.js
# telex listening on http://127.0.0.1:4123/mcp
# telex dashboard:   http://127.0.0.1:4123/
```

Leave it running. Open `http://127.0.0.1:4123/` for the landing page, then click
**Connect to dashboard** (or go straight to `/dashboard`), and optionally start the TUI
in another terminal:

```bash
node tui.js          # or: npm run tui
```

## Connect agents

In **every** Claude Code session that should join the network:

```bash
claude mcp add --transport http telex http://127.0.0.1:4123/mcp
```

Use `--scope project` to share the registration via `.mcp.json`, or
`--scope user` to make it available across all your projects.

## Bootstrap an agent

telex tells agents how to use it: the `register` response includes next steps,
and `telex_guide()` returns the full protocol at any time. To get an agent going,
paste a prompt like this into the session:

```text
Connect to the telex network and stay available:
1. Call telex_guide to learn the protocol.
2. register as "firmware"   (use a short name describing your role)
3. who   — see who else is connected
4. Then loop: wait for a message, handle it, reply with send, and wait again.
Use the thread "ble-protocol" for our discussion so it stays separate.
```

For the other side, swap the name (`"app"`) and have it open the conversation:

```text
Connect to telex: call telex_guide, register as "app", then
send to "firmware" with thread "ble-protocol":
"Does the BLE message carry vehicleType yet?"  — then wait for the reply.
```

Because `wait` blocks until a message arrives, the exchange feels synchronous:
one side asks, the other wakes, answers, and waits again.

## Tools

| Tool | Description |
|------|-------------|
| `telex_guide()` | Returns the operating guide. Call first if unsure. |
| `register({ name })` | Claim a name on the network. Required before send/receive. |
| `send({ to, text, thread? })` | `to` = agent name (direct), `*` (broadcast), or `#channel`. `thread` groups a conversation. |
| `inbox({ thread? })` | Return and clear waiting messages now. Non-blocking; optional thread filter. |
| `wait({ timeout_seconds? })` | Block until a message arrives, then return all pending (default 60s). |
| `join({ channel })` | Join a channel to receive its broadcasts. |
| `invite({ channel, agents, message? })` | Pull specific agents into a channel and notify them. |
| `who()` | List connected agents, channels, and active threads. |

Sessions can also declare `model`, `account`, and `cost` when they `register`, so
an operator can see what each session runs on and balance work across subscriptions.

### Operator console

Run a two-way command desk to work with sessions at a higher level — DM individuals,
broadcast, post tasks, tag sessions, and watch replies live:

```bash
node harness/console.js            # registers as "operator"
# @device do X   ·   #delivery ship it   ·   * heads up   ·   /who   ·   /tag app cost=high   ·   /task title | role
```

## Control harness

On top of messaging, telex has a coordination layer — a shared **task board**
plus a workflow runner and session launcher — that lets a crew of agents
execute multi-step work with no extra API calls (the sessions you already run do
the thinking). Tools: `post_task`, `claim_task`, `report_task`, `tasks`,
`barrier`. Drive it from `harness/run.js` (workflows) and `harness/launch.js`
(spawn workers), and supervise from the dashboard.

See **[HARNESS.md](HARNESS.md)** for the full guide. Quick taste:

```bash
node server.js                                   # daemon
node harness/launch.js examples/roles.json       # spawn the crew (dry-run first!)
node harness/run.js examples/workflow.json        # drive a dependency-ordered workflow
```

## Dashboard

Open `http://127.0.0.1:4123/dashboard` (the root `/` is a landing page with a
Connect button). It shows, updating live over SSE:

- connected **agents** with activity LEDs (green = idle, amber = blocked in `wait`, blip = just sent/received),
- active **channels** and their members,
- **threads** with message counts (click one to filter),
- a **message log** you can filter by agent, `#channel`, thread, or text.

It is strictly read-only — it observes the bus and never injects messages.

## Terminal UI (`telex-top`)

A dependency-free, full-screen terminal monitor for operational watching:

```bash
node tui.js
TELEX_URL=http://127.0.0.1:4123 node tui.js   # point at a remote daemon
```

It shows live agents with blinking activity LEDs, channels, threads, traffic
counters, and a scrolling feed of event blips as messages, joins, and channel
joins fire. Press `q` to quit.

## HTTP API

Read-only endpoints behind the dashboard and TUI:

| Endpoint | Description |
|----------|-------------|
| `GET /api/state?limit=N` | Snapshot: stats, agents, channels, threads, last N messages. |
| `GET /api/events` | Server-Sent Events stream of `message`, `join`, `join_channel`. |
| `GET /health` | Liveness check with current roster. |
| `POST/GET/DELETE /mcp` | The MCP Streamable-HTTP endpoint for clients. |

## Configuration

| Env var | Default | Description |
|---------|---------|-------------|
| `TELEX_PORT` | `4123` | Port to listen on. |
| `TELEX_HOST` | `127.0.0.1` | Bind address. Localhost only by default. |
| `TELEX_HISTORY` | `1000` | Max messages kept in the in-memory ring buffer. |
| `TELEX_URL` | — | Used by the TUI to locate the daemon. |

## How it works

telex is a single Node process holding an in-memory broker. Each MCP client gets
a stateful Streamable-HTTP session; tool calls route messages between named
agents. `wait` resolves a promise the moment a message is enqueued, so blocked
agents wake instantly. A bounded ring buffer records recent events purely for
observation and fans them out to the dashboard/TUI over SSE — none of which
touches delivery between agents.

State is in-memory by design: it's a message bus, not a database. Everything
resets when the daemon restarts, and it binds to localhost unless you change
`TELEX_HOST` (do not expose it to a network without adding authentication).

## License

[MIT](LICENSE) © 2026 Jon Rasmussen
