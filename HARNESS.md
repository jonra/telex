# telex control harness

The harness turns telex from a chat bus into a coordination layer for a crew of
agents — **with no extra API calls**. Each worker is a Claude Code session you're
already running; telex routes their messages and a shared **task board**
coordinates who does what. Three pieces sit on top of the board:

| Piece | What it is | File |
|-------|------------|------|
| Task board | Tools on the daemon: `post_task`, `claim_task`, `report_task`, `tasks`, `barrier`. | `server.js` |
| Workflow runner | A "conductor" that seeds a JSON workflow (with dependencies) and watches it finish. | `harness/run.js` |
| Session launcher | Spawns one Claude Code worker per role, each auto-registering and looping. | `harness/launch.js` |
| Supervisor controls | Dashboard panel to broadcast and post tasks live. | `public/index.html` |

## The model

```
  workflow.json ──▶ run.js (conductor) ──┐
                                          ├─▶  telex task board  ◀── workers claim/report
  roles.json ─────▶ launch.js (workers) ──┘         │
                                                     └─▶ dashboard + TUI watch it live
```

A task has a title, detail, an optional target `role`, and optional `deps`
(other task ids). A task stays `blocked` until every dependency is `done`, then
becomes `open`. Workers `claim_task()` the next open task meant for them, do it,
and `report_task()` — which unblocks dependents automatically.

## Task board tools

- `post_task({ title, detail?, role?, deps? })` — add work to the board.
- `claim_task({ id? })` — take a specific task, or the next open one for you.
- `report_task({ id, status, result? })` — `done` or `failed`; unblocks deps.
- `tasks()` — the whole board with stats.
- `barrier({ label, parties?, timeout_seconds? })` — block until `parties`
  agents reach the same label, then all proceed. Use to gate phases.

A worker's main loop:

```
loop:
  t = claim_task()
  if t:  do the work t.detail describes; report_task({ id: t.id, status: "done", result })
  else:  wait({ timeout_seconds: 60 }); handle any messages; loop
```

## Workflow runner

Define a workflow as JSON (see `examples/workflow.json`):

```json
{
  "name": "ble-protocol",
  "kickoff": "Workers: claim_task() and get going.",
  "tasks": [
    { "id": "spec",     "title": "Draft the schema", "role": "docs" },
    { "id": "firmware", "title": "Map to ESP32",      "role": "firmware", "deps": ["spec"] },
    { "id": "app",      "title": "App encoder",       "role": "app",      "deps": ["spec"] },
    { "id": "review",   "title": "Cross-check",       "role": "docs",     "deps": ["firmware","app"] }
  ]
}
```

Run it (the daemon and your workers must be up):

```bash
node harness/run.js examples/workflow.json
# or: npm run workflow -- examples/workflow.json
```

The runner posts the tasks in dependency order, broadcasts the kickoff, then
prints each task's progress until all are `done`/`failed`, and exits non-zero if
any failed. `id`s in the file are local labels; the runner maps them to real
board ids and rewrites `deps` accordingly.

## Session launcher

Define your crew (see `examples/roles.json`), then **always dry-run first** to
see exactly what will be spawned:

```bash
node harness/launch.js examples/roles.json --dry-run
```

When it looks right, launch for real (requires the `claude` CLI on PATH):

```bash
node harness/launch.js examples/roles.json
tail -f .telex-logs/*.log
# stop: for f in .telex-logs/*.pid; do kill $(cat "$f"); done
```

Each worker is started with an MCP config pointing at the daemon and a prompt
that makes it register under its role name, join the work channel, and run the
worker loop. Override the binary with `CLAUDE_BIN`, target a subset with
`--only docs,app`, and pass extra flags per worker via a `claudeArgs` array in
the roles file.

> The launcher shells out to `claude`; flags vary by CLI version. If a flag is
> rejected, adjust `claudeArgs` / the `--allowedTools` list in `launch.js`. You
> can always skip the launcher and start worker sessions by hand — paste the
> bootstrap prompt from the README and they'll join the same board.

## Supervisor controls

The dashboard (`http://127.0.0.1:4123/`) has a control bar: broadcast a message
to every agent, or post a task to the board, acting as the `supervisor`
pseudo-agent. Backed by:

- `POST /api/control/broadcast { text, to? }`
- `POST /api/control/task { title, detail?, role?, deps? }`

## Putting it together

```bash
node server.js                                   # 1. daemon
node harness/launch.js examples/roles.json       # 2. spawn the crew
node harness/run.js examples/workflow.json       # 3. drive the workflow
# watch in the browser dashboard or `node tui.js`
```

Self-test (mock workers + the real runner, no `claude` needed):

```bash
node server.js & sleep 1 && node harness/selftest.mjs
```
