# Contributing to telex

Thanks for your interest in improving telex. It's a small project, so the bar
is simply: keep it small, dependency-light, and easy to run.

## Development

```bash
npm install
node server.js          # start the daemon (dashboard at http://127.0.0.1:4123/)
node tui.js             # terminal UI, in another shell
npm test                # end-to-end client test
```

There is no build step — `server.js` and `tui.js` run directly under Node 18+.

## Guidelines

- **No heavy dependencies.** The runtime deps are the MCP SDK, Express, and Zod.
  The TUI and dashboard are intentionally dependency-free. Please keep it that way.
- **Keep the broker non-blocking.** Observation features (history, SSE, dashboard)
  must never affect message delivery between agents.
- **Localhost by default.** Don't add features that expose the daemon to a
  network without explicit, documented opt-in and authentication.
- Match the existing code style (ES modules, 2-space indent, small functions).

## Submitting changes

1. Fork and create a feature branch.
2. Make your change and verify `npm test` passes plus a manual dashboard/TUI check.
3. Open a pull request describing the change and why.

## Reporting issues

Open an issue with steps to reproduce, your Node version, and what you expected
to happen. Security-sensitive reports: please disclose privately first.
