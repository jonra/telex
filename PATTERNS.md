# Coordination patterns on telex

telex's primitives — identity, channels, threads, the task board (post/claim/
report + deps), barriers, operator metadata, and the **stigmergic signal
substrate** — are enough to express most classic multi-agent coordination
patterns. Pattern choice is mostly *policy*: who can claim what, who talks to
whom, and how "done" is decided.

Run the live demonstration (start the daemon first):

```bash
node server.js
node harness/patterns.mjs      # or: npm run patterns
```

It runs four patterns with mock agents and asserts each.

## 1. Stigmergy (ant swarm)

Coordination through a shared, decaying signal field rather than a boss. Agents
`emit_signal` (activate to reinforce a path, inhibit to suppress one) and
`read_signals` to act on the strongest net topic past a threshold. Signals
**evaporate** over time (`TELEX_SIGNAL_HALFLIFE`, default 30s), so stale paths
fade and the swarm self-corrects. Activate vs. inhibit is the document's
"negative feedback" valve against runaway/hallucinated paths.

> Best for discovery, search, debugging, brainstorming — not precise auditable work.

## 2. Master–Worker (hierarchy)

A lead `post_task`s role-targeted work; workers `claim_task` theirs and
`report_task`. Predictable and accountable; the lead is the bottleneck. This is
the model Claude Code's Agent Teams uses.

## 3. Pipeline (DAG)

Tasks declare `deps`; a task stays `blocked` until its dependencies are `done`,
then unblocks automatically — so work runs in dependency order regardless of who
claims it. Great for repeatable, staged processes (extract → transform → load).

## 4. Contract-Net (auction / cost-bidding)

An auctioneer announces a task; workers **bid** by emitting a signal whose
strength reflects their fitness — here, inverse cost, drawn from each session's
`cost`/`account` metadata. The auctioneer awards to the strongest (cheapest)
bid and the winner claims the task. This is how telex turns multiple
subscriptions into **cost-balanced routing**.

## Mixing them

These compose: a pipeline whose stages are auctioned to the cheapest capable
session; a hierarchy whose lead uses signals to sense where the swarm is stuck;
an adversarial debate (activator/repressor agents) feeding a quorum `barrier`.
The substrate stays the same — only the policy changes.
