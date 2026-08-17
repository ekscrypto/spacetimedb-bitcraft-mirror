# Upstream connection resets in one-process multi-mirror mode (OPEN)

> First hard evidence: 2026-08-17 production switch attempt (01:13–01:26 UTC).
> Status: **OPEN** — unresolved, under investigation.
> Sibling analysis: [`MULTI-MIRROR-STARVATION.md`](MULTI-MIRROR-STARVATION.md)
> (fixed 2026-08-16) — that failure was *self-inflicted* (process-wide client
> gate + internal cascade). This one originates **upstream of us**: the
> upstream edge kills our connections. Per-database client gating contains
> the blast radius, but the busy regions flap.

## Symptom

One `spacetimedb-standalone --public-mirror-v1` process mirroring many
`bitcraft-live-*` databases progressively loses its upstream WebSocket
sessions. The mirror's own journal (all at
`crates/public-mirror/src/runtime.rs:247`) shows three death modes:

```
ERROR … upstream error: websocket error: WebSocket protocol error:
       Connection reset without closing handshake; cold-reset then reconnect in 1s (lived <Ns>)
ERROR … upstream error: liveness probe timed out — upstream did not respond
       to OneOffQuery within 30s; cold-reset then reconnect in 1s (lived <Ns>)
ERROR … upstream error: websocket error: IO error: unexpected EOF …
```

Each death triggers the designed cold-reset: kick that database's clients,
flush all its tables (`core/src/host/module_host.rs` — "flushing 274
table(s) before reconnect re-seed"), re-acquire the single subscribe-gate
slot, and re-download the entire seed. Regions other than the affected one
keep serving (per-database gating, fixed 2026-08-16) — but a flapping region
cycles `disconnected → re-seed → live → reset`, and re-seeds add burst
traffic that appears to feed further resets.

**Operator-reported history:** this reproduces every time the whole fleet
runs as one instance — the 2026-07 monolith era, the 2-mirror trial, and the
2026-08-17 switch. The per-instance fleet (one process per database, same
host, same IP, same 14 upstream connections) is stable by comparison: it ran
2026-07-31 → 2026-08-16 with only occasional single-region reconnects.

## Evidence — 2026-08-17 switch (host `ns518212`, `bitcraft-mirror.service`)

14 mirrors, subscribe gate = 1, blue fleet fully stopped (~50 GiB free —
memory was never a factor). Cold start itself was healthy and matched the
local benchmark (~5 min):

| UTC | Live | State |
|-----|------|-------|
| 01:13:32–01:18:38 | — | all 14 connected in sequence; first regions live within ~40 s |
| 01:20:07 | 12/14 | only 8 mid-re-seed, 19 queued |
| ~01:24 | 9/14 | 7, 8, 12, 14 (the four busiest, tx counters 270k–880k) disconnected |
| 01:25:23 | **5/14** | 3, 9, 17, 18 also down; 19 re-seeding — cascade spreading to previously-stable sessions |

Errors 01:19–01:25 (9 total): 4× `Connection reset without closing
handshake` (external TCP RST; one session `lived 0ns` — refused instantly),
4× liveness-probe timeout (upstream stopped answering OneOffQuery on a
connection that had been live for minutes), 1× unexpected EOF. Lived-times
of dying sessions: 0 s, 257 s, 274 s, 337 s, 516 s — long-lived live
sessions die too; this is **not** only a seed-time phenomenon.

**Differential that rules out our code as the sole cause:** the identical
binary, identical 14 mirrors, one process, on a dev laptop
(`scripts/local-full-fleet-test.sh`, 2026-08-16) ran a 66-minute full-fleet
soak with **zero disconnects**. The trigger is environmental — host/IP/path
dependent — not deterministic in the process itself.

## Differences vs the starvation post-mortem

- Starvation triggers were internal (probe/stall/backlog-cap) amplified by a
  process-wide client gate. Here: external RSTs + upstream probe
  non-response, with per-DB gating already in place.
- Not explained by: the 1 GiB live-backlog cap (no such errors), the 5-min
  subscribe stall, CPU/RAM contention (host ~50 GiB free, blue stopped,
  load fine during the local no-repro run).

## Hypotheses (unvalidated, most plausible first)

1. **Upstream edge throttling / anti-abuse on bursty multi-connection
   patterns.** 14 WebSocket connections established within ~5 minutes plus
   multi-hundred-MB Brotli seed downloads from one IP, then sustained
   ~100k tx/min ingest. Datacenter source IP (OVH) may be policed more
   aggressively than the residential IP used in the passing local test.
2. **Shared-DNS edge variance.** `bitcraft-early-access.spacetimedb.com` is
   a shared DNS name with multiple A records; sessions land on different
   edge nodes. Some nodes/paths may reset long-lived or heavy connections.
   The progressive worsening (12→5 live) is consistent with one bad path
   progressively claiming connections.
3. **Re-seed amplification loop.** Every reset re-downloads full seeds;
   the added burst raises the chance of further resets — a self-sustaining
   flap once it starts.

## Impact

- Blocks convergence of the one-process topology: the cutover's wait can
  time out while regions flap. Service impact is per-database (gating), but
  the flapping set is exactly the busiest regions (7, 8, 12, 14, then 3, 9,
  17, 18).
- Does **not** affect the per-instance fleet topology (stable for weeks).

## Next steps / mitigations to evaluate

1. **Logging gap (do first):** session-error lines at `runtime.rs:247`
   carry no `database=` tag — tonight's errors cannot be attributed to a
   mirror from the journal alone (had to correlate connect timestamps and
   lived-times). Add the database to every session lifecycle error.
2. **Differential test on the host:** run the existing isolated trial rig
   (`bitcraft-relay/tools/public-mirror-trial.service`, 2 mirrors on
   `:3030`) and a single-mirror instance on the same host/IP for an hour:
   - single mirror stable + 14-mirror flapping  → burst/multi-connection
     trigger (hypothesis 1/3);
   - single mirror also flapping → host/IP/edge path (hypothesis 2).
3. **tcpdump during a flap:** record who sends the RST (upstream edge IP?)
   and whether OneOffQuery probe responses genuinely stop arriving.
4. **Log the resolved peer address** per mirror to test the shared-DNS
   variance hypothesis (which A record each session landed on).
5. **Traffic shaping in the mirror:** pace/stagger seed downloads (rate-
   limit wire reads; stagger gate admissions) to avoid the burst signature.
6. **Report upstream** to Clockwork Labs with timestamps — the RSTs
   originate at their edge (`bitcraft-early-access`).

## Quick reference

| Item | Where |
|------|-------|
| Error site / cold-reset loop | `crates/public-mirror/src/runtime.rs:247`, loop at `runtime.rs:181-246` |
| Liveness probe (60 s interval / 30 s timeout) | `crates/public-mirror/src/upstream.rs` probe config |
| Flush tables on reset | `crates/core/src/host/module_host.rs:1897-1934` |
| Per-database client gating (mitigates impact) | `crates/client-api/src/routes/mirrors.rs` `public_mirror_accepts_clients_for` |
| Local no-repro run (66 min, 0 disconnects) | `scripts/local-full-fleet-test.sh`, summary 2026-08-16 |
| Stable per-instance fleet for comparison | `bitcraft-relay/tools/public-mirror@.service` |
