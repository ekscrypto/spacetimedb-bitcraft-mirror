# Upstream connection resets in one-process multi-mirror mode (OPEN)

> First hard evidence: 2026-08-17 production switch attempt (01:13–01:26 UTC).
> Status: **OPEN** — mechanism confirmed in the calibrated local repro
> 2026-08-17; mitigations 1–3 validated locally but the **2026-08-17 second
> production attempt (10:22–10:31 UTC, mitigated build) still cascaded during
> the unpaced cold-start tail** — death profile changed completely (6×
> upstream EOF, zero self-inflicted probe/RST kills) but 37–51 s poll gaps
> exceeded what the local repro modeled. See "Production attempt #2" below.
> Sibling analysis: [`MULTI-MIRROR-STARVATION.md`](MULTI-MIRROR-STARVATION.md)
> (fixed 2026-08-16) — that failure was *self-inflicted* (process-wide client
> gate + internal cascade). This one originates **upstream of us**: the
> upstream edge kills our connections. Per-database client gating contains
> the blast radius, but the busy regions flap.

## Leading hypothesis (2026-08-17): CPU starvation violates the session deadlines

The production host is a **4-physical/8-logical-core Xeon E3-1270 v6** (~4k
CPU-score class; the dev laptop that produced the 66-minute clean soak is
~20k — ~5× per-core). The session loop is built around hard deadlines that
assume Tokio workers poll tasks promptly:

- client WS Ping every **10 s** (`upstream.rs` `CLIENT_PING_INTERVAL`),
- upstream RSTs a connection whose Pongs aren't flushed within **~30 s**
  (documented behavior, `upstream.rs` un-split-socket comment),
- OneOffQuery liveness probe: **30 s wall-clock from send to processing the
  response** (`upstream.rs` `PROBE_TIMEOUT`).

Production builds have **no core partitioning** — the `core-pinning` feature
is non-default and `tools/deploy.sh` builds with no features — so all 14
sessions' socket tasks, TLS, and client serving share one multi-thread
runtime sized `num_cpus` = 8 workers (on ~4 cores' worth of throughput).
Frames under the 256 KiB offload threshold — including Brotli-compressed
live transaction updates — decompress and BSATN-decode **inline on those
workers**. Each upstream death triggers a cold reset + full re-seed (a
hundreds-of-MB Brotli decode plus millions of row inserts plus embedded-cache
rebuild). On this host's cores a re-seed burst takes ~5× longer than on the
laptop; overlapping with 13 other mirrors' live ingest it starves worker
polling past the 10–30 s deadlines → missed Pongs → upstream RST, or probe
responses sitting unprocessed → false `ProbeTimeout` → more cold resets →
more re-seeds. The cascade.

This reframes rather than contradicts "the upstream edge kills our
connections": the RSTs do originate at the edge, but the trigger is our
slow-client behavior under CPU starvation. Note the one-process design does
**less total work** than the fleet + standalone relay-cache it replaces (the
embedded cache removes the WS hop and the second BSATN decode) — but the
work it eliminates is not the deadline-threatening work. The irreducible
first decode + relational apply is identical in both topologies and
dominates; the integration also *concentrates* it onto one shared runtime,
losing the per-process isolation that lets the slow host survive re-seed
bursts in fleet mode.

**Host measurements (2026-08-17 ~01:40 UTC, immediately after rollback to
the per-instance fleet):** the 14 fleet mirror processes — which carry only
the irreducible ingest work — summed to **771 % CPU (system total 99.7 %)**
while re-seeding/catching up post-rollback, decaying within minutes to
**~422 %** for the eight busy regions with regions 7 and 12 individually
sustained at **~95 % of a core each**. The busy set (7, 8, 9, 12, 14, 17,
18, 19) is exactly the set that died first in the incident. No sampler
existed on the host during the incident window (no sar/atop/monitoring
agents) — CPU for 01:13–01:26 UTC itself is unrecorded; the OVH Manager CPU
graph is the only possible source (manual check pending).

**Predictions that distinguish this hypothesis:** (1) event-loop poll gaps
≥5 s precede deaths — **confirmed** (135 gap-warnings, 5–10 s, repro);
(2) probe timeouts log "bytes arrived since probe send" (false timeouts),
not "socket silent" — **confirmed** (6 false vs 1 silent, repro);
(3) the CPU-capped local repro reproduces the death modes at host-equivalent
`--cpus` — **confirmed** (21 deaths: 7 RST + 14 probe timeout at `--cpus=1`;
the `--cpus=4` control ran clean, matching the fast-laptop soak);
(4) on the next production attempt with `pidstat`/`vmstat` capturing, CPU is
pegged during the cascade — pending.

## Production attempt #2 (2026-08-17 10:22–10:31 UTC, mitigated build `56de7e806`)

Operator ran `switch-to-bitcraft-mirror.sh`; green started clean and reached
11/14 live in ~7 min with zero deaths. Then, during the cold-start tail
(regions 18/19/23 still seeding, 11 mirrors ingesting), six busy regions
(7, 8, 9, 12, 14, 17) died within ~2.5 min; operator rolled back at 7/14
live.

**What the mitigations changed** (vs attempt #1 at 01:13):

| | Attempt #1 (unmitigated) | Attempt #2 (mitigated) |
|---|---|---|
| Deaths | 9 | 6 |
| Modes | 4 RST-without-close + 4 probe timeout + 1 EOF | **6× unexpected EOF only** |
| Self-inflicted (probe) kills | 4 | **0** |
| Attribution | correlate timestamps | instant (`database=` tags) |

M1 (silence-aware probe) eliminated every self-inflicted death — no probe
kills occurred despite poll gaps that would have false-killed sessions under
the old logic. The deaths that remain are upstream-initiated closes under
genuine deep starvation: **event-loop gaps hit 37.5 s (r18) and 51.3 s
(r12)** — 6–8× the local mitigated repro's maximum (~6 s).

**M3 (re-seed pacing) never engaged**: the cascade ran entirely inside the
cold-start tail, where session 1 is unpaced by design. The dying regions
never reached their (paced) reconnects before the rollback.

**Telemetry gap (operator error):** the CPU capture died at 10:25:45 (the
restart's `pkill` self-matched its own ssh session), so there is no
CPU/iowait data for the cascade window, and post-rollback the cumulative
`/proc` counters cannot isolate it. Consequently the deepest question is
**unresolved**: whether the 37–51 s worker stalls were deeper CPU starvation
than the 1-quota repro models, or **disk iowait inside task polls** (seed
applies hitting the on-disk store — which would explain long gaps at modest
CPU). The local repro cannot distinguish these: Docker's CFS quota produces
full-speed bursts between throttles rather than a slow host's sustained
queueing, and the container's page cache hides disk behavior.

**Next steps specific to this attempt:**

1. Pace seeding whenever any other mirror is live (not just reconnect
   sessions) — directly targets the cold-start tail where both production
   cascades occurred.
2. Fixed capture for the next attempt: CPU **and iowait split** +
   `/proc/diskstats` + per-process IO bytes, started from a pidfile-guarded
   script (no `pkill` self-match), before green starts.
3. If iowait confirms: the fix family changes from CPU scheduling to IO
   (batch/relax durability for mirror stores, or keep mirror DBs on
   tmpfs-style storage).
4. The embedded-cache observer dispatch on workers remains the next CPU
   lever if CPU (not IO) is confirmed.

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
  subscribe stall, RAM (host ~50 GiB free, blue stopped). CPU contention is
  **not** ruled out: "load fine" was observed only during the local no-repro
  run — the host had no CPU sampler during the incident window (see leading
  hypothesis above).

## Hypotheses (unvalidated; leading hypothesis above, then secondary)

0. **CPU starvation on the 4C/8T production host violates the 10–30 s session
   deadlines** — see "Leading hypothesis". The RSTs are the edge's response to
   our starved slow-client behavior; the re-seed amplification loop is its
   engine. Under confirmation.
1. **Upstream edge throttling / anti-abuse on bursty multi-connection
   patterns.** 14 WebSocket connections established within ~5 minutes plus
   multi-hundred-MB Brotli seed downloads from one IP, then sustained
   ~100k tx/min ingest. Datacenter source IP (OVH) may be policed more
   aggressively than the residential IP used in the passing local test.
   Best match for the one `lived 0ns` instantly-refused connect, which CPU
   starvation does not explain (connect-time RST = likely connection-rate
   policing; secondary, distinct mechanism).
2. **Shared-DNS edge variance.** `bitcraft-early-access.spacetimedb.com` is
   a shared DNS name with multiple A records; sessions land on different
   edge nodes. Some nodes/paths may reset long-lived or heavy connections.
   Now testable per-session: the instrumented build logs `peer=` on every
   connect.
3. **Re-seed amplification loop.** Every reset re-downloads full seeds;
   the added burst raises the chance of further resets — a self-sustaining
   flap once it starts. Consistent with hypothesis 0 (CPU-driven burst
   crowding) rather than purely bandwidth-driven.

## Impact

- Blocks convergence of the one-process topology: the cutover's wait can
  time out while regions flap. Service impact is per-database (gating), but
  the flapping set is exactly the busiest regions (7, 8, 12, 14, then 3, 9,
  17, 18).
- Does **not** affect the per-instance fleet topology (stable for weeks).

## Next steps / mitigations to evaluate

Done 2026-08-17 (instrumented build — built and unit-tested locally, **not
yet deployed**; deploy via `tools/deploy.sh bitcraft` when ready):

1. ✅ `database=` tag on every session lifecycle error (`runtime.rs` exit
   lines) — errors are now attributable from the journal alone.
2. ✅ Resolved `peer=` address logged on every upstream connect (tests the
   shared-DNS hypothesis per session).
3. ✅ Probe false-timeout detection: at `ProbeTimeout` the log now says
   whether socket bytes arrived since the probe was sent ("response likely
   delayed by local processing") or not ("upstream non-response").
4. ✅ Event-loop gap warnings: any ≥5 s gap between session-task polls logs
   `event loop gap … worker starvation suspected` — the direct starvation
   signal, named per database.

Open:

5. **CPU capture during the next production attempt** (no sampler existed
   for the 01:13–01:26 window): run `pidstat -t 5` + `vmstat 5` to a file
   from before start through the soak; also check the OVH Manager CPU graph
   for 2026-08-17 01:10–01:30 UTC.
6. ✅ **CPU-capped local repro** (`scripts/local-full-fleet-test-cpu-capped.sh`):
   **CONFIRMED 2026-08-17.** Calibrated with `openssl speed -evp sha256`
   (8192-byte blocks): the whole production host (Xeon E3-1270 v6, 4C/8T; 8
   threads aggregate to 3.88× its single core) reaches 1414 k/s vs 2328 k/s
   for **one** M2 Max quota unit — the entire host ≈ 0.6 units on sha256
   (per-core gap 6.4×), ≈ 1–1.3 units on branchy integer work. At the
   host-equivalent `--cpus=1`: 21 deaths (7 reset-without-close-handshake,
   14 probe timeout), 135 event-loop gaps of 5–10 s, and 6 of 7 probe
   timeouts were **false** (socket bytes had arrived). The `--cpus=4`
   control (~4–6× the whole host's compute) ran clean, matching the native
   fast-laptop soak. Verdict: aggregate CPU starvation reproduces the
   production failure locally, with the instrumented build attributing it.
7. **tcpdump during a flap:** record who sends the RST (upstream edge IP?)
   and whether OneOffQuery probe responses genuinely stop arriving.
8. **Mitigations, ranked by leverage now that the repro confirms the
   mechanism** — **1–3 implemented and validated 2026-08-17** (local tests
   13/13; calibrated `--cpus=1` repro, ~65 min: baseline 21 deaths /
   never-recovering cascade → mitigated 14 deaths all during the pipelined
   cold start, then **converged 14/14 live and held**; false-probe kills
   6 → 0, probe kills 14 → 8, event-loop-gap warns 145 → 83, observed max
   gap ~10 s → ~6 s):
   - ✅ **Socket-silence-aware `ProbeTimeout`** (6 of 7 baseline timeouts
     were false): a probe that is late while the socket is still receiving
     is now tolerated (one-time warn, hard cap at 150 s) instead of
     cold-resetting the session. Only a socket-silent probe kills.
   - ✅ **Heavy decode off the workers** (RSTs from missed Pongs): all frames
     ≥256 KiB **and** compressed frames ≥4 KiB decompress + BSATN-decode +
     TU-convert on the (niced) blocking pool, keeping worker poll sections
     tiny so Ping/Pong/probe servicing stays latency-bounded under load.
     Restructuring the deferred-frame replay also fixed a latent race where
     a wire seed arriving during an offloaded TU decode could be swallowed
     as a background message.
   - ✅ **Re-seed pacing** (reconnects only): each table's seed applies
     locally before the next table downloads, bounding a re-seed's burst to
     one table at a time; cold starts stay pipelined (nothing live to
     starve). Observed in the repro: recovery proceeded one paced re-seed at
     a time, live count monotonically 7→14, zero disconnected.
   - **Capacity / topology** (still open): the host's ~4 slow cores measured
     ~771 % demand during fleet catch-up; if the mitigations don't hold in
     production, the host is sized for the per-instance fleet, not
     one-process mode.
   - **Next lever if more is needed**: the embedded-cache observer dispatch
     still runs inside the apply future polled from session tasks — a
     remaining CPU-on-worker path worth offloading next.
   Note: `std::thread::yield_now()` is *not* the fix — the heavy threads
   (seed decode, applies) are already niced, the OS already preempts
   threads at timeslice granularity, and the starvation is in Tokio's
   userspace task queues (poll sections + aggregate saturation), which a
   thread yield doesn't touch.
9. **Report upstream** to Clockwork Labs with timestamps — the RSTs
   originate at their edge (`bitcraft-early-access`).

## Quick reference

| Item | Where |
|------|-------|
| Error site / cold-reset loop | `crates/public-mirror/src/runtime.rs:247`, loop at `runtime.rs:181-246` |
| Liveness probe (60 s interval / 30 s timeout) | `crates/public-mirror/src/upstream.rs` probe config |
| Event-loop gap warning (starvation signal) | `crates/public-mirror/src/upstream.rs` `note_event_gap` / `EVENT_LOOP_LAG_WARN` |
| Probe false-timeout diagnosis | `crates/public-mirror/src/upstream.rs` `ProbeCheck` arm |
| Flush tables on reset | `crates/core/src/host/module_host.rs:1897-1934` |
| Per-database client gating (mitigates impact) | `crates/client-api/src/routes/mirrors.rs` `public_mirror_accepts_clients_for` |
| Local no-repro run (66 min, 0 disconnects) | `scripts/local-full-fleet-test.sh`, summary 2026-08-16 |
| CPU-capped repro (aggregate-starvation test) | `scripts/local-full-fleet-test-cpu-capped.sh` |
| Stable per-instance fleet for comparison | `bitcraft-relay/tools/public-mirror@.service` |
| Host: 4C/8T Xeon E3-1270 v6, no CPU sampler | `ns518212` (measurements 2026-08-17 in Leading hypothesis) |
