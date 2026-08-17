# Upstream connection resets in one-process multi-mirror mode (root cause found in #5: unit `MemoryHigh=32G` memcg throttle — local, ours, fixed; soak in progress)

> First hard evidence: 2026-08-17 production switch attempt (01:13–01:26 UTC).
> Status: attempts #1–#3 all lost busy regions to upstream closes.
> **Root cause identified 2026-08-17 from the attempt-#3 artifacts** (see
> "Root cause" under "Production attempt #3"): synchronous logging blocked
> inside `Future::poll` whenever the host's storage stalled (whole-host
> iowait 20–95 % through the cascade — the first telemetry reading of
> "~0 disk" was a minute-average artifact). A slow journald/pipe froze every
> session process-wide; upstream RST'd Pong-deadline violators; re-seeds
> amplified. That fix landed (`93f259007`), was deployed for attempt #4
> (14:31–14:55 UTC), and **eliminated the log-stall mechanism** — but the
> busy regions still died, and the attempt-#4 telemetry measured the layer
> underneath end-to-end: the host's ~3 MB/s legacy HDD (no SSD) starves the
> process's page-fault reads whenever host-side writeback bursts (this time:
> nginx's own logging of the outage), stalling `Future::poll` past the pong
> deadline. Attempt #5 (17:39 UTC →) deployed the full fix list — and the
> new D-state stack telemetry caught the remaining killer in the act: the
> staged unit's **`MemoryHigh=32G` soft cap** (set from the *local* soak
> peak, ~26 GiB; production steady state is ~35–37 GiB) put green
> permanently over `memory.high`, and every memory charge was memcg-throttled
> with nothing reclaimable (no swap, binary mlocked) — 154 k throttle events,
> tokio workers D-stalled in `mem_cgroup_handle_over_high`, one RST. Raised
> to 48G mid-attempt; the throttle froze instantly. See "Production attempt
> #5". This cap rode along in **every** prior attempt (staged `313e894`),
> so all four failure layers — logging, disk, swap-adjacent, memcg — were
> local and ours; nothing points at the upstream edge.
> Sibling analysis: [`MULTI-MIRROR-STARVATION.md`](MULTI-MIRROR-STARVATION.md)
> (fixed 2026-08-16) — that failure was *self-inflicted* (process-wide client
> gate + internal cascade). This one is a feedback loop between our own
> logging stalls and the upstream edge's pong-deadline RSTs. Per-database
> client gating contains the blast radius, but the busy regions flap.

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

1. ✅ **Serial seeding: one mirror at a time, one table at a time**
   (implemented 2026-08-17): `run_public_mirror_loop` consults the status
   registry before each session — pacing applies to reconnects *and*
   cold-start sessions with ≥1 live mirror (only the very first mirror of a
   cold start pipelines; nothing is live to starve). Pacing is strict: each
   table's seed applies locally before the next downloads, and because the
   waits happen inside the gate-held seeding phase, the subscribe gate then
   covers the mirror's entire seeding — one mirror seeds end-to-end at a
   time. `CUTOVER_WAIT_TIMEOUT` default raised 3600 → 7200 s for the longer
   serialized cold start.
2. ✅ **Fixed capture for the next attempt** (implemented 2026-08-17):
   `bitcraft-relay/tools/cutover-telemetry-capture.sh` — pidfile-guarded (no
   pattern-match kills), CPU **and iowait split**, `/proc/diskstats`, and
   the green process's CPU + `/proc/PID/io` bytes, pid re-resolved every
   sample; `summarize` prints per-minute rows. Wired best-effort into
   `cutover-to-bitcraft-mirror.sh` step 5/8 so it cannot be forgotten.
3. If iowait confirms: the fix family changes from CPU scheduling to IO
   (batch/relax durability for mirror stores, or keep mirror DBs on
   tmpfs-style storage).
4. The embedded-cache observer dispatch on workers remains the next CPU
   lever if CPU (not IO) is confirmed.

## Production attempt #3 (2026-08-17 11:32 UTC →, serialized-seeding build `a627e4220`)

Strict serial seeding (one table at a time, one mirror at a time) plus the
auto-started telemetry capture. **The telemetry finally covers the failure
window — and its first reading (below) re-attributed the mechanism; a
subsequent per-interval decode of the same samples re-read it again** (see
"Root cause" next section): aggregate CPU starvation is disproven either way
(the box's *compute* was never saturated), but the storage was not idle —
whole-host **iowait ran 20–95 %** through the cascade.

| measurement (during 22–31 s poll gaps + deaths) | first reading | corrected reading (per-interval `/proc/stat` decode) |
|---|---|---|
| System CPU | 2–5 % | 0.4–13 % during freezes (never compute-bound) ✔ |
| iowait | "3–13 %, ~1 core" | **16 % → 47 % → 84 %** at cascade onset (11:42:26+), **91–95 %** inside the mass freezes ✔ |
| disk throughput | 0.00–0.12 MiB/s | 32–53 MB/s write burst at 11:42:26–36, then 1–5 MB/s flushes against a stalled device |
| Green `--data-dir` | 5.5 MB (control-db + config + logs — mirror stores memory-only, as designed) ✔ | unchanged |
| Swap | 1 GiB nearly full, zero current traffic ✔ | unchanged |

The corrected row matters because the first reading ("box idle, ~0 disk")
made an environmental blocker look impossible and pushed the suspicion
inward. What actually holds at both readings: the *process* was idle (proc
CPU 0.4–1.2 % of one core during the freezes, own disk writes ≈ 0) while its
tasks were parked — **in-process synchronous blocking inside `Future::poll`
on a Tokio worker**. The per-interval decode plus the journal timestamps
then identified the blocker (next section): not lock contention in the
apply plumbing, but the synchronous logging path against a stalled storage
stack. The local Docker repro's CPU-starvation reproduction was genuine for
its own environment (CFS quota throttling) but modeled the wrong resource
for production — the "Leading hypothesis" section above is retained as
history, superseded here.

**Mitigations' production scorecard (attempt #3, vs #1 / #2):** cold start
strictly serialized (visible in `/v1/mirrors` — exactly one mirror
subscribing at a time); silence-aware probe absorbed every late probe
(multiple "tolerating starved runtime" events with megabytes received, zero
self-inflicted kills); the cascade is dramatically slower — 4 deaths (12, 8,
9, 19), all upstream-initiated RST / `ECONNRESET`, fleet holding at 10/14
(vs collapse to 5 in #1, 7 in #2) with reconnects proceeding strictly
serially. The underlying stall mechanism remained unfixed *at that build* —
see below.

**Attempt #3 outcome:** rolled back at ~11:58 UTC after 5 upstream-initiated
deaths (regions 12, 8, 9, 7 — the last live 23 min — plus region 19, whose
serialized re-seed died mid-subscribe at `lived 0ns`, confirming that a
region whose apply loop stalls past the pong deadline may never complete a
re-seed). Fleet held 10/14 throughout; no collapse. Full telemetry and the
green journal are kept operator-side (never published; scrubbed from the
public repo history 2026-08-17 — they contain production hostnames and
peer IPs). Journal also on the host at `/tmp/cutover-telemetry/`.

**Recovery-path observation:** a disconnected region takes minutes before it
visibly re-attempts — cold-reset (flushing all 274 in-memory tables), then
backoff, then queueing at the subscribe gate behind whichever serialized
re-seed is in progress. The `disconnected` status does not distinguish
flushing / backing-off / gate-queued, which reads as "never re-attempts".

## Root cause (2026-08-17, post-#3 artifact re-analysis): synchronous logging against stalled storage froze the whole process

Three signatures in the attempt-#3 artifacts identify the blocker precisely:

1. **Process-wide log silence for 18.3 s.** The green journal's last line is
   `subscribe [162/274]` at 11:44:57.199; the next lines — eight
   `event loop gap` warnings from eight *different* databases — land at
   11:45:15.489–15.525, i.e. **within 36 ms of each other** after 18 s of
   nothing. When the blockage cleared, every blocked writer flushed at once.
   Repeated in waves at 11:45:23/26/27/29/35 with gaps up to 31.8 s.
2. **Freezes end simultaneously across unrelated sessions** — a shared,
   process-wide resource, not per-database locks (the apply path had already
   been verified per-database: `SingleThreadedExecutor` per DB, channel-in /
   oneshot-out, no worker blocking).
3. **The process itself was doing nothing** during the freezes: proc CPU
   0.4–1.2 % of one core, own disk writes ≈ 0, whole-host iowait 91–95 %.
   All cores idle, storage choked, everything waiting.

The shared resource is the **logging path**. `configure_tracing`
(`crates/core/src/startup.rs`) wrote every event synchronously from the
emitting task, twice: to `std::io::stdout` — under systemd a 16–64 KiB pipe
to journald — and, because disk logging defaults **on**, to a rolling file
in the data dir. When the host's storage stalls (whole-device iowait
20–95 % for the final 16 minutes of the attempt; the write volume itself
was only 1–5 MB/s of flushes — trivial for a healthy disk, saturating for
this one), journald stops draining the stdout pipe and dirty-page writeback
throttles the file writes. Every `log::`/`tracing` event then blocks its
caller inside `Future::poll` on a Tokio worker. Ping/Pong/probe servicing
stops process-wide; upstream RSTs sessions whose Pongs don't flush within
~30 s; each death triggers serialized re-seeds that log and allocate more,
deepening the stall — the cascade, and the "recovery that never visibly
re-attempts" (the reconnect path logs too).

Aggravators, all confirmed in the code at the time:

- **`RUST_LOG` was silently ignored** — `conf_to_filter` parsed only the
  config.toml directives, and the data-dir config.toml (copied from the
  shipped default `crates/standalone/config.toml`) pins
  `spacetimedb=debug`. That is why DEBUG probe lines appear in the
  production journal despite the unit's `RUST_LOG=…=info`. The unit's env
  var never worked.
- **Per-table subscribe progress at INFO** — two lines × 274 tables per
  region-seed; the biggest single multiplier of storm volume.

Why the per-instance fleet is stable on the same host: fourteen separate
processes each get their own runtime and their own small pipe to journald;
no single process's cold start multiplies fourteen regions' log storm, and
the fleet's steady state never re-seeds everything at once.

### Fix (landed as `93f259007`; deployed and validated in attempt #4)

1. **Non-blocking writers** (`crates/core/src/startup.rs`): both fmt writers
   (stdout and the rolling file) are wrapped in
   `tracing_appender::non_blocking` — a dedicated writer thread with a
   bounded (100 000-line) lossy queue. A stalled sink now drops log lines
   instead of freezing the database.
2. **`RUST_LOG` honored**: the env var now overrides the config.toml
   directives, so the unit's `RUST_LOG=spacetimedb=info,public_mirror=info,relay_cache=info`
   takes effect.
3. **Per-table subscribe progress downgraded INFO→DEBUG**
   (`crates/public-mirror/src/upstream.rs`); `/v1/mirrors` exposes the same
   progress without the firehose.

**Local A/B validation** (`scripts/local-logging-stall-test.sh`: one region,
no CPU cap, stdout piped through a reader that never drains — a fully wedged
journald pipe; verdict = `event loop gap` warnings in the data-dir rolling
logs, which keep flowing on a healthy local disk):

- **pre-fix binary** (HEAD before the fix): process parked at **0.0 % CPU**,
  seeding **stuck at table 147/274 for 100+ consecutive seconds**, event-loop
  gaps **5.8 s and 42.0 s** (42 s is past the ~30 s pong deadline — the
  production kill), never reached live in the window. Under a gentler 4 KB/s
  drain the same binary stalls 6.5 s mid-seed — matching the production
  freeze/release cycling as the pipe trickles.
- **fixed binary**: **96.7 % CPU (working)** through the same wedged pipe,
  seeded all 274 tables and went **live in ~105 s**, event-loop gaps only
  2 × 5.5 s — a bounded big-table decode hiccup that the pre-fix binary
  shows identically at the same tables (it is not the freeze mechanism and
  is far inside the pong deadline).

The residual 5–6 s gaps during the multi-million-row table decodes
(`location_state`, `knowledge_*`) exist in both arms and are pre-existing:
the offloaded `spawn_blocking` decode plus task scheduling at that scale.
They did not escalate and did not kill sessions; watching whether they
stretch on the slower production cores is part of the next attempt's
telemetry review.

**Next steps (revised):**

1. ✅ Logging fix landed (above) — deployed for attempt #4 and validated (see
   the scorecard there).
2. Recommend `Environment="SPACETIMEDB_DISABLE_DISK_LOGGING=1"` in
   `bitcraft-mirror.service`: journald already captures stdout; the rolling
   file double-writes every line to the same slow disk.
3. Watch **iowait**, not just CPU, in the attempt telemetry (the sampler
   already records `/proc/stat` — decode it per-interval; minute averages
   hid the onset for two analysis rounds).
4. ✅ Settled by attempt #4's per-interval disk decode (see "Production
   attempt #4"): the burst class is dirty-page writeback of host-side
   logging — #4's 180 MB flush traced to nginx's unrotated 14.9 GB
   access.log plus the error-log 502-storm; #3's own burst additionally
   included the then-unfixed debug firehose.
5. Swap: 1 GiB nearly exhausted, dormant through #3 — still no cushion.
6. Status granularity (flushing / backoff / gate-queued) — still worth
   doing for operator legibility, now decoupled from the incident.
7. Report upstream to Clockwork Labs the RSTs that arrive *after* the
   pong-deadline stalls — those stalls were self-inflicted, but the edge's
   response (RST vs close) remains their side.

## Production attempt #4 (2026-08-17 14:31–14:55 UTC, logging-fix build `93f259007`)

First attempt on the fixed build. **The log-stall root cause is confirmed
eliminated — and the layer underneath is now measured end-to-end: the host's
~3 MB/s legacy HDD (no SSD) starves the process's page-fault reads whenever
host-side writeback bursts, and this attempt's burster was nginx's own
logging of the outage.** Operator rolled back at ~14:55; blue fleet was 14/14
again by ~15:08 UTC.

**Fix scorecard (all three deployed fixes verifiably worked):**

| | #1 | #2 | #3 | #4 (fix build) |
|---|---|---|---|---|
| Deaths | 9 | 6 | 5 | 8 |
| Log-stall signature (≈18 s journal silences, waves) | yes | yes | yes | **none** — journal flowed continuously; 234 KB total |
| DEBUG firehose despite unit `RUST_LOG` | yes | yes | yes | **0 DEBUG lines** (env honored) |
| Self-inflicted probe kills | 4 | 0 | 0 | 0 — ≥6 saves ("probe late 30–35 s but socket receiving MBs": r8, r9, r11, r13, r14, r18) |
| Cold start | pipelined | pipelined | serialized | serialized + healthy: 40–70 s per seed, 12/14 live by 14:41:18, 0 errors until 14:46 |
| Green's own disk writes | — | — | 32–53 MB/s burst (11:42) | **815 KB total** (`write_bytes`; stdb.log 5.5 MB; 1 612 journal lines in the whole window) |

**Deaths — 8, all upstream-initiated, again exactly the busy set (plus r13):**

| UTC | region | mode | lived |
|---|---|---|---|
| 14:46:29 | 19 | RST without close handshake | **0 ns** (connect refused — 2nd rate-policing occurrence) |
| 14:46:34 | 7 | RST without close handshake | 845 s |
| 14:48:23 | 9 | unexpected EOF | 797 s |
| 14:48:23 | 18 | unexpected EOF | 302 s |
| 14:48:42 | 17 | unexpected EOF | 449 s |
| 14:48:51 | 14 | unexpected EOF | 565 s |
| 14:50:14 | 13 | unexpected EOF | 749 s |
| 14:52:52 | 12 | unexpected EOF | 945 s |

Quiet regions held (global, 3, 8, 11, 15 live at rollback; r23 mid-seed).
95 event-loop-gap warnings; the stall-correlated maxes were 20–25 s (r19
23.1 s, global 24.8 s — matching the two wave windows below), r11 reached
56 s; a few 195–220 s maxes (r11/r12) likely span reconnect/gate-queue
boundaries rather than single worker stalls — worth a look at
`note_event_gap`'s bookkeeping across session instances.

**Telemetry (5 s samples, full window — first attempt with the per-interval
disk split + loadavg in the raw log):**

1. **180 MB host write burst at 14:41:10–25 (38–72 MB/s)** — not green
   (green `write_bytes` 815 KB total; control-db touched only at start; no
   journald rotation; journald volume 1 612 lines/15 min). Attributed to
   **nginx log writeback**: `/var/log/nginx/access.log` is **14.9 GB,
   unrotated since 13 Jul** (`logrotate.timer` is *not-found* on this host —
   rotation never ran), plus 102 MB error.log. nginx measured writing
   74 KiB/s in steady state with blue serving; during the dark band the
   502-storm (public retries + coordinator probes + dashboard) multiplies
   that, and dirty-page writeback flushed ~180 MB at once.
2. **Two read-starvation waves**: 14:42:16–36 and 14:44:06–26 — iowait
   93–97 %, CPU 1–2 %, and **reads completing at 0.00 MB/s**: green's page
   faults queued behind the platter drain (wave 2 shows writes crawling at
   2.5–3.2 MB/s — the platter rate). Green's baseline reads between waves:
   0.7–1.1 MB/s = the 140 MB binary + libs faulting in lazily
   (`read_bytes` 224 MB over the run ≈ binary + runtime).
3. **Load decomposition (preempts the "CPU 13.24/8" misread):** 1-min load
   10→14.5 from 14:48:02 until green stopped (14:55:15) — but runnable
   threads were 1–3 of 8 logical CPUs in every 5 s sample (one spike of
   11), and 64 % of the CPU in use was *niced* (the offloaded decode
   pool). The load is D-state (uninterruptible IO-blocked), not a
   run-queue. Reconciliation with the local `--cpus=1` repro: CFS
   throttling starves via runnable pileup, production via D-state —
   different mechanisms, same kill condition: **any whole-process stall
   past the 10–30 s session deadlines**.
4. **Ruled out:** THP compaction (counters flat), dmesg IO errors (none),
   OOM/direct reclaim (flat, 26 GB available), swap (1023/1023 full and
   net-static through the window — no pressure; green never paged out; it
   drained ~1 GiB back in during blue's post-rollback re-seed, adding
   hostile reads to the *recovery*).
5. RSS 33.7→35.3 GiB climbing through re-seeds (watch item, not causal).

**Why the per-instance fleet survives this host:** its processes faulted
their text pages weeks ago, each instance's log volume is small, and nothing
in steady state write-bursts. A green first-run faults its 140 MB binary
during the highest-pressure window, and any host write burst turns the
single platter read-hostile for a minute at a time.

### Fix list for attempt #5 (ranked by leverage)

1. ✅ **nginx logging hygiene** (host ops, done 2026-08-17): logrotate
   installed (timer active; was never installed — 14.9 GB access.log since
   13 Jul), giants rotated out as `*.pre-rotation-2026-08-17`,
   `access_log … combined buffer=64k flush=5s` in nginx.conf, `error_log …
   crit` in the WS-band server block (kills the 502-storm prose; access_log
   still records the status codes), rotation `daily, rotate 14, nocompress`
   (no gzip churn on the slow array).
2. ✅ **Pre-fault + lock the binary** so green needs no disk reads after
   startup: `relay-pin-bitcraft-mirror.service` +
   `relay-pin-public-mirror.service` (blue fleet, one lock covers all 14
   instances) running `relay-pin-pages.sh` (vmtouch mlock, ~140 MB each,
   enabled at boot, restarted by deploy.sh after each build — a redeployed
   binary is a new inode).
3. ✅ `Environment="SPACETIMEDB_DISABLE_DISK_LOGGING=1"` added to
   `bitcraft-relay/tools/bitcraft-mirror.service` (kills the 5.5 MB/run
   double-write; deploys with the next cutover's unit install).
4. ✅ Aggregate the per-table `cleared N stale rows` INFO line
   (`crates/core/src/host/public_mirror.rs` `apply_external_update`, 274
   lines per cold reset) into one summary line per flush —
   `cleared {total} stale rows across {n} tables before re-seed`.
5. ✅ **Swap off** (2026-08-17: `swapoff -a`, both fstab swap lines
   commented, backup `/etc/fstab.relay-backup-2026-08-17`): exonerated for
   #4, but all-downside here — full for weeks (no cushion) and its swap-in
   reads land on the same platter.
6. ✅ Telemetry now captures **per-thread kernel stacks of D-state
   (uninterruptible-IO) threads** on every sample while any exist
   (`dstack` lines in telemetry.log: `/proc/PID/task/TID/stack` with an
   always-readable wchan line as corroboration/fallback; zero output in
   steady state). The next stall names its exact blocked syscalls.
7. ~~Report upstream~~ — **not sending** (decided 2026-08-17): blue is
   stable on the same edge/IP with the same 14 connections, so the RSTs
   and both `lived 0ns` refusals only ever followed our own stalls.
   Revisit only if a future attempt shows deaths with no preceding local
   stall.

## Production attempt #5 (2026-08-17 17:39 UTC →, all-fixes build + `MemoryHigh` discovery — ROOT CAUSE)

Full swap via `switch-to-bitcraft-mirror.sh --apply`. Every fix from the
list above was live: pinned binaries, swap off, nginx logging de-fanged
(buffered access_log 61 MB and trickling at ~140 KB/s — nothing like #4's
38–72 MB/s burst), disk logging disabled, aggregated flush lines, dstack
telemetry. Green seeded 14/14 sequentially in ~17 min (r8's death added a
re-seed cycle), nginx flipped at 17:56:56, all public endpoints verified,
blue fully stopped. Public WS outage: ~17.5 min.

**The fix list verifiably worked** — and the one residual death named its
killer via the new telemetry:

- Green's `read_bytes` flat at **536 KB for the whole run** (vs 224 MB in
  #4): the page pin eliminated lazy text-fault reads entirely. No
  read-starvation waves; iowait negligible outside benign md/jbd2 D-states.
- One death: **r8 RST at ~17:54:04** (lived 791 s, its original seed
  connection). The dstack histogram shows **5–9 tokio workers
  simultaneously D-stalled in `mem_cgroup_handle_over_high`** from
  17:52:46 through 17:54:03 — the sample right after the burst ended is
  the disconnect. 139 memcg wchan captures in total, all pre-fix.
- Re-seed worked as designed: 21 aggregated `cleared N stale rows across 1
  tables` lines (vs 274 per-table lines before), 6.1 M-row largest table
  cleared, feed reset generation=2, re-live in ~2 min, caught up to
  1.1 M tx within minutes. All other 13 regions held their original
  connections through the whole window.

**Root cause — `MemoryHigh=32G` in the staged unit (`313e894`, 2026-08-16).**
The cap was set from the *local* soak measurement ("peaks ~26 GiB" — the
unit comment said so), but production steady state is **~35–37 GiB**.
Green crossed 32 GiB while seeding r18/r19 (~17:49–17:51) and spent the
rest of the window **permanently over `memory.high`**:

- `memory.events.local`: **154 526 `high` throttle events** accrued by
  17:57 (~500/s while over) — the counter is the smoking gun; it froze
  the instant the limit was raised and has not moved since.
- With swap off and the binary mlocked, in-cgroup reclaim has nothing to
  free — nearly all pages are anonymous or pinned — so `try_charge`
  throttling (`mem_cgroup_handle_over_high`) degrades from a nudge into
  sustained multi-second D-stalls on whatever worker faults or allocates.
  Same kill condition as every prior layer: any whole-process stall past
  the 10–30 s session deadlines.
- Remediation (mid-attempt): `systemctl set-property
  bitcraft-mirror.service MemoryHigh=48G` at ~17:57:20 — throttle froze
  immediately, zero memcg D-states since, `memory.current` stable at
  ~36 GiB with 23 GiB MemAvailable. Persisted in the unit
  (`bitcraft-relay` `e51d332`), synced to the host, and the
  `system.control` override removed so the unit file is the single
  source. Ram-guard (stop if MemAvailable < 2 GiB) stays the hard
  backstop; `memory.max` remains unlimited.

**Retroactive reach:** the cap rode along in attempts #1–#5 alike, so
every attempt ran green permanently over the soft limit once seeded. The
prior layers remain real and measured (logging storm, disk read
starvation, writeback bursts) — and they compound: memcg reclaim pressure
drives page-cache writeback/eviction, which on ~3 MB/s platters is exactly
the read-starvation seen in #4. With the cap fixed at 48G, the soak is
clean so far; decommission-blue decision pending that soak.

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

## Hypotheses (0 superseded — root cause identified; the rest secondary)

0. ~~**CPU starvation on the 4C/8T production host violates the 10–30 s
   session deadlines**~~ — **superseded twice over**: aggregate CPU was idle
   during production freezes, and the local CFS-quota reproduction, while
   real, reproduced a different environment's failure. The deadline
   violations were real, the starvation was **of the logging sink**, not the
   CPU (see "Root cause").
0.5. **Synchronous logging against stalled storage** — **CONFIRMED** (code
   path, per-interval iowait correlation, journal-silence signatures, and a
   local single-region repro through a wedged stdout pipe: pre-fix parks at
   0 % CPU with a 42 s event-loop gap; fixed runs at ~97 % CPU to live).
   Fixed 2026-08-17. Attempt #4 validated the fix in production (no log
   stalls, no firehose) — and exposed the residual layer below: HDD
   read-starvation of page faults during host writeback bursts (see
   "Production attempt #4").
1. **Upstream edge throttling / anti-abuse on bursty multi-connection
   patterns.** 14 WebSocket connections established within ~5 minutes plus
   multi-hundred-MB Brotli seed downloads from one IP, then sustained
   ~100k tx/min ingest. Datacenter source IP (OVH) may be policed more
   aggressively than the residential IP used in the passing local test.
   Best match for the one `lived 0ns` instantly-refused connect, which CPU
   starvation does not explain (connect-time RST = likely connection-rate
   policing; secondary, distinct mechanism). A second `lived 0ns` connect
   refusal occurred in attempt #4 (r19, 14:46:29) — twice-observed now.
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
| Logging fix (non-blocking writers, `RUST_LOG`, quieter seeds) | commit `93f259007`; deployed and validated in attempt #4 |
| Attempt-#4 artifacts (host) | `/tmp/cutover-telemetry/{telemetry-attempt4.log, green-journal-attempt4.log}` |
| The #4 write-burst writer | nginx `/var/log/nginx/access.log` (14.9 GB, unrotated since 13 Jul — `logrotate.timer` not-found) + `error.log` (102 MB) |
| Host storage | legacy HDD, ~3 MB/s sustained (no NVMe/SSD) — the "stalled storage" of #3/#4 is arithmetic, not device failure |
