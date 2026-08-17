# Running the BitCraft mirror on an OVH K-5 (and similar spinning-disk hosts)

> **Who this is for:** anyone deploying this fork to mirror the BitCraft
> `bitcraft-live-*` databases on their own hardware — specifically on OVH's
> K-5 dedicated-server template, but every lesson here applies to any host
> whose storage is legacy HDDs rather than NVMe/SSD.
>
> This is the distilled, ordered list of everything we had to change to make
> the one-process mirror survive in production on that machine. The full
> evidence trail (telemetry, timelines, kill chains) lives in
> [`MULTI-MIRROR-UPSTREAM-RESETS.md`](MULTI-MIRROR-UPSTREAM-RESETS.md); this
> document is the operator-facing "do these things" version.

## The machine

OVH **K-5** (Advance line, 2026-era order):

| Component | Spec | Note |
|---|---|---|
| CPU | Xeon E3-1270 v6 — 4 cores / 8 threads | ~4k CPU-score class; fine for steady state, tight during re-seed bursts |
| RAM | 64 GB DDR4 | the mirror needs ~35–40 GiB once all 14 regions are seeded |
| Storage | 2× 2 TB-class SATA HDD, mdadm RAID1 (`md3`) | **the weakness.** No SSD/NVMe tier. Measured effective platter drain under contention: **~2.5–3.2 MB/s** |
| OS | Debian, systemd, nginx front | stock server image — see the logrotate gotcha below |

The CPU is not the problem and never was: steady state is ~1.5–4 cores, and
even during incident windows the *runnable* thread count stayed at 1–3 of 8.
**Every failure we hit traced back to the storage or the memory subsystem
stalling the process**, and the process is unusually stall-sensitive.

## Why this workload punishes stalls

The mirror holds long-lived WebSocket sessions upstream with hard deadlines:

- client WS **ping every 10 s**,
- upstream **RSTs a connection whose pongs aren't flushed within ~30 s**,
- liveness probe (**OneOffQuery**): **30 s** send-to-process budget.

One process mirrors all 14 regions (plus the embedded read cache), so one
stalled Tokio worker can blow the deadline for every session it polls. The
upstream edge does not wait: a missed pong is an RST, an RST is a cold reset,
a cold reset is a **full re-seed of that region** (hundreds of MB of Brotli
decode + millions of row inserts), and re-seed bursts make further stalls
more likely. On a fast dev box you will never notice any of this; on a K-5
it is fatal unless you do the work below.

Measured production footprint after all 14 regions are live: **~35–37 GiB
RSS** (creeping toward 40 GiB as live data accumulates), ~140 MB of binary +
shared libraries, steady disk traffic near zero *after* the fixes.

---

## Fix 1 — never let logging block the runtime (code fix, in this fork)

**Symptom:** the process went journal-silent for ~18 s at a time; all eight
databases' stall warnings flushed within 36 ms of each other when the pipe
drained; CPU sat at ~1 % of one core; upstream RST'd everything.

**Cause:** every `log::`/`tracing` event was written **synchronously from
the emitting task** — to stdout (under systemd: a 16–64 KiB pipe to journald)
and, disk logging being on by default, to a rolling file. When the platters
stall, the first log line after the stall blocks inside `Future::poll` on a
Tokio worker. Logging is on every hot path, so the whole process freezes.

**Fix (in this fork, no operator action needed — just don't regress it):**
both writers run on dedicated `tracing_appender::non_blocking` writer threads
with a **bounded 100k-line lossy queue** — a stalled sink *drops log lines*
instead of freezing the database. Also in the same fix: `RUST_LOG` now
overrides `config.toml` directives (it was silently ignored before, so a
stale `spacetimedb=debug` in the data-dir config ran production at debug —
the biggest log-volume multiplier), and per-table subscribe progress moved
INFO→DEBUG (`/v1/mirrors` exposes the same progress live).

**Operator-side logging hygiene that complements it:**

- `Environment="SPACETIMEDB_DISABLE_DISK_LOGGING=1"` in the unit — journald
  already captures stdout; the rolling `stdb.log` in the data dir is a
  double-write of every line to the slow array.
- Keep `RUST_LOG` at `info` (or tighter). At `debug` a single region re-seed
  emits thousands of lines; multiply by 14 regions.
- The fork also aggregates the re-seed "cleared stale rows" lines to one
  summary per table per flush (was: one line per chunk — 274 lines per
  re-seed, per region).

## Fix 2 — stop paging the binary in from the platters (vmtouch mlock)

**Symptom:** during host writeback bursts, the process's page-fault reads
completed at **0.00 MB/s** for 20 s at a time while the platter drained
(~180 MB of dirty pages at 38–72 MB/s entering a ~3 MB/s device). A
first-run process faults its ~140 MB of text pages lazily — exactly during
the highest-pressure window.

**Fix:** pre-fault and mlock the binary **and its dynamic libraries** so the
process needs zero disk reads after startup:

```sh
# relay-pin-pages.sh — invoked by the pin units below
BIN="${PIN_BINARY:?PIN_BINARY must be set by the unit}"
set -- "$BIN" $(ldd "$BIN" 2>/dev/null \
    | sed -n -e 's/.* => \(\/[^ ]*\) (.*/\1/p' -e 's/^\(\/[^ ]*\) (.*)/\1/p' \
    | sort -u)
exec /usr/bin/vmtouch -q -l "$@"
```

Two systemd units run it (one per binary), `Nice=19 IOSchedulingClass=idle`
so the pinning itself never competes with real work:

```ini
# relay-pin-bitcraft-mirror.service
[Unit]
Description=vmtouch mlock pin for the bitcraft-mirror binary
Before=bitcraft-mirror.service
[Service]
Type=simple
Environment=PIN_BINARY=/srv/relay/spacetimedb-bitcraft-mirror/target/release/spacetimedb-standalone
ExecStart=/srv/relay/bitcraft-relay/tools/relay-pin-pages.sh
LimitMEMLOCK=1G
Nice=19
IOSchedulingClass=idle
Restart=on-failure
[Install]
WantedBy=multi-user.target
```

**Two gotchas:**

1. **The lock follows the inode.** Rebuilding the binary creates a new inode
   that is *not* pinned. Restart (or re-run) the pin unit after every deploy —
   our `deploy.sh` and cutover scripts do this automatically. An unpinned
   fresh binary silently reintroduces lazy faulting.
2. A Rust release binary is mostly static — only `libstdc++`, `libgcc_s`,
   `libm`, `libc`, and `ld-linux` are dynamic — but those still page-fault,
   which is why the script pins the `ldd` closure and not just the ELF.

**Verification:** after startup, `grep read_bytes /proc/$(pidof spacetimedb-standalone)/io`
should be flat (we measured **536 KB total for a full 14-region seed+soak**
after pinning, vs 224 MB of fault reads before).

## Fix 3 — de-fang nginx (the burster you didn't know you had)

**Symptom:** the single largest writeback burst on the host was **nginx's
own access log** — ours had grown to **14.9 GB unrotated** because the stock
image shipped **without logrotate even installed** (`logrotate.timer:
not-found`). During a traffic event the dirty pages flushed at once and the
platter became read-hostile for a minute.

**Fix:**

```nginx
# nginx.conf — name the format BEFORE buffer params, or reload fails with
# 'unknown log format "buffer=64k"'
access_log /var/log/nginx/access.log combined buffer=64k flush=5s;

# inside the WebSocket-band server block: the 502-storm prose was the
# error.log multiplier during outages
error_log /var/log/nginx/error.log crit;
```

```sh
# /etc/logrotate.d/nginx — daily, keep 14, NO compression:
# gzip churn on a 3 MB/s array is its own outage
/var/log/nginx/*.log {
    daily
    rotate 14
    missingok
    notifempty
    nocompress
    delaycompress
    sharedscripts
    postrotate
        [ -f /var/log/nginx/nginx.pid ] && kill -USR1 $(cat /var/log/nginx/nginx.pid) 2>/dev/null || true
    endscript
}
```

If a giant log already exists: `mv` it aside, then `nginx -s reopen` (the
correct signal name — not `USR1` on the CLI) so workers drop the old fd.
Check `systemctl status logrotate.timer` actually shows active/running.

## Fix 4 — swap off (a liability with 64 GB RAM and no fast disk)

64 GB RAM with the mirror using ~36–40 GiB means swap is never needed as a
cushion. It had been **full and dormant for weeks** (no pressure relief, no
cushion left), and if the kernel ever did swap-in, those reads land on the
same ~3 MB/s platters as everything else — converting a memory hiccup into
a session-deadline violation. We exonerated it as a *cause* of the incidents
(it was net-static through them), then removed it anyway as pure downside:

```sh
sudo swapoff -a
# comment the swap lines in /etc/fstab (keep a backup) so it survives reboot
```

## Fix 5 — size the cgroup soft limit from *production* (the one that killed us last)

**Symptom:** with every fix above in place, one busy region still died. The
new D-state stack telemetry caught 5–9 Tokio workers simultaneously stuck in
`mem_cgroup_handle_over_high` — the systemd unit's `MemoryHigh=32G` was
sized from a **local** soak (~26 GiB peak), but production with real BitCraft
data runs at **~35–37 GiB**. The process lived permanently above its own
soft limit: **154,526 throttle events** (~500/second), and with swap off and
the binary mlocked, in-cgroup reclaim had *nothing to free* — so the memcg's
"gentle slowdown" degraded into sustained multi-second stalls, a missed
pong, and an upstream RST.

**Fix:** size from production footprint plus headroom:

```ini
# ~36 GiB steady state + ~12 GiB headroom; hard backstop is the ram-guard
# unit (stop if MemAvailable < 2 GiB), memory.max stays unlimited
MemoryHigh=48G
```

**How to diagnose this class of failure** (do this during any soak):

```sh
CG=/sys/fs/cgroup/system.slice/bitcraft-mirror.service
awk '/high/{print $2}' $CG/memory.events.local   # throttle events — must be 0/frozen
cat $CG/memory.current                            # vs MemoryHigh

# catch offenders in the act: any thread in D (uninterruptible) state,
# with its kernel stack — mem_cgroup_handle_over_high is the smoking gun
ps -eLo stat=,pid=,tid=,comm= --no-headers | awk '$1 ~ /^D/'
sudo cat /proc/<PID>/task/<TID>/stack
```

Our `bitcraft-relay/tools/cutover-telemetry-capture.sh` samples all of this
every 5 s (including per-thread D-state kernel stacks) and is what finally
named the killer after four failed production attempts.

## Also worth knowing

- **Serialized seeding** (`--mirror-subscribe-concurrency 1`): one region
  seeds at a time, one table at a time through the subscribe gate. On this
  class of host, parallel re-seeds stack decode bursts onto the shared
  runtime and start the cascade. Full 14-region cold seed takes ~15–17 min;
  plan the maintenance window for it.
- **ram-guard:** a sidecar unit stops the mirror if `MemAvailable` drops
  below 2 GiB. With `memory.max` unlimited, it is the real OOM protection.
- **Watch memory creep:** RSS grew 36 → 40 GiB over the first hours of live
  traffic. If you pin `MemoryHigh` too close to steady state you re-create
  Fix 5's failure; revisit when live data grows.
- **Load average lies:** a K-5 incident shows load 10–14 on 8 CPUs with only
  1–3 *runnable* threads — the rest is D-state (uninterruptible IO), not CPU
  starvation. Decompose before concluding the CPU is too small.

## Checklist

```sh
# storage tier
apt install vmtouch logrotate                 # the stock image lacked BOTH
systemctl enable --now relay-pin-bitcraft-mirror.service   # per binary, after every rebuild
systemctl status logrotate.timer              # must be active

# nginx
#   access_log ... combined buffer=64k flush=5s
#   error_log ... crit  (on the WS band)
#   logrotate: daily, rotate 14, nocompress
ls -la /var/log/nginx/                        # no multi-GB stragglers

# memory
swapoff -a                                    # + fstab
grep MemoryHigh /etc/systemd/system/bitcraft-mirror.service   # production-sized
systemctl status bitcraft-mirror-ram-guard    # the hard backstop

# logging
journalctl -u bitcraft-mirror -n 20           # info-level, no debug firehose
grep -c DEBUG <(journalctl -u bitcraft-mirror --since -1h)   # expect 0

# verify no disk reads after startup
grep read_bytes /proc/$(pidof spacetimedb-standalone)/io      # flat
awk '/high/{print $2}' /sys/fs/cgroup/system.slice/bitcraft-mirror.service/memory.events.local  # frozen
```

The five fixes are ordered by how much grief each caused us: logging (four
production attempts), writeback/fault reads, then the memory cap. On any
host with real NVMe storage, fixes 2–4 are belt-and-braces; fix 1 and fix 5
apply everywhere.
