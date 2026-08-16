#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
"""Sampling monitor for scripts/local-full-fleet-test.sh.

Polls the mirror status sidecar (/v1/mirrors), the embedded cache
(/cache-health + /internal/stats), and the mirror process RSS/CPU until every
expected database has been live and a steady-state soak has elapsed, then
writes summary.md + summary.json next to the samples.

Exit codes: 0 completed, 2 RSS guard tripped (mirror killed), 3 mirror exited,
130 interrupted. State is checkpointed to watch-state.json so a killed monitor
can be restarted without losing first-live timestamps.
"""

import json
import os
import signal
import subprocess
import sys
import time
import urllib.request
from datetime import datetime
from pathlib import Path


def env(name, default):
    v = os.environ.get(name)
    return v if v not in (None, "") else default


def db_name(tok):
    if tok == "global":
        return "bitcraft-live-global"
    if tok.startswith("bitcraft-live-"):
        return tok
    return f"bitcraft-live-{tok}"


RUN_DIR = Path(env("RUN_DIR", "/tmp/bitcraft-full-fleet"))
STATUS_ADDR = env("STATUS_ADDR", "127.0.0.1:3130")
CACHE_BIND = env("CACHE_BIND", "127.0.0.1:8089")
SOAK_SECONDS = float(env("SOAK_MINUTES", "60")) * 60
RSS_GUARD_MB = float(env("RSS_GUARD_MB", "49152"))
SAMPLE_SECONDS = float(env("SAMPLE_SECONDS", "30"))
EXPECTED = [db_name(t) for t in env("REGIONS", "global 3 7 8 9 11 12 13 14 15 17 18 19 23").split()]

PID_FILE = RUN_DIR / "mirror.pid"
JSONL = RUN_DIR / "monitor.jsonl"
STATE_FILE = RUN_DIR / "watch-state.json"
SUMMARY_MD = RUN_DIR / "summary.md"
SUMMARY_JSON = RUN_DIR / "summary.json"

ACTIVE_STATES = {"connecting", "subscribing", "live"}
stop_requested = False


def get_json(host, path, timeout=10):
    try:
        with urllib.request.urlopen(f"http://{host}{path}", timeout=timeout) as r:
            return json.load(r)
    except Exception:
        return None


def iso(epoch):
    return datetime.fromtimestamp(epoch).astimezone().isoformat(timespec="seconds")


def hms(seconds):
    seconds = max(0, int(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def gib(kb):
    return f"{kb / 1048576:.2f} GiB"


def read_pid():
    try:
        return int(PID_FILE.read_text().strip())
    except Exception:
        return None


def pid_alive(pid):
    if pid is None:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def cputime_seconds(text):
    """Parse ps cputime: [[dd-]hh:]mm:ss[.cc]."""
    days = 0
    if "-" in text:
        days_s, text = text.split("-", 1)
        days = int(days_s)
    parts = text.split(":")
    parts = [float(p) for p in parts]
    while len(parts) < 3:
        parts.insert(0, 0)
    h, m, s = parts[-3:]
    return days * 86400 + h * 3600 + m * 60 + s


def sample_process(pid):
    try:
        out = subprocess.run(
            ["ps", "-o", "rss=,cputime=", "-p", str(pid)],
            capture_output=True, text=True, timeout=10,
        ).stdout.strip()
        if not out:
            return None
        rss_kb_s, cpu_s = out.split()
        return int(rss_kb_s), cputime_seconds(cpu_s)
    except Exception:
        return None


def new_state():
    return {
        "monitor_start": time.time(),
        "mirror_start": None,
        "last_conn": {},
        "first_active": {},
        "first_live": {},
        "live_rss_kb": {},
        "live_tx": {},
        "disconnects": {},
        "transitions": [],
        "all_live": None,
        "rss_peak_kb": 0,
        "cpu_seconds_total": None,
        "cores_seeding_sum": 0.0,
        "cores_seeding_n": 0,
        "cores_seeding_max": 0.0,
        "cores_steady_sum": 0.0,
        "cores_steady_n": 0,
        "cores_steady_max": 0.0,
        "samples": 0,
        "last_stats": None,
        "failed": None,
    }


def load_state():
    st = new_state()
    try:
        saved = json.loads(STATE_FILE.read_text())
        st.update(saved)
    except Exception:
        pass
    return st


def save_state(st):
    STATE_FILE.write_text(json.dumps(st, indent=1))


def progress_line(t, st, mirrors, cache):
    live = sum(1 for d in EXPECTED if d in st["first_live"])
    sub = ""
    for db, m in (mirrors or {}).items():
        if m.get("connectivity") == "subscribing":
            rows = m.get("current_table_seed_rows") or 0
            done = m.get("current_table_seed_rows_applied") or 0
            pct = f" {100 * done // rows}%" if rows else ""
            sub = f" | seeding {db.removeprefix('bitcraft-live-')}:{m.get('current_table')}{pct}"
            break
    disc = sum(st["disconnects"].values())
    cr = cache or {}
    cache_s = f" | cache {cr.get('ready')}/{len(cr.get('regions') or [])}" if cr else " | cache -"
    elapsed = t - (st["mirror_start"] or t)
    rss = st["rss_peak_kb"]
    print(
        f"[+{hms(elapsed)}] live {live}/{len(EXPECTED)}{sub}{cache_s}"
        f" | rss_peak {gib(rss)} | disc {disc}",
        flush=True,
    )


def finalize(st, code):
    t = time.time()
    save_state(st)
    stats = st["last_stats"] or {}
    regions = stats.get("regions") or []

    lines = []
    outcome = st["failed"] or ("completed" if st["all_live"] else "incomplete")
    lines.append("# Local full-fleet test summary")
    lines.append("")
    lines.append(f"- outcome: **{outcome}**")
    if st["mirror_start"]:
        lines.append(f"- mirror start: {iso(st['mirror_start'])}")
        lines.append(f"- monitor end:  {iso(t)} (elapsed {hms(t - st['mirror_start'])})")
    if st["all_live"]:
        lines.append(f"- all {len(EXPECTED)} databases live at: {iso(st['all_live'])} "
                     f"(+{hms(st['all_live'] - (st['mirror_start'] or st['all_live']))} from start)")
    lines.append(f"- samples: {st['samples']}")
    lines.append(f"- peak RSS: {gib(st['rss_peak_kb'])}")

    if st["cores_seeding_n"]:
        lines.append(f"- CPU while seeding: avg {st['cores_seeding_sum'] / st['cores_seeding_n']:.1f} cores, "
                     f"peak {st['cores_seeding_max']:.1f} cores")
    if st["cores_steady_n"]:
        lines.append(f"- CPU after all-live: avg {st['cores_steady_sum'] / st['cores_steady_n']:.1f} cores, "
                     f"peak {st['cores_steady_max']:.1f} cores")
    disc_total = sum(st["disconnects"].values())
    lines.append(f"- disconnect events: {disc_total}"
                 + (f" {st['disconnects']}" if disc_total else ""))

    lines.append("")
    lines.append("## Per-database time to live")
    lines.append("")
    lines.append("| database | first active | live at | seed time | Δ from previous live | RSS at live | transactions |")
    lines.append("|---|---|---|---|---|---|---|")
    prev_live = None
    start = st["mirror_start"] or st["monitor_start"]
    ordered = sorted(st["first_live"], key=lambda d: st["first_live"][d])
    for db in EXPECTED:
        if db not in st["first_live"]:
            lines.append(f"| {db} | - | never | - | - | - | - |")
    for db in ordered:
        fa, fl = st["first_active"].get(db), st["first_live"][db]
        delta = f"+{hms(fl - prev_live)}" if prev_live is not None else "first"
        prev_live = fl
        rss = gib(st["live_rss_kb"][db]) if db in st["live_rss_kb"] else "-"
        seed = hms(fl - fa) if fa else "-"
        tx = st["live_tx"].get(db, "-")
        lines.append(f"| {db} | +{hms(fa - start) if fa else '-'} | +{hms(fl - start)} | {seed} | {delta} | {rss} | {tx} |")

    if regions:
        lines.append("")
        lines.append("## Cache rows (last /internal/stats snapshot)")
        lines.append("")
        lines.append("| region | ready | total rows | top tables |")
        lines.append("|---|---|---|---|")
        for r in sorted(regions, key=lambda r: r["region"]):
            rows = r.get("rows") or {}
            total = sum(rows.values())
            top = ", ".join(f"{k}={v}" for k, v in sorted(rows.items(), key=lambda kv: -kv[1])[:5])
            lines.append(f"| {r['region']} | {r.get('ready')} | {total} | {top} |")

    if st["transitions"]:
        lines.append("")
        lines.append("## Connectivity transitions of interest")
        lines.append("")
        for tr in st["transitions"]:
            if tr["to"] in ("disconnected",) or (tr["to"] == "live" and tr["from"] != "subscribing"):
                lines.append(f"- {iso(tr['t'])} {tr['db']}: {tr['from'] or '∅'} → {tr['to']}")

    SUMMARY_MD.write_text("\n".join(lines) + "\n")
    SUMMARY_JSON.write_text(json.dumps({
        "outcome": outcome,
        "expected": EXPECTED,
        "state": st,
        "last_stats": st["last_stats"],
    }, indent=1))
    print(f"summary written: {SUMMARY_MD}", flush=True)
    sys.exit(code)


def on_signal(signum, frame):
    global stop_requested
    stop_requested = True


def main():
    global stop_requested
    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    pid = read_pid()
    st = load_state()
    if st["mirror_start"] is None:
        try:
            st["mirror_start"] = PID_FILE.stat().st_mtime
        except Exception:
            st["mirror_start"] = time.time()
    if not pid_alive(pid):
        st["failed"] = "mirror-exited-before-monitor"
        finalize(st, 3)

    print(f"monitoring pid {pid}, expecting {len(EXPECTED)} databases: {', '.join(EXPECTED)}", flush=True)
    print(f"soak {SOAK_SECONDS / 60:.0f} min after all-live, RSS guard {RSS_GUARD_MB:.0f} MB, "
          f"sampling every {SAMPLE_SECONDS:.0f}s", flush=True)

    last_t = time.time()
    while True:
        t = time.time()
        proc = sample_process(pid)
        if proc is None or not pid_alive(pid):
            st["failed"] = "mirror-exited"
            finalize(st, 3)
        rss_kb, cpu_total = proc
        dt = max(1e-9, t - last_t)
        cores = max(0.0, (cpu_total - st["cpu_seconds_total"]) / dt) if st["cpu_seconds_total"] is not None else 0.0
        st["cpu_seconds_total"] = cpu_total
        st["rss_peak_kb"] = max(st["rss_peak_kb"], rss_kb)
        bucket = "steady" if st["all_live"] else "seeding"
        st[f"cores_{bucket}_sum"] += cores
        st[f"cores_{bucket}_n"] += 1
        st[f"cores_{bucket}_max"] = max(st[f"cores_{bucket}_max"], cores)

        resp = get_json(STATUS_ADDR, "/v1/mirrors")
        mirrors = {m["database"]: m for m in (resp or {}).get("mirrors", [])}
        for db, m in mirrors.items():
            conn = m.get("connectivity")
            prev = st["last_conn"].get(db)
            if conn != prev:
                st["transitions"].append({"t": t, "db": db, "from": prev, "to": conn})
                if conn == "disconnected" and prev in ACTIVE_STATES:
                    st["disconnects"][db] = st["disconnects"].get(db, 0) + 1
                if conn in ACTIVE_STATES and db not in st["first_active"]:
                    st["first_active"][db] = t
                if conn == "live" and db not in st["first_live"]:
                    st["first_live"][db] = t
                    st["live_rss_kb"][db] = rss_kb
                    st["live_tx"][db] = m.get("transactions_processed", 0)
                    print(f"{iso(t)} {db} LIVE (+{hms(t - st['mirror_start'])} from start, "
                          f"rss {gib(rss_kb)})", flush=True)
                st["last_conn"][db] = conn
            elif conn == "live":
                st["live_tx"][db] = max(st["live_tx"].get(db, 0), m.get("transactions_processed", 0))

        cache_health = get_json(CACHE_BIND, "/cache-health")
        stats = get_json(CACHE_BIND, "/internal/stats")
        if stats:
            st["last_stats"] = stats
        cache_regions = {}
        for r in (stats or {}).get("regions") or []:
            cache_regions[r["region"]] = r.get("ready")

        missing = [db for db in EXPECTED if db not in st["first_live"]]
        if not missing and st["all_live"] is None:
            st["all_live"] = t
            print(f"{iso(t)} ALL {len(EXPECTED)} DATABASES LIVE; soak starts "
                  f"({SOAK_SECONDS / 60:.0f} min)", flush=True)

        st["samples"] += 1
        with JSONL.open("a") as f:
            f.write(json.dumps({
                "t": t,
                "rss_kb": rss_kb,
                "cpu_seconds_total": cpu_total,
                "cores": round(cores, 3),
                "all_live": st["all_live"],
                "live_n": len(st["first_live"]),
                "mirrors": {db: {
                    "c": m.get("connectivity"),
                    "tl": m.get("tables_live"),
                    "tt": m.get("tables_total"),
                    "tx": m.get("transactions_processed"),
                    "ct": m.get("current_table"),
                    "sr": m.get("current_table_seed_rows"),
                    "sa": m.get("current_table_seed_rows_applied"),
                } for db, m in mirrors.items()},
                "cache_ready": (cache_health or {}).get("ready"),
                "cache_regions": cache_regions,
                "memory_pressure": (stats or {}).get("memory_pressure"),
            }) + "\n")

        if rss_kb / 1024 > RSS_GUARD_MB:
            st["failed"] = f"rss-guard ({gib(rss_kb)} > {RSS_GUARD_MB / 1024:.0f} GiB)"
            save_state(st)
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            finalize(st, 2)

        if stop_requested:
            st["failed"] = "interrupted"
            finalize(st, 130)

        if st["all_live"] is not None and t - st["all_live"] >= SOAK_SECONDS:
            finalize(st, 0)

        progress_line(t, st, mirrors, cache_health)
        save_state(st)
        last_t = t
        time.sleep(SAMPLE_SECONDS)


if __name__ == "__main__":
    main()
