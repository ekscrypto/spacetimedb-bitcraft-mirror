#!/usr/bin/env python3
"""Sweep protocols x send-linger against a throwaway local standalone.

usage: run.py [--rate 1260] [--subs 40] [--secs 20] [--lingers 0,2,10,50]
              [--protos v1.json,v1.bsatn,v2.bsatn,v3.bsatn]
Everything lives under tools/fanout-bench/run/ (deleted and recreated per server start).
"""
import argparse, json, os, shutil, subprocess, sys, time, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
BIN = os.path.join(REPO, "target/release/spacetimedb-standalone")
WASM = os.path.join(HERE, "module/target/wasm32-unknown-unknown/release/fanout_bench_module.wasm")
CLIENT = os.path.join(HERE, "client/target/release/fanout-bench-client")
RUN = os.path.join(HERE, "run")
ADDR = "127.0.0.1:3210"
DB = "fanout"


def cpu(pid):
    """(user_s, sys_s) of the server process."""
    out = subprocess.check_output(["ps", "-o", "utime=,stime=", "-p", str(pid)], text=True).split()

    def secs(t):
        parts = t.split(":")
        v = 0.0
        for p in parts:
            v = v * 60 + float(p)
        return v

    return secs(out[0]), secs(out[1])


def metric(name):
    txt = urllib.request.urlopen(f"http://{ADDR}/v1/metrics", timeout=10).read().decode()
    return sum(float(l.rsplit(" ", 1)[1]) for l in txt.splitlines() if l.startswith(name + "{") or l.startswith(name + " "))


def start_server(linger):
    shutil.rmtree(RUN, ignore_errors=True)
    os.makedirs(RUN)
    env = dict(os.environ, RUST_LOG="warn")
    if os.environ.get("INLINE_V1"):
        env["STDB_WS_V1_INLINE_ENCODE_MAX_ROWS"] = os.environ["INLINE_V1"]
    if linger > 0:
        env["STDB_WS_SEND_LINGER_MS"] = str(linger)
    log = open(os.path.join(RUN, "server.log"), "w")
    p = subprocess.Popen([BIN, "start", "--data-dir", os.path.join(RUN, "data"), "--listen-addr", ADDR,
                          "--jwt-key-dir", os.path.expanduser("~/.config/spacetime"), "--non-interactive"],
                         env=env, stdout=log, stderr=subprocess.STDOUT)
    for _ in range(100):
        try:
            urllib.request.urlopen(f"http://{ADDR}/v1/ping", timeout=1)
            break
        except Exception:
            time.sleep(0.2)
    else:
        raise SystemExit("server did not start; see run/server.log")
    r = subprocess.run(["spacetime", "publish", "--server", f"http://{ADDR}", "--bin-path", WASM,
                        "--anonymous", "--yes", DB], capture_output=True, text=True)
    if r.returncode != 0:
        p.kill()
        raise SystemExit("publish failed:\n" + r.stdout + r.stderr)
    return p


def one(pid, proto, subs, rate, secs):
    c = subprocess.Popen([CLIENT, f"ws://{ADDR}", DB, proto, str(subs), str(rate), str(secs)],
                         stdout=subprocess.PIPE, text=True)
    res = None
    for line in c.stdout:
        if line.startswith("WINDOW_START"):
            u0, s0 = cpu(pid)
            m0 = metric("spacetime_websocket_sent_msg_size_bytes_count")
            z0 = metric("spacetime_websocket_serialize_secs_sum")
        elif line.startswith("WINDOW_END"):
            u1, s1 = cpu(pid)
            m1 = metric("spacetime_websocket_sent_msg_size_bytes_count")
            z1 = metric("spacetime_websocket_serialize_secs_sum")
            kv = dict(x.split("=") for x in line.split()[1:])
            res = dict(proto=proto, subs=subs, frames=int(kv["frames"]), bytes=int(kv["bytes"]),
                       calls=int(kv["calls"]), secs=secs, user=u1 - u0, sys=s1 - s0,
                       payloads=m1 - m0, serialize=z1 - z0)
    c.kill()
    time.sleep(1.5)
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=float, default=1260)
    ap.add_argument("--subs", type=int, default=40)
    ap.add_argument("--secs", type=int, default=20)
    ap.add_argument("--lingers", default="0,2,10,50")
    ap.add_argument("--protos", default="v1.json,v1.bsatn,v2.bsatn,v3.bsatn")
    ap.add_argument("--out", default=os.path.join(HERE, "results.jsonl"))
    a = ap.parse_args()
    out = open(a.out, "a")
    print(f"{'linger':>6} {'proto':>9} {'subs':>4} {'tx/s':>6} {'frames/s':>9} {'B/frame':>8} {'Mbit/s':>7} "
          f"{'user%':>6} {'sys%':>6} {'cpu%':>6} {'ser%':>5}")
    for linger in [float(x) for x in a.lingers.split(",")]:
        p = start_server(linger)
        try:
            runs = [("v2.bsatn", 0)] + [(pr, a.subs) for pr in a.protos.split(",")]
            for proto, subs in runs:
                r = one(p.pid, proto, subs, a.rate, a.secs)
                if r is None:
                    print(f"{linger:>6} {proto:>9} {subs:>4}  FAILED")
                    continue
                r.update(linger=linger, rate=a.rate)
                out.write(json.dumps(r) + "\n")
                out.flush()
                s = r["secs"]
                print(f"{linger:>6} {proto if subs else 'baseline':>9} {subs:>4} {r['calls']/s:>6.0f} {r['frames']/s:>9.0f} "
                      f"{(r['bytes']/r['frames'] if r['frames'] else 0):>8.0f} {r['bytes']*8/s/1e6:>7.2f} "
                      f"{100*r['user']/s:>6.1f} {100*r['sys']/s:>6.1f} {100*(r['user']+r['sys'])/s:>6.1f} "
                      f"{100*r['serialize']/s:>5.1f}", flush=True)
        finally:
            p.terminate()
            try:
                p.wait(10)
            except Exception:
                p.kill()


if __name__ == "__main__":
    main()
