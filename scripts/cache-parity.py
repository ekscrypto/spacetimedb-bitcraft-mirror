#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
"""Diff two relay-cache HTTP endpoints for parity (embedded vs WS-mode).

Primary use: validating the in-process feed (--bitcraft-cache) against the
standalone WebSocket-mode binary running against the same mirror, e.g.:

    # terminal 1: mirror with embedded cache
    spacetimedb-standalone start ... --mirror wss://.../bitcraft-live-7 \
        --bitcraft-cache --cache-bind 127.0.0.1:8089
    # terminal 2: WS-mode cache pointed at the same mirror
    relay-cache --bind 127.0.0.1:8091 --mirrors-url http://127.0.0.1:3130/v1/mirrors \
        --mirror-ws-host 127.0.0.1:3100 --schema-host 127.0.0.1:3100 --schema-db bitcraft-live-7

    python3 scripts/cache-parity.py 127.0.0.1:8089 127.0.0.1:8091

Both sides see the same live upstream, so a row that changed between the two
fetches can legitimately differ; every mismatch is re-checked after a settle
pause and only reported if it persists. Exit 0 = parity, 1 = mismatches.
"""

import json
import sys
import time
import urllib.request

RETRY_SETTLE_SECONDS = 3
SETTLE_ATTEMPTS = 3


def get(host: str, path: str):
    with urllib.request.urlopen(f"http://{host}{path}", timeout=60) as r:
        return json.load(r)


def wait_ready(host: str, timeout_s: float = 1800.0):
    deadline = time.monotonic() + timeout_s
    last = None
    while time.monotonic() < deadline:
        try:
            h = get(host, "/cache-health")
            if h.get("ready") and all(r.get("ready") for r in h.get("regions", [])):
                return h
            last = h
        except Exception as e:  # noqa: BLE001 - polling loop
            last = e
        time.sleep(2)
    raise SystemExit(f"{host} never became ready: {last}")


def canonical(v):
    """Canonicalize for comparison: sort object arrays (store iteration
    order can differ after live churn) and drop fetch-time-computed fields
    (e.g. supplies_run_out = now + supplies duration), then dump stably."""
    if isinstance(v, dict):
        v = {k: canonical(x) for k, x in v.items() if k != "supplies_run_out"}
    if isinstance(v, list) and all(isinstance(x, (dict, list, str, int, float, bool)) or x is None for x in v):
        try:
            return [canonical(x) for x in sorted(v, key=lambda x: json.dumps(x, sort_keys=True, default=str))]
        except TypeError:
            pass
    if isinstance(v, list):
        return [canonical(x) for x in v]
    return v


def same(a, b) -> bool:
    return canonical(a) == canonical(b)


def main() -> int:
    a = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:8089"
    b = sys.argv[2] if len(sys.argv) > 2 else "127.0.0.1:8091"
    ha, hb = wait_ready(a), wait_ready(b)
    print(f"both ready: {a} {json.dumps(ha)}")
    print(f"           {b} {json.dumps(hb)}")

    # Gross store comparison first: /internal/stats row counts per region.
    sa, sb = get(a, "/internal/stats"), get(b, "/internal/stats")
    if same(sa, sb):
        print(f"internal/stats: identical ({json.dumps(sa)[:200]}…)")
    else:
        print("internal/stats differ (live churn may cause small deltas):")
        print(f"  {a}: {json.dumps(sa, sort_keys=True)[:400]}")
        print(f"  {b}: {json.dumps(sb, sort_keys=True)[:400]}")

    # Sample entity ids from side A's public listings.
    paths = ["/proto"]
    deposits = get(a, "/deposits").get("deposits", [])
    claim_ids = [d["entity_id"] for d in deposits][:15]
    for needle in ("a", "e", "o"):
        try:
            for hit in get(a, f"/claim?name={needle}")[:10]:
                if "entity_id" in hit and hit["entity_id"] not in claim_ids:
                    claim_ids.append(hit["entity_id"])
        except Exception as e:  # noqa: BLE001
            print(f"claim search `{needle}` failed on {a}: {e}")
    for cid in claim_ids[:20]:
        paths += [
            f"/claim/{cid}",
            f"/claim/{cid}/inventory",
            f"/claim/{cid}/members",
            f"/claim/{cid}/citizens",
            f"/claim/{cid}/hexcoins",
            f"/claim/{cid}/crafts",
        ]
    # Player ids from the first claim's members response.
    player_ids = []
    for cid in claim_ids[:5]:
        members = get(a, f"/claim/{cid}/members")
        for m in members.get("members", members) if isinstance(members, dict) else members:
            if isinstance(m, dict) and "player_entity_id" in m:
                player_ids.append(m["player_entity_id"])
    for pid in player_ids[:10]:
        paths += [
            f"/player/{pid}",
            f"/player/{pid}/inventory",
            f"/player/{pid}/housing",
            f"/player/{pid}/skills",
            f"/player/{pid}/crafts",
        ]
    paths += ["/deposits"]

    mismatches = []
    checked = 0
    for path in paths:
        checked += 1
        try:
            va, vb = get(a, path), get(b, path)
        except Exception as e:  # noqa: BLE001
            mismatches.append((path, f"fetch error: {e}"))
            continue
        if same(va, vb):
            continue
        # Re-check after settling: the row may have legitimately moved.
        persistent = True
        for _ in range(SETTLE_ATTEMPTS - 1):
            time.sleep(RETRY_SETTLE_SECONDS)
            try:
                va, vb = get(a, path), get(b, path)
            except Exception as e:  # noqa: BLE001
                mismatches.append((path, f"refetch error: {e}"))
                persistent = False
                break
            if same(va, vb):
                persistent = False
                break
        if persistent:
            da = json.dumps(canonical(va), sort_keys=True, default=str)
            db = json.dumps(canonical(vb), sort_keys=True, default=str)
            mismatches.append((path, f"{da[:300]}\n   vs {db[:300]}"))

    print(f"\ncompared {checked} endpoints; {len(mismatches)} persistent mismatches")
    for path, detail in mismatches:
        print(f"MISMATCH {path}\n   {detail}")
    return 1 if mismatches else 0


if __name__ == "__main__":
    sys.exit(main())
