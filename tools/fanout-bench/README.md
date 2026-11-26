# fanout-bench

Local experiment: measures the server CPU cost of fanning subscription updates
out to N WebSocket subscribers, per protocol, with and without the
`STDB_WS_SEND_LINGER_MS` batching window. Not part of the workspace build.

- `module/`  synthetic module: four tables shaped like the hot BitCraft tables,
  and a `tick` reducer that updates a few rows per call (one call = one tx).
- `client/`  load client: one writer calling `tick` at a fixed rate, plus N
  subscribers that count frames and bytes.
- `run.sh`   starts a throwaway standalone, publishes, sweeps, prints a table.

`experiments.patch` holds two env-gated switches that `run.py` can drive
(`STDB_WS_SEND_LINGER_MS` via `--lingers`, and `STDB_WS_V1_INLINE_ENCODE_MAX_ROWS`
via `INLINE_V1=`). They are not in the tree; `git apply` the patch to use them.
Without it, `--lingers` values other than 0 have no effect.
