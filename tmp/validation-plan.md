# Dynamic Config Validation Plan

Smart validation layer for `WriteConfig` and `etcdctl put` operations. Two goals:
prevent operationally dangerous writes, and surface relational inconsistencies across
related config keys before they cause production impact.

---

## Validation levels

| Level | Behaviour | Use when |
|---|---|---|
| `OK` | Write proceeds, no output | Value is valid and safe |
| `WARN` | Write proceeds, warning logged + metric incremented | Value is valid but operationally risky |
| `BLOCK` | Write rejected, error returned to caller, metric incremented | Value would cause immediate or severe production impact |

---

## New metrics

Expand `dynconfig_write_total` result tag values:

| Result tag | Meaning |
|---|---|
| `ok` | Write accepted, all checks passed |
| `warned` | Write accepted, one or more WARN checks triggered |
| `blocked` | Write rejected, one or more BLOCK checks triggered |
| `error` | Write failed for a non-validation reason (etcd error, marshal error) |

---

## Check registry design

Each check is a function with signature:

```go
type CheckFunc func(key dynamicconfig.Key, newValues []dynamicconfig.ConstrainedValue, current Snapshot) CheckResult

type CheckResult struct {
    Level   ValidationLevel // OK, WARN, BLOCK
    Message string
}

// Snapshot is a read-only view of the current in-memory map — no etcd round-trip.
type Snapshot interface {
    GetValue(key dynamicconfig.Key) []dynamicconfig.ConstrainedValue
}
```

Checks are evaluated in order. Worst level across all checks wins. All triggered
messages are collected and returned together so the caller sees the full picture,
not just the first failure.

---

## Checks to implement

### Layer 1 — Type and key validity (BLOCK)

- Key is not in Temporal's known dynamic config key registry → BLOCK
- Value type does not match the key's expected type (int, float, bool, duration, string) → BLOCK

### Layer 2 — Zero-value checks

Zero on a rate limiter is a kill switch — every request in that scope is immediately
rejected. These are always BLOCK. Zero on a sizing setting degrades performance but
does not hard-break the system — these are WARN.

**BLOCK on zero:**
- `frontend.globalRPS` = 0 — rejects all frontend requests cluster-wide
- `frontend.globalNamespaceRPS` = 0 — rejects all requests for every namespace
- `frontend.namespaceRPS.visibility` = 0 — rejects all visibility (list) calls
- `frontend.globalNamespaceRPS.visibility` = 0 — same, fleet-wide
- `frontend.namespaceCount` = 0 — no namespaces can be served
- `frontend.globalNamespaceCount` = 0 — same, fleet-wide
- `history.persistenceMaxQPS` = 0 — history service cannot read/write persistence
- `history.persistenceGlobalMaxQPS` = 0 — same, fleet-wide
- `matching.persistenceMaxQPS` = 0 — matching cannot read/write persistence
- `matching.numTaskqueueReadPartitions` = 0 — no task queue reads possible
- `matching.numTaskqueueWritePartitions` = 0 — no task queue writes possible
- `frontend.persistenceMaxQPS` = 0 — frontend cannot read/write persistence

**WARN on zero:**
- `history.cacheMaxSize` = 0 — disables shard cache, causes massive DB load
- `history.hostLevelCacheMaxSizeBytes` = 0 — disables host cache
- `worker.perNamespaceWorkerCount` = 0 — stops internal workers for that namespace
- `worker.schedulerNamespaceStartWorkflowRPS` = 0 — stops scheduled workflow starts

### Layer 3 — Relational checks (cross-key consistency)

These require reading current values of related keys from the in-memory snapshot.

**RPS/QPS hierarchy (WARN):**
- per-namespace RPS (`frontend.globalNamespaceRPS`) > global RPS (`frontend.globalRPS`)
  → a namespace could consume the entire global budget
- per-namespace visibility RPS (`frontend.namespaceRPS.visibility`) > global visibility RPS
  (`frontend.globalNamespaceRPS.visibility`)
- per-shard persistence QPS (`history.persistencePerShardNamespaceMaxQPS`) >
  per-host persistence QPS (`history.persistenceMaxQPS`) / estimated shard count
  → individual shards can exceed host budget

**Partition consistency (WARN):**
- `matching.numTaskqueueReadPartitions` ≠ `matching.numTaskqueueWritePartitions`
  → mismatched partitions cause uneven load distribution and potential task loss

**Cache sizing (WARN):**
- `history.cacheMaxSize` (per-shard) × default shard count > `history.hostLevelCacheMaxSizeBytes`
  → per-shard caches would collectively exceed host cache budget

### Layer 4 — Negative value checks (BLOCK)

Any negative value for RPS, QPS, count, partition, or cache size setting → BLOCK.
Negative values are not meaningful for these settings and result in undefined behaviour.

---

## Scope of relational checks

Relational checks read from the **in-memory snapshot** (the same map `GetValue` uses),
not from etcd directly. This means:
- No extra etcd round-trip on every write
- Consistent with what the server is currently using
- If a related key has never been set, the check uses the Temporal server's compiled-in
  default for that key as the comparison baseline

---

## Integration points

- `WriteConfig` runs all checks before writing to etcd
- Checks that are `WARN` log at WARN level with key, value, and message
- Checks that are `BLOCK` log at ERROR level and return the error to the caller
- All check results (including message) are included in the log and metric tags

---

## Future checks (out of scope for initial implementation)

- Cross-namespace headroom: one namespace's per-namespace limit is so high it could
  starve other namespaces of the global budget
- Persistence QPS: total across all namespaces exceeds DB capacity estimate
- Task queue partition reduction: lowering partitions on a live queue without draining
  → tasks stranded on removed partitions
- Duration sanity: retry policy `initialInterval` > `maximumInterval`
- Frontend keepalive: `keepAliveMinTime` > `keepAliveTimeout`
