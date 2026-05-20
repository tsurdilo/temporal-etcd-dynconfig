# temporal-etcd-dynconfig

OSS Temporal Server ships with a file-based dynamic config client. It works, but it has real operational limits: you edit a YAML file, wait up to 10 seconds for the poll interval, and repeat that edit on every server host. In a multi-host or multi-cluster deployment this becomes error-prone — hosts can diverge silently, passive clusters drift from active ones, and there is no audit trail for what changed when.

This library replaces that client with one backed by etcd. All Temporal server hosts watch the same etcd prefix and receive config changes simultaneously via etcd's watch API — no polling, no per-host file management, no drift. A single `etcdctl put` (or a call to `WriteConfig`) propagates to every host in the cluster within milliseconds.

It implements both `dynamicconfig.Client` and `dynamicconfig.NotifyingClient`, so Temporal uses push-based updates rather than polling. It is a drop-in replacement: wire it in at server startup, point it at your etcd cluster, and the rest of your server code is unchanged.

**When to use this:**
- You run multiple Temporal server hosts and want config changes applied simultaneously across all of them
- You run active/passive multi-cluster Temporal and want a single source of truth for dynamic config
- You run multiple environments (prod, staging, dev) and want prefix-isolated config on a shared etcd cluster
- You want an audit log of every config change with old and new values

## Table of contents

- [How it works](#how-it-works)
- [Prerequisites](#prerequisites)
- [Repository structure](#repository-structure)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Storing dynamic config values in etcd](#storing-dynamic-config-values-in-etcd)
- [Startup behaviour](#startup-behaviour)
- [Shutdown](#shutdown)
- [Connection resilience](#connection-resilience)
- [Metrics](#metrics)
- [Differences from the OSS file-based client](#differences-from-the-oss-file-based-client)
- [Multi-environment setup](#multi-environment-setup)
- [Active/passive multi-cluster setup](#activepassive-multi-cluster-setup)
- [Local etcd for development](#local-etcd-for-development)
- [Production notes](#production-notes)

## How it works

- On startup, bulk-loads all keys under `globalKeyPrefix` from etcd into an in-memory map
- Opens an etcd `Watch` stream on the prefix — changes propagate immediately
- Implements both `dynamicconfig.Client` and `dynamicconfig.NotifyingClient`, so Temporal uses push-based updates instead of polling
- The watch supervisor handles etcd compaction, leader election, and connection resets transparently — reloads all values and opens a fresh stream on any disruption
- Every key change is logged at INFO with old and new values diffed

## Prerequisites

- Go 1.22+
- A running etcd cluster (v3.5+)
- OSS Temporal server

## Repository structure

```
atomic.go     atomicValue[T] — typesafe sync/atomic.Value wrapper
client.go     Dynamic config client: GetValue, Subscribe, WriteConfig, DumpAll, LogAll, watch loop
config.go     Config/EtcdConfig structs, YAML tags, validation, BuildConfig helper
provider.go   NewEtcdClient — clientv3.Client with round-robin LB and startup health check
tls.go        newClientMTLSConfig — stdlib mTLS helper (cert/key/CA files)
```

## Installation

This library does not import `go.temporal.io/server` from the public module proxy — it requires a local checkout of the Temporal server source. This is intentional: the library compiles against the same server version you are running, so there is no version skew between the dynamic config client and the server internals it integrates with.

**Step 1** — check out the Temporal server at the release tag matching the version you are deploying:

```bash
git clone https://github.com/temporalio/temporal.git /path/to/temporal
cd /path/to/temporal
git checkout v1.31.0   # use the tag matching your deployment
```

**Step 2** — in your `go.mod`, add a replace directive pointing at that checkout:

```
replace go.temporal.io/server => /path/to/temporal
```

> **Important:** always point the replace directive at a release tag checkout, not `master`. The `master` branch of the Temporal server uses pre-release versions of `go.temporal.io/api` that are not published to the module proxy, which will break `go mod tidy` for anyone who does not also have those pre-release modules locally.

## Configuration

Config is a plain Go struct — populate it directly or unmarshal it from YAML.

### Minimal (no TLS, local etcd)

```go
cfg := etcddynconfig.Config{
    EtcdConfigs: []etcddynconfig.EtcdConfig{
        {Name: "primary", Endpoints: []string{"127.0.0.1:2379"}},
    },
    GlobalKeyPrefix: "/temporal/dynamicconfig/",
    DisableTLS:      true,
    ClientName:      "temporal-server",
}
cfg.EnsureDefaults()
```

Equivalent YAML (e.g. loaded from a file and passed to `BuildConfig`):

```yaml
etcdConfigs:
  - name: primary
    endpoints:
      - "127.0.0.1:2379"
globalKeyPrefix: "/temporal/dynamicconfig/"
disableTLS: true
clientName: temporal-server
```

### With mTLS

```yaml
etcdConfigs:
  - name: primary
    endpoints:
      - "etcd-1.example.com:2379"
      - "etcd-2.example.com:2379"
globalKeyPrefix: "/temporal/dynamicconfig/"
disableTLS: false
clientTlsCaCertFile:   /etc/temporal/certs/ca.crt
clientTlsCertFile:     /etc/temporal/certs/client.crt
clientTlsKeyFile:      /etc/temporal/certs/client.key
clientName:            temporal-server
dialTimeout:           2s
maxCallSendMsgSize:    4194304
```

### Config fields

| Field | Required | Default | Description |
|---|---|---|---|
| `etcdConfigs` | yes | — | List of etcd clusters. Currently only the first entry is used. |
| `globalKeyPrefix` | yes | — | Prepended to every key. Use a unique prefix per environment for isolation. |
| `clientName` | yes | — | Used for TLS SNI and log context. |
| `disableTLS` | no | `false` | Set `true` for local dev without certs. |
| `clientTlsCaCertFile` | if TLS | — | PEM CA certificate for verifying the etcd server. |
| `clientTlsCertFile` | if TLS | — | PEM client certificate for mTLS. |
| `clientTlsKeyFile` | if TLS | — | PEM client private key for mTLS. |
| `dialTimeout` | no | `2s` | Timeout for the initial etcd connection. |
| `maxCallSendMsgSize` | no | `4 MiB` | Max gRPC message size. Must match etcd server's `--max-request-bytes`. |

## Usage

### Wire into OSS Temporal server

```go
package main

import (
    "context"

    etcddynconfig "github.com/temporalio/temporal-etcd-dynconfig"
    "go.temporal.io/server/common/log"
    "go.temporal.io/server/temporal"
)

func main() {
    logger := log.NewZapLogger(log.BuildZapLogger(log.Config{Level: "info"}))
    ctx := context.Background()

    cfg := etcddynconfig.Config{
        EtcdConfigs: []etcddynconfig.EtcdConfig{
            {Name: "primary", Endpoints: []string{"127.0.0.1:2379"}},
        },
        GlobalKeyPrefix: "/temporal/dynamicconfig/",
        DisableTLS:      true,
        ClientName:      "temporal-server",
    }
    cfg.EnsureDefaults()

    // Create the raw etcd client (performs startup connectivity check).
    etcdClient := etcddynconfig.NewEtcdClient(cfg, logger)
    defer etcdClient.Close()

    // Create the dynamic config client.
    // Pass your server's metrics.Handler so dynconfig metrics are emitted alongside
    // all other Temporal server metrics. Use metrics.NoopMetricsHandler to opt out.
    dcClient, err := etcddynconfig.NewClient(ctx, etcdClient, cfg.GlobalKeyPrefix, logger, metricsHandler)
    if err != nil {
        logger.Fatal("failed to create dynamic config client", ...)
    }
    defer dcClient.Stop()

    // Pass it to the Temporal server.
    server, err := temporal.NewServer(
        temporal.WithConfig(temporalCfg),
        temporal.WithDynamicConfigClient(dcClient),
        // ... other options
    )
    if err != nil {
        logger.Fatal("failed to create server", ...)
    }
    server.Start()
}
```

### Load config from YAML

```go
import "gopkg.in/yaml.v3"

var raw map[string]any
_ = yaml.Unmarshal(yamlBytes, &raw)

cfg, err := etcddynconfig.BuildConfig(raw)
if err != nil {
    // validation error
}
```

`BuildConfig` validates all required fields and fills in defaults. Use it when loading config from a file or a custom datastore options map.

## Storing dynamic config values in etcd

Each key is stored as `<globalKeyPrefix><temporalKeyName>`. The value is a YAML list of constrained values — the same format as the OSS file-based dynamic config.

The recommended prefix is `/temporal/dynamicconfig/` (note the leading slash). The leading slash is required for etcd UI tools like etcdkeeper to display keys in a proper directory tree. Without it, keys are stored at the root level and most UIs won't show them.

### Simple global value

```yaml
# etcd key: /temporal/dynamicconfig/frontend.rps
- value: 1200
  constraints: {}
```

### Per-namespace override with global fallback

```yaml
# etcd key: /temporal/dynamicconfig/frontend.rps
- value: 500
  constraints:
    namespace: high-traffic-namespace
- value: 1200
  constraints: {}
```

### Supported constraint fields

| Constraint key | Type | Description |
|---|---|---|
| `namespace` | string | Namespace name |
| `namespaceId` | string | Namespace ID |
| `taskQueueName` | string | Task queue name |
| `taskType` | string or int | `Workflow` or `Activity` |
| `historyTaskType` | string or int | Internal history task type |
| `shardId` | int | History shard ID |
| `destination` | string | Nexus destination |

Temporal evaluates constraints in precedence order (most specific wins). A value with `constraints: {}` acts as the global default.

### Writing values programmatically

```go
import "go.temporal.io/server/common/dynamicconfig"

err := dcClient.WriteConfig(ctx,
    dynamicconfig.FrontendRPS,
    []dynamicconfig.ConstrainedValue{
        {
            Value:       500,
            Constraints: dynamicconfig.Constraints{Namespace: "high-traffic-namespace"},
        },
        {
            Value: 1200,
        },
    },
)
```

`WriteConfig` serializes the values to YAML, writes them to etcd, and immediately reloads the in-memory cache. Intended for CLI tooling and bootstrappers; not for hot paths.

### Inspecting the loaded config (DumpAll / LogAll)

OSS Temporal has no built-in way to see what dynamic config values are currently
active. The etcd client adds two methods for this.

**`DumpAll()`** returns a snapshot of the full in-memory map as
`map[string][]dynamicconfig.ConstrainedValue`. The map is a copy — safe to
iterate after the client is stopped:

```go
snapshot := dcClient.DumpAll()
for key, values := range snapshot {
    fmt.Printf("%s: %+v\n", key, values)
}
```

Typical uses:
- Expose it from a debug HTTP handler so you can `curl` the live state
- Log it at startup to confirm all expected overrides were loaded from etcd
- Diff two snapshots to see what changed between deployments

**`LogAll()`** writes every key and its constrained values to the logger at INFO
level — one log line per key. Useful as a startup diagnostic without any extra
wiring:

```go
// call once after NewClient returns, before starting the server
dcClient.LogAll()
```

Example output (structured logging):

```
dynamic config dump start   totalKeys=12
dynamic config entry        key=frontend.rps          values=[{constraints:{} value:1200}]
dynamic config entry        key=history.cacheMaxSize  values=[{constraints:{} value:512}]
...
dynamic config dump end
```

Both methods read directly from the same atomic in-memory map that `GetValue`
uses — no etcd round-trip, no lock contention.

### Writing values with etcdctl

```bash
etcdctl put /temporal/dynamicconfig/frontend.rps -- '
- value: 1200
  constraints: {}
'

etcdctl put /temporal/dynamicconfig/history.defaultActivityRetryPolicy -- '
- value:
    initialInterval: 1s
    backoffCoefficient: 2.0
    maximumAttempts: 10
  constraints: {}
'
```

> **Note:** the `--` separator is required when the value starts with `-` (a YAML list), otherwise etcdctl interprets it as a flag.

### Deleting a value (reverts to compiled-in default)

```bash
etcdctl del /temporal/dynamicconfig/frontend.rps
```

### Listing all current dynamic config values

```bash
etcdctl get /temporal/dynamicconfig/ --prefix
```

## Startup behaviour

`NewEtcdClient` performs a connectivity check before returning. It retries up to 3 times with exponential backoff (2s initial, 2× coefficient). If etcd is unreachable it calls `logger.Fatal` — the server should not start with a broken config backend.

## Shutdown

```go
defer dcClient.Stop()      // closes the etcd watcher, cancels watch goroutines
defer etcdClient.Close()   // closes the underlying gRPC connection
```

Call `Stop()` before `Close()`.

## Connection resilience

The watch supervisor handles:

| Event | Behaviour |
|---|---|
| Transient stream error | Reload all values, reopen Watch from new revision |
| etcd compaction past last-seen revision | Same — reloads and resubscribes |
| Leader election / connection reset | Same |
| Context cancellation (`Stop()`) | Exits cleanly, no reload |

Backoff on reload failure: 100ms → doubles each attempt → caps at 30s.

## Metrics

The client emits metrics through the same `metrics.Handler` the Temporal server already uses — Prometheus, OpenTelemetry, or any other backend your server is configured with. Pass the server's scoped handler so metrics are automatically tagged with `service` and `host_name` alongside all other server metrics.

```go
dcClient, err := etcddynconfig.NewClient(ctx, etcdClient, prefix, logger, metricsHandler)
```

Pass `metrics.NoopMetricsHandler` to disable metrics entirely.

### Emitted metrics

| Metric | Type | Tags | Description |
|---|---|---|---|
| `dynconfig_key_updates_total` | counter | `operation` (DynamicConfigUpdate, DynamicConfigDelete), `key` (config key name), `service_name` | Incremented on every key change received from etcd. Each server process increments independently — with 3 frontends + 5 history hosts + 2 matching + 1 worker, a single `etcdctl put` produces 11 increments across all services. |
| `dynconfig_watch_reconnects_total` | counter | `reason` (compacted, stream_ended) | Incremented whenever the watch supervisor has to reload and reopen the stream. A spike here indicates etcd instability. |
| `dynconfig_watch_active` | gauge | — | `1` while the watch stream is running, `0` while stopped or reconnecting. Alert on this going to `0`. |
| `dynconfig_keys_loaded` | gauge | — | Number of keys in the in-memory map after each full reload. |
| `dynconfig_load_duration_seconds` | timer | — | Time taken for a full prefix scan from etcd, on startup and each reconnect. |
| `dynconfig_write_total` | counter | `result` (success, error) | Outcome of each `WriteConfig` call. |

### Useful alert queries

```promql
# Watch is down on any service — config changes are not propagating
dynconfig_watch_active{service=~"frontend|history|matching|worker"} == 0

# A config change was applied on some services but not all within 30s
# (indicates a broken watch on specific nodes)
max(timestamp(dynconfig_key_updates_total)) - min(timestamp(dynconfig_key_updates_total)) > 30

# Frequent watch reconnects — etcd is unstable
rate(dynconfig_watch_reconnects_total[5m]) > 0.1
```

## Differences from the OSS file-based client

| | File-based client | etcd client |
|---|---|---|
| Update latency | Poll interval (default 10s) | Near-realtime via etcd watch |
| Write path | Edit file on disk | `WriteConfig()` or `etcdctl put` |
| Multi-server consistency | Depends on filesystem / config management | All servers in the cluster see the same value simultaneously |
| Resilience | File must be present at startup | Fails fast if etcd unreachable at startup; survives disruptions at runtime |
| Audit log | None | Every change logged at INFO with old/new values |

## Active/passive multi-cluster setup

In a multi-cluster Temporal deployment (active + one or more passive/standby clusters), dynamic config must be kept in sync across all clusters. With file-based dynamic config this is a manual and error-prone process — you edit a file on one cluster, then remember to apply the same change to every passive cluster. Miss one, and your standby diverges silently. When you fail over, the passive cluster runs with stale config.

With etcd-backed dynamic config, all clusters share a single source of truth. A single `etcdctl put` propagates to every cluster simultaneously — active and passive — with no manual steps.

### How it works with shared etcd

All clusters point at the same etcd cluster, each using its own prefix:

```
/active/temporal/dynamicconfig/
/passive-us-west/temporal/dynamicconfig/
/passive-eu/temporal/dynamicconfig/
```

Set `ETCD_KEY_PREFIX` per cluster accordingly. Each cluster watches only its own prefix — the prefixes are isolated, so a change to the active cluster does not automatically touch the passive clusters.

### Keeping passive clusters in sync

To apply a change to all clusters at once, write to all prefixes in a single operation:

```bash
# Update frontend.globalNamespaceRPS on every cluster simultaneously
for prefix in /active /passive-us-west /passive-eu; do
  etcdctl put "${prefix}/temporal/dynamicconfig/frontend.globalNamespaceRPS" -- "- value: 2000"
done
```

All clusters receive the watch event and apply the change within milliseconds — no SSH, no config management tooling, no per-cluster scripts.

### Why this matters for failover

When a passive cluster becomes active during a failover, it is already running with the exact same dynamic config as the cluster it is replacing. There is no config drift to discover under pressure. Rate limits, cache sizes, partition counts, and persistence QPS settings are all identical — the failover is behaviorally transparent.

Without this, a common failure mode after failover is: the passive cluster has outdated dynamic config (lower rate limits, wrong partition counts, stale feature flags) and starts behaving differently under production load, compounding the incident.

### Shared vs. per-cluster etcd

You can also run a dedicated etcd cluster per Temporal cluster. In that case there is no prefix isolation needed, but you lose the single-write-to-all-clusters convenience. Use shared etcd when your clusters are in the same region or trust boundary; use per-cluster etcd when clusters are geographically separated and you want full isolation.

## Local etcd for development

```bash
# Single-node etcd via Docker
docker run -d \
  --name etcd \
  -p 2379:2379 \
  gcr.io/etcd-development/etcd:v3.5.12 \
  etcd \
  --advertise-client-urls http://0.0.0.0:2379 \
  --listen-client-urls http://0.0.0.0:2379

# Verify
etcdctl --endpoints=127.0.0.1:2379 endpoint health

# Verify keys are visible
etcdctl --endpoints=127.0.0.1:2379 get /temporal/dynamicconfig/ --prefix --keys-only
```

Then use `disableTLS: true` in your config.

## Multi-environment setup

A single etcd cluster can serve multiple Temporal environments (prod, staging, dev) by giving each a unique `globalKeyPrefix`. Each cluster only reads and watches its own prefix — a change to a staging key never touches prod.

Recommended prefix convention:

```
/prod/temporal/dynamicconfig/
/staging/temporal/dynamicconfig/
/dev/temporal/dynamicconfig/
```

Set the prefix via the `ETCD_KEY_PREFIX` environment variable (or equivalent config) per deployment:

```bash
# prod
ETCD_KEY_PREFIX=/prod/temporal/dynamicconfig/

# staging
ETCD_KEY_PREFIX=/staging/temporal/dynamicconfig/

# dev
ETCD_KEY_PREFIX=/dev/temporal/dynamicconfig/
```

To update a value for staging only:

```bash
etcdctl put /staging/temporal/dynamicconfig/frontend.globalNamespaceRPS -- "- value: 800"
```

Prod is unaffected. To list all keys for a specific environment:

```bash
etcdctl get /prod/temporal/dynamicconfig/ --prefix --keys-only
etcdctl get /staging/temporal/dynamicconfig/ --prefix --keys-only
```

The default seeding from `defaults.yaml` applies independently per prefix — each environment gets its own copy of the defaults on first start.

## Production notes

- **Prefix isolation**: use a unique `globalKeyPrefix` per cell or environment (e.g. `prod-us-east/dynamicconfig/`, `staging/dynamicconfig/`) to avoid key collisions when multiple clusters share an etcd cluster.
- **etcd sizing**: dynamic config values are small and infrequently written. A 3-node etcd cluster used solely for this purpose can be very lightweight.
- **TLS**: always enable mTLS in production. Generate client certs with the same CA as your etcd cluster.
- **Temporal server version**: pin your replace directive to a release tag, not `master`. See the Installation section.
