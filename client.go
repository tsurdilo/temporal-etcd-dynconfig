// Package etcddynconfig implements a Temporal dynamic config client backed by
// etcd.
//
// Wire it into an OSS Temporal server with:
//
//	temporal.WithDynamicConfigClient(client)
//
// Keys are stored in etcd under <globalKeyPrefix><keyName>. Each value is a
// YAML list of constrained values, identical in shape to the OSS file-based
// dynamic config format:
//
//	# etcd key: temporal/dynamicconfig/frontend.rps
//	- value: 1200
//	  constraints: {}
//	- value: 500
//	  constraints:
//	    namespace: my-namespace
package etcddynconfig

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"maps"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"gopkg.in/yaml.v3"
)

//go:embed defaults.yaml
var defaultsYAML []byte

const (
	metricKeyUpdates      = "dynconfig_key_updates_total"
	metricWatchReconnects = "dynconfig_watch_reconnects_total"
	metricWatchActive     = "dynconfig_watch_active"
	metricKeysLoaded      = "dynconfig_keys_loaded"
	metricLoadDuration    = "dynconfig_load_duration_seconds"
	metricWriteTotal      = "dynconfig_write_total"
)

// Client extends dynamicconfig.Client with a Subscribe method for push-based
// updates and a WriteConfig method for programmatic writes.
//
// It implements both dynamicconfig.Client and dynamicconfig.NotifyingClient,
// so the Temporal server will use push notifications instead of polling.
type Client interface {
	dynamicconfig.Client
	dynamicconfig.NotifyingClient

	// WriteConfig writes a key+value into etcd. Intended for CLI tooling and
	// bootstrappers, not hot paths. After writing it reloads all values so the
	// in-memory cache is immediately consistent.
	WriteConfig(ctx context.Context, key dynamicconfig.Key, values []dynamicconfig.ConstrainedValue) error

	// DumpAll returns a snapshot of every key/value currently loaded in the
	// in-memory map. Keys are the string form of dynamicconfig.Key
	// (e.g. "frontend.rps"). Values are the same constrained-value slices
	// returned by GetValue. The returned map is a copy — safe to read after
	// the client has been stopped.
	DumpAll() map[string][]dynamicconfig.ConstrainedValue

	// LogAll writes the full loaded config to the logger at INFO level.
	// Useful as a startup diagnostic or triggered from a debug handler.
	LogAll()

	// Stop closes the etcd watcher and cancels the background goroutines.
	// Call this on server shutdown.
	Stop()
}

type (
	configValueMap map[dynamicconfig.Key][]dynamicconfig.ConstrainedValue

	yamlConstrainedValue struct {
		Constraints map[string]any `yaml:"constraints"`
		Value       any            `yaml:"value"`
	}

	client struct {
		lifecycleCtx   context.Context
		cancel         context.CancelFunc
		cli            *clientv3.Client
		prefix         string
		logger         log.Logger
		metricsHandler metrics.Handler
		values         atomicValue[configValueMap]
		watcher        clientv3.Watcher

		subscriptions struct {
			sync.Mutex
			idx             int
			updateCallbacks map[int]dynamicconfig.ClientUpdateFunc
		}
	}
)

const requestTimeout = 5 * time.Second

// NewClient creates a dynamic config client backed by etcd. It loads all keys
// under keyPrefix at startup, then watches for changes.
//
// cli is typically the result of NewEtcdClient. keyPrefix should match
// Config.GlobalKeyPrefix. Pass metrics.NoopMetricsHandler if you do not want
// metrics emitted.
func NewClient(ctx context.Context, cli *clientv3.Client, keyPrefix string, logger log.Logger, metricsHandler metrics.Handler) (Client, error) {
	if cli == nil {
		return nil, errors.New("etcd client is nil")
	}
	if metricsHandler == nil {
		metricsHandler = metrics.NoopMetricsHandler
	}

	c := &client{
		cli:            cli,
		prefix:         keyPrefix,
		logger:         logger,
		metricsHandler: metricsHandler,
	}
	c.subscriptions.updateCallbacks = make(map[int]dynamicconfig.ClientUpdateFunc)
	c.lifecycleCtx, c.cancel = context.WithCancel(clientv3.WithRequireLeader(ctx))

	rev, err := c.loadAll()
	if err != nil {
		return nil, err
	}

	c.seedDefaults()

	c.watcher = clientv3.NewWatcher(c.cli)
	go c.watchForCancel()
	go c.watch(rev, c.loadAll)
	return c, nil
}

// Stop closes the watcher and cancels lifecycle goroutines.
func (c *client) Stop() {
	defer c.cancel()
	if err := c.watcher.Close(); err != nil {
		c.logger.Error("failed to close etcd watcher", tag.Error(err))
	}
}

func (c *client) watchForCancel() {
	<-c.lifecycleCtx.Done()
	c.Stop()
}

// WriteConfig serializes values as YAML and writes them to etcd under the
// client's prefix. Immediately reloads all values afterwards to ensure the
// in-memory cache is consistent before the next read.
func (c *client) WriteConfig(ctx context.Context, key dynamicconfig.Key, values []dynamicconfig.ConstrainedValue) error {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	b, err := c.marshalValues(values)
	if err != nil {
		c.logger.Error("failed to marshal config for writing to etcd", tag.Error(err))
		return err
	}

	etcdKey := fmt.Sprintf("%s%s", c.prefix, key.String())
	c.logger.Info("writing dynamic config to etcd",
		tag.NewStringTag("key", key.String()),
		tag.NewStringTag("value", b))
	if _, err := c.cli.Put(ctx, etcdKey, b); err != nil {
		c.logger.Error("failed to write dynamic config to etcd", tag.Error(err))
		c.metricsHandler.Counter(metricWriteTotal).Record(1, metrics.StringTag("result", "error"))
		return err
	}

	// Reload eagerly — the watch may not fire before the next read.
	if _, err := c.loadAll(); err != nil {
		c.logger.Error("failed to reload config after write", tag.Error(err))
		c.metricsHandler.Counter(metricWriteTotal).Record(1, metrics.StringTag("result", "error"))
		return err
	}
	c.metricsHandler.Counter(metricWriteTotal).Record(1, metrics.StringTag("result", "success"))
	return nil
}

// GetValue returns the constrained values for key from the in-memory cache.
// Satisfies dynamicconfig.Client.
func (c *client) GetValue(key dynamicconfig.Key) []dynamicconfig.ConstrainedValue {
	values, ok := c.values.Load()
	if !ok {
		c.logger.Warn("config value map not yet loaded", tag.NewStringTag("key", key.String()))
		return nil
	}
	return values[key]
}

// DumpAll returns a snapshot of every key/value currently in the in-memory
// map. The returned map is a copy — mutations do not affect the client.
func (c *client) DumpAll() map[string][]dynamicconfig.ConstrainedValue {
	values, ok := c.values.Load()
	if !ok {
		return nil
	}
	out := make(map[string][]dynamicconfig.ConstrainedValue, len(values))
	for k, v := range values {
		out[k.String()] = v
	}
	return out
}

// LogAll logs every currently loaded key and its constrained values at INFO
// level. Each key is a separate log line so output is readable in structured
// logging systems. Fires once — not a watch.
func (c *client) LogAll() {
	values, ok := c.values.Load()
	if !ok {
		c.logger.Info("dynamic config map not yet loaded")
		return
	}
	if len(values) == 0 {
		c.logger.Info("dynamic config map is empty (no keys loaded from etcd)")
		return
	}
	c.logger.Info("dynamic config dump start", tag.NewInt("totalKeys", len(values)))
	for k, cvs := range values {
		var b strings.Builder
		appendCVs(&b, cvs)
		c.logger.Info("dynamic config entry",
			tag.NewStringTag("key", k.String()),
			tag.NewStringTag("values", b.String()),
		)
	}
	c.logger.Info("dynamic config dump end")
}

// Subscribe registers f to be called whenever any dynamic config value
// changes. Returns a cancel func that removes the subscription.
// Satisfies dynamicconfig.NotifyingClient.
func (c *client) Subscribe(f dynamicconfig.ClientUpdateFunc) (cancel func()) {
	c.subscriptions.Lock()
	defer c.subscriptions.Unlock()
	c.subscriptions.idx++
	id := c.subscriptions.idx
	c.subscriptions.updateCallbacks[id] = f
	return func() {
		c.subscriptions.Lock()
		defer c.subscriptions.Unlock()
		delete(c.subscriptions.updateCallbacks, id)
	}
}

// loadAll does a full Get with prefix and seeds the in-memory cache.
// Returns the etcd revision so the subsequent Watch can start from there.
func (c *client) loadAll() (int64, error) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(c.lifecycleCtx, requestTimeout)
	defer cancel()

	c.values.Store(make(configValueMap))

	resp, err := c.cli.Get(ctx, c.prefix, clientv3.WithPrefix())
	if err != nil {
		return 0, err
	}

	events := make([]*clientv3.Event, len(resp.Kvs))
	for i, kv := range resp.Kvs {
		events[i] = &clientv3.Event{Type: clientv3.EventTypePut, Kv: kv}
	}
	c.applyEvents(events)
	c.metricsHandler.Gauge(metricKeysLoaded).Record(float64(len(resp.Kvs)))
	c.metricsHandler.Timer(metricLoadDuration).Record(time.Since(start))
	return resp.Header.Revision, nil
}

// watchExitReason describes why a single Watch stream ended.
type watchExitReason int

const (
	watchExitStreamEnded watchExitReason = iota
	watchExitCompacted
	watchExitCancelled
)

const (
	watchReloadInitialBackoff = 100 * time.Millisecond
	watchReloadMaxBackoff     = 30 * time.Second
)

// watch supervises the etcd Watch stream. It survives transient disruptions
// (leader election, connection resets, compaction) by reloading state and
// reopening a fresh stream. Only Stop / context cancellation exits the loop.
//
// Compaction note: even a healthy watcher can hit ErrCompacted if the server
// compacts past the client's last-seen revision during a reconnect — observed
// as a fleet-wide event where all watchers in a cell died within ~3s. The
// supervisor converts that into a reload + fresh Watch from the new revision.
func (c *client) watch(rev int64, reload func() (int64, error)) {
	c.metricsHandler.Gauge(metricWatchActive).Record(1)
	defer c.metricsHandler.Gauge(metricWatchActive).Record(0)

	backoff := watchReloadInitialBackoff
	for {
		reason := c.watchOnce(rev)
		if reason == watchExitCancelled {
			return
		}
		reasonStr := "stream_ended"
		if reason == watchExitCompacted {
			reasonStr = "compacted"
		}
		c.metricsHandler.Counter(metricWatchReconnects).Record(1, metrics.StringTag("reason", reasonStr))

		select {
		case <-c.lifecycleCtx.Done():
			return
		case <-time.After(backoff):
		}
		newRev, err := reload()
		if err != nil {
			c.logger.Error("etcd reload after watch failure failed; will retry", tag.Error(err))
			backoff = min(backoff*2, watchReloadMaxBackoff)
			continue
		}
		rev = newRev
		backoff = watchReloadInitialBackoff
	}
}

// watchOnce runs one Watch stream and returns why it ended.
func (c *client) watchOnce(rev int64) watchExitReason {
	ch := c.watcher.Watch(c.lifecycleCtx, c.prefix,
		clientv3.WithPrefix(),
		clientv3.WithRev(rev))
	for resp := range ch {
		if err := resp.Err(); err != nil {
			if clientv3.IsConnCanceled(err) {
				c.logger.Warn("etcd connection cancelled, stopping watcher", tag.Error(err))
				return watchExitCancelled
			}
			if resp.CompactRevision > 0 {
				c.logger.Warn("etcd watch hit compaction; reloading",
					tag.Error(err),
					tag.NewInt64("compactRevision", resp.CompactRevision))
				return watchExitCompacted
			}
			c.logger.Warn("etcd watch stream error; reloading", tag.Error(err))
			return watchExitStreamEnded
		}
		c.applyEvents(resp.Events)
	}
	if c.lifecycleCtx.Err() != nil {
		return watchExitCancelled
	}
	return watchExitStreamEnded
}

func (c *client) applyEvents(events []*clientv3.Event) {
	oldMap, ok := c.values.Load()
	if !ok {
		oldMap = make(configValueMap)
	}

	newMap := maps.Clone(oldMap)
	changed := make(map[dynamicconfig.Key][]dynamicconfig.ConstrainedValue)

	for _, ev := range events {
		keyName := strings.TrimPrefix(string(ev.Kv.Key), c.prefix)
		key := dynamicconfig.MakeKey(keyName)

		switch ev.Type {
		case clientv3.EventTypeDelete:
			delete(newMap, key)
			changed[key] = nil
			c.logDiff(keyName, oldMap[key], nil)
			c.metricsHandler.Counter(metricKeyUpdates).Record(1,
					metrics.StringTag("operation", "DynamicConfigDelete"),
					metrics.StringTag("key", strings.TrimPrefix(string(ev.Kv.Key), c.prefix)),
				)

		case clientv3.EventTypePut:
			cvs, err := c.unmarshalValues(ev.Kv.Value)
			if err != nil {
				c.logger.Error("failed to parse dynamic config value",
					tag.Error(err),
					tag.Key(key.String()),
					tag.NewStringTag("raw", string(ev.Kv.Value)))
				continue
			}
			newMap[key] = cvs
			changed[key] = cvs
			c.logDiff(keyName, oldMap[key], cvs)
			c.metricsHandler.Counter(metricKeyUpdates).Record(1,
					metrics.StringTag("operation", "DynamicConfigUpdate"),
					metrics.StringTag("key", strings.TrimPrefix(string(ev.Kv.Key), c.prefix)),
				)
		}
	}

	if len(changed) == 0 {
		return
	}
	c.values.Store(newMap)

	var subs []dynamicconfig.ClientUpdateFunc
	c.subscriptions.Lock()
	for _, s := range c.subscriptions.updateCallbacks {
		subs = append(subs, s)
	}
	c.subscriptions.Unlock()
	for _, s := range subs {
		s(changed)
	}
}

// unmarshalValues parses a YAML-encoded []yamlConstrainedValue.
func (c *client) unmarshalValues(data []byte) ([]dynamicconfig.ConstrainedValue, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var yvs []yamlConstrainedValue
	if err := yaml.Unmarshal(data, &yvs); err != nil {
		return nil, err
	}
	cvs := make([]dynamicconfig.ConstrainedValue, len(yvs))
	for i, yv := range yvs {
		val, err := normalizeKeys(yv.Value)
		if err != nil {
			return nil, err
		}
		cvs[i].Value = val
		cvs[i].Constraints = c.parseConstraints(yv.Constraints)
	}
	return cvs, nil
}

// marshalValues is the inverse of unmarshalValues.
func (c *client) marshalValues(values []dynamicconfig.ConstrainedValue) (string, error) {
	yvs := make([]yamlConstrainedValue, 0, len(values))
	for _, v := range values {
		yv := yamlConstrainedValue{Value: v.Value, Constraints: make(map[string]any)}
		cs := v.Constraints
		if cs.Namespace != "" {
			yv.Constraints["namespace"] = cs.Namespace
		}
		if cs.NamespaceID != "" {
			yv.Constraints["namespaceId"] = cs.NamespaceID
		}
		if cs.TaskQueueName != "" {
			yv.Constraints["taskQueueName"] = cs.TaskQueueName
		}
		if cs.TaskQueueType != 0 {
			yv.Constraints["taskType"] = cs.TaskQueueType.String()
		}
		if cs.TaskType != 0 {
			yv.Constraints["historyTaskType"] = cs.TaskType.String()
		}
		if cs.ShardID != 0 {
			yv.Constraints["shardId"] = cs.ShardID
		}
		if cs.Destination != "" {
			yv.Constraints["destination"] = cs.Destination
		}
		yvs = append(yvs, yv)
	}
	b, err := yaml.Marshal(yvs)
	return string(b), err
}

// parseConstraints converts the raw YAML map into dynamicconfig.Constraints.
func (c *client) parseConstraints(m map[string]any) dynamicconfig.Constraints {
	var cs dynamicconfig.Constraints
	for k, v := range m {
		switch strings.ToLower(k) {
		case "namespace":
			if s, ok := v.(string); ok {
				cs.Namespace = s
			} else {
				c.logger.Error("namespace constraint must be a string")
			}
		case "namespaceid":
			if s, ok := v.(string); ok {
				cs.NamespaceID = s
			} else {
				c.logger.Error("namespaceId constraint must be a string")
			}
		case "taskqueuename":
			if s, ok := v.(string); ok {
				cs.TaskQueueName = s
			} else {
				c.logger.Error("taskQueueName constraint must be a string")
			}
		case "tasktype":
			switch v := v.(type) {
			case string:
				t, err := enumspb.TaskQueueTypeFromString(v)
				if err != nil {
					c.logger.Error("invalid taskType value", tag.Error(err))
				} else if t > enumspb.TASK_QUEUE_TYPE_UNSPECIFIED {
					cs.TaskQueueType = t
				}
			case int:
				if v > int(enumspb.TASK_QUEUE_TYPE_UNSPECIFIED) {
					cs.TaskQueueType = enumspb.TaskQueueType(v)
				}
			default:
				c.logger.Error("taskType must be a string (Workflow/Activity) or int")
			}
		case "historytasktype":
			switch v := v.(type) {
			case string:
				t, err := enumsspb.TaskTypeFromString(v)
				if err != nil {
					c.logger.Error("invalid historyTaskType value", tag.Error(err))
				} else if t > enumsspb.TASK_TYPE_UNSPECIFIED {
					cs.TaskType = t
				}
			case int:
				cs.TaskType = enumsspb.TaskType(v)
			default:
				c.logger.Error("historyTaskType must be a string or int")
			}
		case "shardid":
			if v, ok := v.(int); ok {
				cs.ShardID = int32(v)
			} else {
				c.logger.Error("shardId constraint must be an integer")
			}
		case "destination":
			if s, ok := v.(string); ok {
				cs.Destination = s
			} else {
				c.logger.Error("destination constraint must be a string")
			}
		default:
			c.logger.Error("unknown constraint type", tag.Key(k))
		}
	}
	return cs
}

// normalizeKeys recursively converts map[any]any (produced by gopkg.in/yaml.v3
// for nested maps) to map[string]any so downstream code can use type assertions.
func normalizeKeys(v any) (any, error) {
	switch v := v.(type) {
	case map[any]any:
		out := make(map[string]any, len(v))
		for key, val := range v {
			sk, ok := key.(string)
			if !ok {
				return nil, errors.New("map key is not a string")
			}
			nv, err := normalizeKeys(val)
			if err != nil {
				return nil, err
			}
			out[sk] = nv
		}
		return out, nil
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			nv, err := normalizeKeys(item)
			if err != nil {
				return nil, err
			}
			out[i] = nv
		}
		return out, nil
	default:
		return v, nil
	}
}

func (c *client) logDiff(key string, oldVs, newVs []dynamicconfig.ConstrainedValue) {
	var b strings.Builder
	b.WriteString("dynamic config changed: ")
	b.WriteString(key)
	b.WriteString(" old=")
	appendCVs(&b, oldVs)
	b.WriteString(" new=")
	appendCVs(&b, newVs)
	c.logger.Info(b.String())
}

func appendCVs(b *strings.Builder, cvs []dynamicconfig.ConstrainedValue) {
	if cvs == nil {
		b.WriteString("nil")
		return
	}
	b.WriteByte('[')
	for i, cv := range cvs {
		if i > 0 {
			b.WriteByte(' ')
		}
		fmt.Fprintf(b, "{constraints:%+v value:%v}", cv.Constraints, cv.Value)
	}
	b.WriteByte(']')
}

// parseDefaultsFile parses the embedded defaults.yaml into a map of
// key name → marshaled YAML value string ready to write to etcd.
func parseDefaultsFile(data []byte) (map[string]string, error) {
	var raw map[string]any
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parse defaults YAML: %w", err)
	}
	out := make(map[string]string, len(raw))
	for k, v := range raw {
		b, err := yaml.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("marshal default for key %s: %w", k, err)
		}
		out[k] = strings.TrimSpace(string(b))
	}
	return out, nil
}

// seedDefaults writes any key from the embedded defaults.yaml that does not
// already exist in etcd. Existing keys are never touched — customer values
// always win. Called once during NewClient after the initial loadAll.
func (c *client) seedDefaults() {
	defaults, err := parseDefaultsFile(defaultsYAML)
	if err != nil {
		c.logger.Error("etcd dynconfig: failed to parse embedded defaults", tag.Error(err))
		return
	}

	current, ok := c.values.Load()
	if !ok {
		current = make(configValueMap)
	}

	existing := make(map[string]struct{}, len(current))
	for k := range current {
		existing[k.String()] = struct{}{}
	}

	seeded := 0
	for keyName, valStr := range defaults {
		if _, exists := existing[keyName]; exists {
			continue
		}
		etcdKey := c.prefix + keyName
		ctx, cancel := context.WithTimeout(c.lifecycleCtx, requestTimeout)
		_, putErr := c.cli.Put(ctx, etcdKey, valStr)
		cancel()
		if putErr != nil {
			c.logger.Error("etcd dynconfig: failed to seed default",
				tag.NewStringTag("key", keyName), tag.Error(putErr))
		} else {
			c.logger.Info("etcd dynconfig: seeded default", tag.NewStringTag("key", keyName))
			seeded++
		}
	}

	if seeded > 0 {
		c.logger.Info("etcd dynconfig: finished seeding defaults", tag.NewInt("count", seeded))
		if _, err := c.loadAll(); err != nil {
			c.logger.Error("etcd dynconfig: failed to reload after seeding defaults", tag.Error(err))
		}
	}
}
