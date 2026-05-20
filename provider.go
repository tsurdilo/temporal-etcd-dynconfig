package etcddynconfig

import (
	"context"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/health" // required to enable gRPC lb health checking
)

const (
	connectivityCheckTimeout = 10 * time.Second

	// Explicit round_robin + health checking overrides etcd's default pick_first policy.
	grpcServiceConfig = `{
	"loadBalancingPolicy": "round_robin",
	"healthCheckConfig": {
		"serviceName": ""
	}
}`
)

// NewEtcdClient creates a *clientv3.Client from Config, performs a startup
// connectivity check, and fatals if etcd is unreachable after retries.
//
// The returned client is intended to be passed to NewClient as the raw etcd
// handle. Call client.Close() on shutdown.
func NewEtcdClient(cfg Config, logger log.Logger) *clientv3.Client {
	endpoint := cfg.EtcdConfigs[0]

	v3cfg := clientv3.Config{
		Endpoints:            endpoint.Endpoints,
		MaxCallSendMsgSize:   cfg.MaxCallSendMsgSize,
		DialTimeout:          cfg.DialTimeout,
		DialKeepAliveTime:    30 * time.Second,
		DialKeepAliveTimeout: 10 * time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithDisableServiceConfig(),
			grpc.WithDefaultServiceConfig(grpcServiceConfig),
		},
	}

	if !cfg.DisableTLS {
		tlsCfg, err := newClientMTLSConfig(cfg.ClientTLSCertFile, cfg.ClientTLSKeyFile, cfg.ClientTLSCAFile)
		if err != nil {
			logger.Fatal("error creating etcd TLS config", tag.Error(err))
		}
		v3cfg.TLS = tlsCfg
	}

	client, err := clientv3.New(v3cfg)
	if err != nil {
		logger.Fatal("unable to initialize etcd client", tag.Error(err))
	}

	// Fail fast at startup: a TLS misconfiguration or unreachable cluster
	// should not silently allow the server to start.
	err = backoff.ThrottleRetry(
		func() error {
			ctx, cancel := context.WithTimeout(context.Background(), connectivityCheckTimeout)
			defer cancel()
			_, err := client.Get(ctx, "initHealthCheck", clientv3.WithSerializable())
			if err != nil {
				logger.Warn("etcd connectivity check failure", tag.Error(err))
			}
			return err
		},
		backoff.NewExponentialRetryPolicy(2*time.Second).
			WithBackoffCoefficient(2.0).
			WithMaximumAttempts(3),
		func(error) bool { return true },
	)
	if err != nil {
		logger.Fatal("etcd healthcheck did not pass", tag.Error(err))
	}

	return client
}
