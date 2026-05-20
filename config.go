package etcddynconfig

import (
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the top-level configuration for the etcd-backed dynamic config client.
//
// Minimal no-TLS example:
//
//	etcdConfigs:
//	  - name: primary
//	    endpoints: ["127.0.0.1:2379"]
//	globalKeyPrefix: "temporal/dynamicconfig/"
//	disableTLS: true
//	clientName: temporal-server
//
// With mTLS:
//
//	etcdConfigs:
//	  - name: primary
//	    endpoints: ["etcd.example.com:2379"]
//	globalKeyPrefix: "temporal/dynamicconfig/"
//	disableTLS: false
//	clientTlsCaCertFile: /etc/certs/ca.crt
//	clientTlsCertFile:   /etc/certs/client.crt
//	clientTlsKeyFile:    /etc/certs/client.key
//	clientName: temporal-server
type Config struct {
	// EtcdConfigs lists the etcd clusters. Currently only the first entry is used.
	EtcdConfigs []EtcdConfig `yaml:"etcdConfigs"`

	// GlobalKeyPrefix is prepended to every dynamic config key stored in etcd.
	// Use a unique prefix per cell/environment for isolation.
	GlobalKeyPrefix string `yaml:"globalKeyPrefix"`

	// DisableTLS disables mTLS when connecting to etcd. Useful for local dev.
	DisableTLS bool `yaml:"disableTLS"`

	// ClientTLSCAFile, ClientTLSCertFile, ClientTLSKeyFile are required when DisableTLS is false.
	ClientTLSCAFile   string `yaml:"clientTlsCaCertFile"`
	ClientTLSCertFile string `yaml:"clientTlsCertFile"`
	ClientTLSKeyFile  string `yaml:"clientTlsKeyFile"`

	// ClientName is used for TLS SNI and logging.
	ClientName string `yaml:"clientName"`

	// DialTimeout for the initial etcd connection. Defaults to 2s.
	DialTimeout time.Duration `yaml:"dialTimeout"`

	// MaxCallSendMsgSize is the maximum gRPC message size sent to etcd.
	// Must match etcd server's --max-request-bytes. Defaults to 4 MiB.
	MaxCallSendMsgSize int `yaml:"maxCallSendMsgSize"`
}

// EtcdConfig holds the connection details for a single etcd cluster.
type EtcdConfig struct {
	Name      string   `yaml:"name"`
	Endpoints []string `yaml:"endpoints"`
}

// Validate returns an error if the config is incomplete.
func (c *Config) Validate() error {
	if len(c.EtcdConfigs) == 0 {
		return fmt.Errorf("etcdConfigs: at least one entry required")
	}
	for i, ec := range c.EtcdConfigs {
		if ec.Name == "" {
			return fmt.Errorf("etcdConfigs[%d].name: required", i)
		}
		if len(ec.Endpoints) == 0 {
			return fmt.Errorf("etcdConfigs[%d].endpoints: at least one endpoint required", i)
		}
	}
	if c.GlobalKeyPrefix == "" {
		return fmt.Errorf("globalKeyPrefix: required")
	}
	if c.ClientName == "" {
		return fmt.Errorf("clientName: required")
	}
	if !c.DisableTLS {
		if c.ClientTLSCAFile == "" || c.ClientTLSCertFile == "" || c.ClientTLSKeyFile == "" {
			return fmt.Errorf("clientTlsCaCertFile, clientTlsCertFile, clientTlsKeyFile are required when disableTLS is false")
		}
	}
	return nil
}

// EnsureDefaults fills in default values for optional fields.
func (c *Config) EnsureDefaults() {
	if c.DialTimeout == 0 {
		c.DialTimeout = 2 * time.Second
	}
	if c.MaxCallSendMsgSize == 0 {
		c.MaxCallSendMsgSize = 4 * 1024 * 1024
	}
}

// BuildConfig parses a map[string]any (e.g. from a custom datastore options block) into Config.
func BuildConfig(options map[string]any) (Config, error) {
	b, err := yaml.Marshal(options)
	if err != nil {
		return Config{}, fmt.Errorf("marshal etcd config: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return Config{}, fmt.Errorf("unmarshal etcd config: %w", err)
	}
	cfg.EnsureDefaults()
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}
