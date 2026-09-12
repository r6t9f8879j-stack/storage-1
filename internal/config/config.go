package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

// PeerConfig configures replication toward the sibling node.
type PeerConfig struct {
	Enabled              bool   `toml:"enabled"`
	URL                  string `toml:"url"`
	Key                  string `toml:"key"` // X-Peer-Secret on outbound calls (injected via env in practice)
	PollIntervalSeconds  int    `toml:"poll_interval_seconds"`
	ApplyBatchSize       int    `toml:"apply_batch_size"`
}

// Config is the full storaged runtime configuration.
type Config struct {
	Listen           string     `toml:"listen"`
	PublicURL        string     `toml:"public_url"`
	DataDir          string     `toml:"data_dir"`
	NodeID           string     `toml:"node_id"`
	Role             string     `toml:"role"` // leader | follower

	AdminKey         string     `toml:"admin_key"`
	ReadKey          string     `toml:"read_key"`
	PeerSecret       string     `toml:"peer_secret"`

	SupabaseURL      string     `toml:"supabase_url"`
	SupabaseAnonKey  string     `toml:"supabase_anon_key"`

	MaxBodyBytes     int64      `toml:"max_body_bytes"`
	HighWaterBytes   int64      `toml:"high_water_bytes"`
	UploadTTLStr     string     `toml:"upload_ttl"`
	GCGraceStr       string     `toml:"gc_grace"`
	SignedURLTTLStr  string     `toml:"signed_url_ttl"`

	RatePerMinute    int        `toml:"rate_per_minute"`

	Peer             PeerConfig `toml:"peer"`

	uploadTTL        time.Duration
	gcGrace          time.Duration
	signedURLTTL     time.Duration
}

const (
	RoleLeader   = "leader"
	RoleFollower = "follower"
)

// Load reads a TOML file (optional) then layers environment overrides on top.
// Env vars win. No hidden defaults beyond sane fallbacks.
func Load(path string) (*Config, error) {
	cfg := defaults()
	if path != "" {
		if _, err := toml.DecodeFile(path, cfg); err != nil {
			return nil, fmt.Errorf("decode config %s: %w", path, err)
		}
	}
	cfg.applyEnv()
	if err := cfg.parseDurations(); err != nil {
		return nil, err
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func defaults() *Config {
	return &Config{
		Listen:         "127.0.0.1:5000",
		DataDir:        "storage",
		NodeID:         "node-a",
		Role:           RoleLeader,
		MaxBodyBytes:   2 << 30, // 2 GiB non-multipart cap
		HighWaterBytes: 10 << 30, // refuse writes when free space < 10 GiB
		UploadTTLStr:    "24h",
		GCGraceStr:      "24h",
		SignedURLTTLStr: "5m",
		RatePerMinute:  600,
	}
}

func (c *Config) applyEnv() {
	str := func(key string, dst *string) {
		if v, ok := os.LookupEnv(key); ok && v != "" {
			*dst = v
		}
	}
	str("STORAGED_LISTEN", &c.Listen)
	str("STORAGED_PUBLIC_URL", &c.PublicURL)
	str("STORAGED_DATA_DIR", &c.DataDir)
	str("STORAGED_NODE_ID", &c.NodeID)
	str("STORAGED_ROLE", &c.Role)
	str("STORAGED_ADMIN_KEY", &c.AdminKey)
	str("STORAGED_READ_KEY", &c.ReadKey)
	str("STORAGED_PEER_SECRET", &c.PeerSecret)
	str("STORAGED_SUPABASE_URL", &c.SupabaseURL)
	str("STORAGED_SUPABASE_ANON_KEY", &c.SupabaseAnonKey)
	str("STORAGED_PEER_URL", &c.Peer.URL)
	str("STORAGED_PEER_KEY", &c.Peer.Key)
	intEnv := func(key string, dst *int) {
		if v, ok := os.LookupEnv(key); ok && v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				*dst = n
			}
		}
	}
	intEnv("STORAGED_RATE_PER_MINUTE", &c.RatePerMinute)
	if v, ok := os.LookupEnv("STORAGED_MAX_BODY_BYTES"); ok && v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			c.MaxBodyBytes = n
		}
	}
	if v, ok := os.LookupEnv("STORAGED_HIGH_WATER_BYTES"); ok && v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			c.HighWaterBytes = n
		}
	}
	if v, ok := os.LookupEnv("STORAGED_UPLOAD_TTL"); ok && v != "" {
		c.UploadTTLStr = v
	}
	if v, ok := os.LookupEnv("STORAGED_GC_GRACE"); ok && v != "" {
		c.GCGraceStr = v
	}
	if v, ok := os.LookupEnv("STORAGED_SIGNED_URL_TTL"); ok && v != "" {
		c.SignedURLTTLStr = v
	}
	if v, ok := os.LookupEnv("STORAGED_RATE_PER_MINUTE"); ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.RatePerMinute = n
		}
	}
	if v, ok := os.LookupEnv("STORAGED_PEER_ENABLED"); ok && strings.EqualFold(v, "true") {
		c.Peer.Enabled = true
	}
	if v, ok := os.LookupEnv("STORAGED_PEER_POLL_SECONDS"); ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.Peer.PollIntervalSeconds = n
		}
	}
	if c.Peer.PollIntervalSeconds == 0 {
		c.Peer.PollIntervalSeconds = 10
	}
	if c.Peer.ApplyBatchSize == 0 {
		c.Peer.ApplyBatchSize = 200
	}
}

func (c *Config) parseDurations() error {
	var err error
	if c.uploadTTL, err = time.ParseDuration(c.UploadTTLStr); err != nil {
		return fmt.Errorf("upload_ttl: %w", err)
	}
	if c.gcGrace, err = time.ParseDuration(c.GCGraceStr); err != nil {
		return fmt.Errorf("gc_grace: %w", err)
	}
	if c.signedURLTTL, err = time.ParseDuration(c.SignedURLTTLStr); err != nil {
		return fmt.Errorf("signed_url_ttl: %w", err)
	}
	return nil
}

func (c *Config) validate() error {
	if c.Listen == "" {
		return errors.New("listen address required")
	}
	if c.DataDir == "" {
		return errors.New("data_dir required")
	}
	if c.NodeID == "" {
		return errors.New("node_id required")
	}
	switch c.Role {
	case RoleLeader, RoleFollower:
	default:
		return fmt.Errorf("role must be %q or %q", RoleLeader, RoleFollower)
	}
	secret := func(name, v string) error {
		if v == "" {
			return fmt.Errorf("%s required (provide via STORAGED_%s env or config file)", name, strings.ToUpper(strings.ReplaceAll(name, "_", "_")))
		}
		return nil
	}
	if err := secret("admin_key", c.AdminKey); err != nil {
		return err
	}
	if err := secret("read_key", c.ReadKey); err != nil {
		return err
	}
	if c.Peer.Enabled && c.Role == RoleFollower {
		if c.Peer.URL == "" {
			return errors.New("peer.url required when role=follower")
		}
		if c.Peer.Key == "" && c.PeerSecret != "" {
			c.Peer.Key = c.PeerSecret
		}
		if c.Peer.Key == "" {
			return errors.New("peer key/secret required for replication")
		}
	}
	return nil
}

// Durations are exported after parsing/validation.
func (c *Config) UploadTTL() time.Duration   { return c.uploadTTL }
func (c *Config) GCGrace() time.Duration     { return c.gcGrace }
func (c *Config) SignedURLTTL() time.Duration { return c.signedURLTTL }
func (c *Config) IsLeader() bool             { return c.Role == RoleLeader }