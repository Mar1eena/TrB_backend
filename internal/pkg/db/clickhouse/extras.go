package clickhouse

import (
	"encoding/json"
	"strings"

	"github.com/Mar1eena/TrB_V3/internal/pkg/env"
)

type NamedConfig struct {
	Name     string
	Config   Config
	Host     string
	Database string
	Default  bool
}

type extraJSON struct {
	Name     string `json:"name"`
	Addr     string `json:"addr"`
	Database string `json:"database"`
	Username string `json:"username"`
	Password string `json:"password"`
}

func DefaultConnectionName() string {
	if name := env.Get("CLICKHOUSE_CONNECTION_NAME"); name != "" {
		return name
	}
	return "default"
}

func NamedConfigs() []NamedConfig {
	base := ClickHouse_config()
	out := []NamedConfig{{
		Name:     DefaultConnectionName(),
		Config:   base,
		Host:     base.Addr,
		Database: base.Database,
		Default:  true,
	}}
	raw := strings.TrimSpace(env.Get("CLICKHOUSE_EXTRA_CONNECTIONS"))
	if raw == "" {
		return out
	}
	var extras []extraJSON
	if err := json.Unmarshal([]byte(raw), &extras); err != nil {
		return out
	}
	seen := map[string]struct{}{out[0].Name: {}}
	for _, item := range extras {
		name := strings.TrimSpace(item.Name)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		cfg := base
		if strings.TrimSpace(item.Addr) != "" {
			cfg.Addr = strings.TrimSpace(item.Addr)
		}
		if strings.TrimSpace(item.Database) != "" {
			cfg.Database = strings.TrimSpace(item.Database)
		}
		if strings.TrimSpace(item.Username) != "" {
			cfg.Username = strings.TrimSpace(item.Username)
		}
		if item.Password != "" {
			cfg.Password = item.Password
		}
		out = append(out, NamedConfig{
			Name:     name,
			Config:   cfg,
			Host:     cfg.Addr,
			Database: cfg.Database,
		})
	}
	return out
}
