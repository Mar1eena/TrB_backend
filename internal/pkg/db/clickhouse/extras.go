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
	Name       string `json:"name"`
	Addr       string `json:"addr"`
	AddrDocker string `json:"addr_docker"`
	HTTP       string `json:"http"`
	Database   string `json:"database"`
	Username   string `json:"username"`
	Password   string `json:"password"`
}

func parseExtraJSON(raw string) []extraJSON {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var extras []extraJSON
	if err := json.Unmarshal([]byte(raw), &extras); err != nil {
		return nil
	}
	return extras
}

func extraItems() []extraJSON {
	local := parseExtraJSON(env.First("CLICKHOUSE_EXTRA_CONNECTIONS", "CLICKHOUSE_EXTRA_CONNECTIONS"))
	docker := parseExtraJSON(env.First("CLICKHOUSE_EXTRA_CONNECTIONS_DOCKER", "CLICKHOUSE_EXTRA_CONNECTIONS_DOCKER"))
	byName := make(map[string]extraJSON)
	order := make([]string, 0)
	add := func(item extraJSON) {
		name := strings.TrimSpace(item.Name)
		if name == "" {
			return
		}
		cur, ok := byName[name]
		if !ok {
			order = append(order, name)
			byName[name] = item
			return
		}
		if strings.TrimSpace(cur.Addr) == "" {
			cur.Addr = item.Addr
		}
		if strings.TrimSpace(cur.AddrDocker) == "" {
			if strings.TrimSpace(item.AddrDocker) != "" {
				cur.AddrDocker = item.AddrDocker
			} else if strings.TrimSpace(item.Addr) != "" {
				cur.AddrDocker = item.Addr
			}
		}
		if strings.TrimSpace(cur.HTTP) == "" {
			cur.HTTP = item.HTTP
		}
		if strings.TrimSpace(cur.Database) == "" {
			cur.Database = item.Database
		}
		if strings.TrimSpace(cur.Username) == "" {
			cur.Username = item.Username
		}
		if cur.Password == "" {
			cur.Password = item.Password
		}
		byName[name] = cur
	}
	for _, item := range local {
		add(item)
	}
	for _, item := range docker {
		add(item)
	}
	out := make([]extraJSON, 0, len(order))
	for _, name := range order {
		out = append(out, byName[name])
	}
	return out
}

func pickAddr(local, docker string) string {
	if env.IsContainer() && strings.TrimSpace(docker) != "" {
		return strings.TrimSpace(docker)
	}
	if strings.TrimSpace(local) != "" {
		return strings.TrimSpace(local)
	}
	return strings.TrimSpace(docker)
}

func displayAddr(local, docker string) string {
	if strings.TrimSpace(local) != "" {
		return strings.TrimSpace(local)
	}
	return strings.TrimSpace(docker)
}

func DefaultConnectionName() string {
	if name := env.Get("CLICKHOUSE_CONNECTION_NAME"); name != "" {
		return name
	}
	return "default"
}

func NamedConfigs() []NamedConfig {
	base := ClickHouse_config()
	display := strings.TrimSpace(env.Get("CLICKHOUSE_URL"))
	if display == "" {
		display = base.Addr
	}
	out := []NamedConfig{{
		Name:     DefaultConnectionName(),
		Config:   base,
		Host:     display,
		Database: base.Database,
		Default:  true,
	}}
	seen := map[string]struct{}{out[0].Name: {}}
	for _, item := range extraItems() {
		name := strings.TrimSpace(item.Name)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		cfg := base
		if addr := pickAddr(item.Addr, item.AddrDocker); addr != "" {
			cfg.Addr = addr
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
		host := displayAddr(item.Addr, item.AddrDocker)
		if host == "" {
			host = cfg.Addr
		}
		out = append(out, NamedConfig{
			Name:     name,
			Config:   cfg,
			Host:     host,
			Database: cfg.Database,
		})
	}
	return out
}
