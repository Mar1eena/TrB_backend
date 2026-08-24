package postgres

import (
	"encoding/json"
	"net/url"
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
	Host       string `json:"host"`
	HostDocker string `json:"host_docker"`
	User       string `json:"user"`
	Password   string `json:"password"`
	Database   string `json:"database"`
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
	local := parseExtraJSON(env.First("POSTGRES_EXTRA_CONNECTIONS", "POSTGRES_EXTRA_CONNECTIONS"))
	docker := parseExtraJSON(env.First("POSTGRES_EXTRA_CONNECTIONS_DOCKER", "POSTGRES_EXTRA_CONNECTIONS_DOCKER"))
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
		if strings.TrimSpace(cur.Host) == "" {
			cur.Host = item.Host
		}
		if strings.TrimSpace(cur.HostDocker) == "" {
			if strings.TrimSpace(item.HostDocker) != "" {
				cur.HostDocker = item.HostDocker
			} else if strings.TrimSpace(item.Host) != "" {
				cur.HostDocker = item.Host
			}
		}
		if strings.TrimSpace(cur.User) == "" {
			cur.User = item.User
		}
		if cur.Password == "" {
			cur.Password = item.Password
		}
		if strings.TrimSpace(cur.Database) == "" {
			cur.Database = item.Database
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

func pickHost(local, docker string) string {
	if env.IsContainer() && strings.TrimSpace(docker) != "" {
		return strings.TrimSpace(docker)
	}
	if strings.TrimSpace(local) != "" {
		return strings.TrimSpace(local)
	}
	return strings.TrimSpace(docker)
}

func displayHost(local, docker string) string {
	if strings.TrimSpace(local) != "" {
		return strings.TrimSpace(local)
	}
	return strings.TrimSpace(docker)
}

func DefaultConnectionName() string {
	if name := env.Get("POSTGRES_CONNECTION_NAME"); name != "" {
		return name
	}
	return "default"
}

func (c Config) Host() string {
	u, err := url.Parse(c.DSN)
	if err != nil {
		return ""
	}
	return u.Host
}

func (c Config) withHost(host string) Config {
	u, err := url.Parse(c.DSN)
	if err != nil {
		return c
	}
	u.Host = strings.TrimSpace(host)
	c.DSN = u.String()
	return c
}

func (c Config) WithHost(host string) Config {
	return c.withHost(host)
}

func (c Config) withUser(user, password string) Config {
	u, err := url.Parse(c.DSN)
	if err != nil {
		return c
	}
	user = strings.TrimSpace(user)
	if user == "" {
		return c
	}
	if password != "" {
		u.User = url.UserPassword(user, password)
	} else {
		u.User = url.User(user)
	}
	c.DSN = u.String()
	return c
}

func NamedConfigs() []NamedConfig {
	base := ConfigFromEnv()
	out := []NamedConfig{{
		Name:     DefaultConnectionName(),
		Config:   base,
		Host:     base.Host(),
		Database: base.Database(),
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
		if host := pickHost(item.Host, item.HostDocker); host != "" {
			cfg = cfg.withHost(host)
		}
		if strings.TrimSpace(item.User) != "" {
			cfg = cfg.withUser(item.User, item.Password)
		} else if item.Password != "" {
			cfg = cfg.withUser(env.First("POSTGRES_USER"), item.Password)
		}
		if strings.TrimSpace(item.Database) != "" {
			cfg = cfg.WithDatabase(strings.TrimSpace(item.Database))
		}
		host := displayHost(item.Host, item.HostDocker)
		if host == "" {
			host = cfg.Host()
		}
		out = append(out, NamedConfig{
			Name:     name,
			Config:   cfg,
			Host:     host,
			Database: cfg.Database(),
		})
	}
	return out
}
