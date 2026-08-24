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
	Name     string `json:"name"`
	Host     string `json:"host"`
	User     string `json:"user"`
	Password string `json:"password"`
	Database string `json:"database"`
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
	raw := strings.TrimSpace(env.Get("POSTGRES_EXTRA_CONNECTIONS"))
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
		if strings.TrimSpace(item.Host) != "" {
			cfg = cfg.withHost(item.Host)
		}
		if strings.TrimSpace(item.User) != "" {
			cfg = cfg.withUser(item.User, item.Password)
		} else if item.Password != "" {
			cfg = cfg.withUser(env.First("POSTGRES_USER"), item.Password)
		}
		if strings.TrimSpace(item.Database) != "" {
			cfg = cfg.WithDatabase(strings.TrimSpace(item.Database))
		}
		out = append(out, NamedConfig{
			Name:     name,
			Config:   cfg,
			Host:     cfg.Host(),
			Database: cfg.Database(),
		})
	}
	return out
}
