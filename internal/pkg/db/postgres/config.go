package postgres

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/Mar1eena/TrB_V3/internal/pkg/env"
)

type Config struct {
	DSN      string
	MaxConns int32
}

func ConfigFromEnv() Config {
	host := env.Addr("POSTGRES_URL", "POSTGRES_URL_DOCKER")
	if host == "" {
		host = "localhost:5432"
	}
	user := env.First("POSTGRES_USER")
	if user == "" {
		user = "postgres"
	}
	password := env.Get("POSTGRES_PASSWORD")
	db := env.Get("POSTGRES_DB")
	if db == "" {
		db = "trb"
	}

	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(user, password),
		Host:   host,
		Path:   "/" + db,
	}
	q := u.Query()
	q.Set("sslmode", "disable")
	u.RawQuery = q.Encode()

	return Config{DSN: u.String()}
}

func (c Config) String() string {
	return fmt.Sprintf("postgres DSN configured (host from POSTGRES_URL*)")
}

func (c Config) Database() string {
	u, err := url.Parse(c.DSN)
	if err != nil {
		return ""
	}
	return strings.Trim(u.Path, "/")
}

// OptunaStorageURL — RDB storage URL для optuna.create_study(storage=...) на
// том же Postgres, что и остальной сервис (SQLAlchemy-диалект psycopg3,
// т.к. движок уже зависит от psycopg[binary]). Optuna заводит свои таблицы
// (trials/studies/...), не пересекающиеся со strategysearch_*. Нужен, чтобы
// после завершения поиска можно было optuna.load_study(...) и посчитать
// param importances (fANOVA) — по умолчанию storage у Optuna in-memory и
// Study теряется вместе с процессом движка.
func (c Config) OptunaStorageURL() string {
	u, err := url.Parse(c.DSN)
	if err != nil {
		return ""
	}
	u.Scheme = "postgresql+psycopg"
	return u.String()
}

func (c Config) WithDatabase(name string) Config {
	u, err := url.Parse(c.DSN)
	if err != nil {
		return c
	}
	u.Path = "/" + name
	c.DSN = u.String()
	return c
}

func (c Config) WithMaxConns(n int32) Config {
	c.MaxConns = n
	return c
}
