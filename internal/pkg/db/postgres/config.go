package postgres

import (
	"fmt"
	"net/url"

	"github.com/Mar1eena/TrB_V3/internal/pkg/env"
)

type Config struct {
	DSN string
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
