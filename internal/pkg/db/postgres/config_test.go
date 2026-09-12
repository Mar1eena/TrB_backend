package postgres

import "testing"

func TestOptunaStorageURL(t *testing.T) {
	t.Setenv("POSTGRES_URL", "localhost:5432")
	t.Setenv("POSTGRES_USER", "postgres")
	t.Setenv("POSTGRES_PASSWORD", "1234")
	t.Setenv("POSTGRES_DB", "trb")

	got := ConfigFromEnv().OptunaStorageURL()
	want := "postgresql+psycopg://postgres:1234@localhost:5432/trb?sslmode=disable"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}
