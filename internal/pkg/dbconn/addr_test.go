package dbconn

import "testing"

func TestParseHostPort(t *testing.T) {
	cases := []struct {
		in         string
		host, port string
		ok         bool
	}{
		{"localhost:9001", "localhost", "9001", true},
		{"http://localhost:8124/", "localhost", "8124", true},
		{"http://127.0.0.1:8123/ping", "127.0.0.1", "8123", true},
		{"127.0.0.1:5435", "127.0.0.1", "5435", true},
		{"logs", "", "", false},
		{"clickhouse-pg:9000", "clickhouse-pg", "9000", true},
	}
	for _, tc := range cases {
		h, p, ok := ParseHostPort(tc.in)
		if ok != tc.ok || h != tc.host || p != tc.port {
			t.Fatalf("%q: got %q %q %v, want %q %q %v", tc.in, h, p, ok, tc.host, tc.port, tc.ok)
		}
	}
}

func TestRewritePorts(t *testing.T) {
	h, p := hostNative("localhost", "8124")
	if h != "localhost" || p != "9001" {
		t.Fatalf("host 8124: %s:%s", h, p)
	}
	h, p = dockerNative("localhost", "8124")
	if h != "clickhouse-pg" || p != "9000" {
		t.Fatalf("docker 8124: %s:%s", h, p)
	}
	h, p = dockerNative("127.0.0.1", "9001")
	if h != "clickhouse-pg" || p != "9000" {
		t.Fatalf("docker 9001: %s:%s", h, p)
	}
	h, p = dockerNative("127.0.0.1", "5435")
	if h != "postgre-1c-db" || p != "5432" {
		t.Fatalf("docker 5435: %s:%s", h, p)
	}
	if !LooksLikeAddr("http://localhost:8124/") {
		t.Fatal("expected HTTP URL to look like an address")
	}
}
