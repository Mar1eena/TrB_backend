package investgo_test

import (
	"crypto/tls"
	"net"
	"testing"
	"time"

	"github.com/Mar1eena/TrB_V3/internal/pkg/investgo"
)

func TestRussianTrustedCATLS(t *testing.T) {
	cfg := investgo.Config{}
	tlsCfg, err := cfg.BuildTLSConfig()
	if err != nil {
		t.Fatal(err)
	}
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	conn, err := tls.DialWithDialer(dialer, "tcp", "invest-public-api.tinkoff.ru:443", tlsCfg)
	if err != nil {
		t.Fatalf("tls handshake failed: %v", err)
	}
	defer conn.Close()
	state := conn.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		t.Fatal("no peer certificates")
	}
	t.Logf("ok peer=%s", state.PeerCertificates[0].Subject)
}
