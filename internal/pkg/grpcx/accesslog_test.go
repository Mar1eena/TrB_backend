package grpcx

import (
	"testing"
)

func TestSplitFullMethod(t *testing.T) {
	svc, method := splitFullMethod("/trb.clickhouse.manager.public.contract.v1.ClickHouseManager/Query")
	if svc != "trb.clickhouse.manager.public.contract.v1.ClickHouseManager" {
		t.Fatalf("service: %q", svc)
	}
	if method != "Query" {
		t.Fatalf("method: %q", method)
	}
}

func TestSkipHealth(t *testing.T) {
	if !skipMethod("/grpc.health.v1.Health/Check") {
		t.Fatal("health не должен попадать в access-лог")
	}
	if skipMethod("/trb.db.api.public.contract.v1.DbApi/ListInstruments") {
		t.Fatal("обычный метод не скип")
	}
}
