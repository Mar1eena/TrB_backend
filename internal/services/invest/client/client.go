package client

import (
	"errors"

	"github.com/Mar1eena/TrB_V3/internal/pkg/env"
	"github.com/Mar1eena/TrB_V3/internal/pkg/grpcx"
	tinvest "github.com/Mar1eena/trb_proto/gen/go/api/tinvest"
	"google.golang.org/grpc"
)

// Dial opens an insecure gRPC connection to the invest facade service.
func Dial(addr string) (*grpc.ClientConn, error) {
	return grpcx.DialInsecure(addr)
}

// DialFromEnv dials using INVEST_API_URL / INVEST_API_URL_DOCKER.
func DialFromEnv() (*grpc.ClientConn, error) {
	addr := env.Addr("INVEST_API_URL", "INVEST_API_URL_DOCKER")
	if addr == "" {
		return nil, errors.New("INVEST_API_URL не задан")
	}
	return Dial(addr)
}

// NewInstruments returns a typed InstrumentsService client.
func NewInstruments(conn grpc.ClientConnInterface) tinvest.InstrumentsServiceClient {
	return tinvest.NewInstrumentsServiceClient(conn)
}
