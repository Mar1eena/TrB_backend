package client

import (
	"errors"

	"github.com/Mar1eena/TrB_V3/internal/pkg/env"
	"github.com/Mar1eena/TrB_V3/internal/pkg/grpcx"
	dbapi "github.com/Mar1eena/trb_proto/gen/go/api/db_api"
	"google.golang.org/grpc"
)

// Dial opens an insecure gRPC connection to the data (DbApi) service.
func Dial(addr string) (*grpc.ClientConn, error) {
	return grpcx.DialInsecure(addr)
}

// DialFromEnv dials using DATA_API_URL / DATA_API_URL_DOCKER.
func DialFromEnv() (*grpc.ClientConn, error) {
	addr := env.Addr("DATA_API_URL", "DATA_API_URL_DOCKER")
	if addr == "" {
		return nil, errors.New("DATA_API_URL не задан")
	}
	return Dial(addr)
}

// New returns a typed DbApi client.
func New(conn grpc.ClientConnInterface) dbapi.DbApiClient {
	return dbapi.NewDbApiClient(conn)
}
