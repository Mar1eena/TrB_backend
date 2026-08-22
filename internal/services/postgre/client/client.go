package client

import (
	"errors"

	"github.com/Mar1eena/TrB_V3/internal/pkg/env"
	"github.com/Mar1eena/TrB_V3/internal/pkg/grpcx"
	pgapi "github.com/Mar1eena/trb_proto/gen/go/postgresql"
	"google.golang.org/grpc"
)

// Dial opens an insecure gRPC connection to the postgre service.
func Dial(addr string) (*grpc.ClientConn, error) {
	return grpcx.DialInsecure(addr)
}

// DialFromEnv dials using POSTGRE_API_URL / POSTGRE_API_URL_DOCKER.
func DialFromEnv() (*grpc.ClientConn, error) {
	addr := env.Addr("POSTGRE_API_URL", "POSTGRE_API_URL_DOCKER")
	if addr == "" {
		return nil, errors.New("POSTGRE_API_URL не задан")
	}
	return Dial(addr)
}

// New returns a typed PostgreSQL client.
func New(conn grpc.ClientConnInterface) pgapi.PostgreSQLClient {
	return pgapi.NewPostgreSQLClient(conn)
}
