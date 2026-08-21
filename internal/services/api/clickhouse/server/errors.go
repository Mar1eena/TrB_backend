package server

import (
	"errors"

	"github.com/ClickHouse/clickhouse-go/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func mapErr(err error) error {
	if err == nil {
		return nil
	}
	if st, ok := status.FromError(err); ok {
		return st.Err()
	}
	var ex *clickhouse.Exception
	if errors.As(err, &ex) {
		switch ex.Code {
		case 16, 60, 81: // NO_SUCH_COLUMN_IN_TABLE, UNKNOWN_TABLE, UNKNOWN_DATABASE
			return status.Errorf(codes.NotFound, "%s", ex.Message)
		case 15, 57, 82: // DUPLICATE_COLUMN, TABLE_ALREADY_EXISTS, DATABASE_ALREADY_EXISTS
			return status.Errorf(codes.AlreadyExists, "%s", ex.Message)
		case 36, 43, 47, 62: // BAD_ARGUMENTS, ILLEGAL_COLUMN, UNKNOWN_IDENTIFIER, SYNTAX_ERROR
			return status.Errorf(codes.InvalidArgument, "%s", ex.Message)
		default:
			return status.Errorf(codes.Internal, "[%d] %s", ex.Code, ex.Message)
		}
	}
	return status.Errorf(codes.Internal, "%v", err)
}
