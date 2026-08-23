package pkg

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func MapErr(err error) error {
	if err == nil {
		return nil
	}
	if st, ok := status.FromError(err); ok {
		return st.Err()
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "42P01", "42703", "42704", "3D000", "3F000": // undefined table/column/object, invalid catalog/schema
			return status.Errorf(codes.NotFound, "%s", pgErr.Message)
		case "42P04", "42P06", "42P07", "42710", "23505": // duplicate database/schema/table/object, unique_violation
			return status.Errorf(codes.AlreadyExists, "%s", pgErr.Message)
		case "42601", "22P02", "22023", "42804", "42883": // syntax, invalid text, invalid parameter, datatype mismatch, undefined function
			return status.Errorf(codes.InvalidArgument, "%s", pgErr.Message)
		case "42501":
			return status.Errorf(codes.PermissionDenied, "%s", pgErr.Message)
		case "53300", "53100":
			return status.Errorf(codes.ResourceExhausted, "%s", pgErr.Message)
		case "57014":
			return status.Errorf(codes.Canceled, "%s", pgErr.Message)
		case "40P01", "55P03":
			return status.Errorf(codes.Aborted, "%s", pgErr.Message)
		case "55006":
			return status.Errorf(codes.FailedPrecondition, "%s", pgErr.Message)
		default:
			return status.Errorf(codes.Internal, "[%s] %s", pgErr.Code, pgErr.Message)
		}
	}
	return status.Errorf(codes.Internal, "%v", err)
}
