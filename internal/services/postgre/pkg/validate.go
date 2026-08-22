package pkg

import (
	pgapi "github.com/Mar1eena/trb_proto/gen/go/postgresql"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func ValidateSync(instruments []*pgapi.SchedulerTargetInstrument, allowEmpty bool) error {
	if len(instruments) == 0 && !allowEmpty {
		return status.Error(codes.InvalidArgument, "пустой список сотрёт все цели; передайте allow_empty")
	}
	return nil
}
