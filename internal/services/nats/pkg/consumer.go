package pkg

import nats_admin "github.com/Mar1eena/trb_proto/gen/go/nats"

func ConsumerName(req *nats_admin.Consumer) string {
	if req.GetConfig() == nil {
		return ""
	}
	if req.GetConfig().GetName() != "" {
		return req.GetConfig().GetName()
	}
	return req.GetConfig().GetDurable()
}
