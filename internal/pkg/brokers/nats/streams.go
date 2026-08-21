package trb_nats

import (
	"context"
	"fmt"
	"time"

	nats_admin "github.com/Mar1eena/trb_proto/gen/go/api/nats"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (js *Nats) AddStream(context context.Context, cfg *nats_admin.StreamConfig, opts ...nats.JSOpt) (*nats_admin.StreamInfos, error) {

	streamCfg := ConvertCfgTrb(cfg)

	streamInfo, err := js.Jsc.AddStream(streamCfg, opts...)
	if err != nil {
		return nil, err
	}
	return ConvertCfgNats(streamInfo), nil
}

func (js *Nats) UpdateStream(context context.Context, cfg *nats_admin.StreamConfig, opts ...nats.JSOpt) (*nats_admin.StreamInfos, error) {

	streamCfg := ConvertCfgTrb(cfg)

	streamInfo, err := js.Jsc.UpdateStream(streamCfg, opts...)
	if err != nil {
		return nil, err
	}

	return ConvertCfgNats(streamInfo), nil
}

func (js *Nats) DeleteStream(context context.Context, req *nats_admin.StreamName, opts ...nats.JSOpt) (*nats_admin.Response, error) {
	err := js.Jsc.DeleteStream(req.Name, opts...)
	if err != nil {
		return nil, err
	}

	return &nats_admin.Response{Response: "OK"}, nil
}

func (js *Nats) StreamInfo(ncontext context.Context, req *nats_admin.StreamName, opts ...nats.JSOpt) (*nats_admin.StreamInfos, error) {
	streamInfo, err := js.Jsc.StreamInfo(req.Name)
	if err != nil {
		return nil, err
	}

	return ConvertCfgNats(streamInfo), nil
}

func (js *Nats) PurgeStream(context context.Context, req *nats_admin.StreamName, opts ...nats.JSOpt) (*nats_admin.Response, error) {
	err := js.Jsc.PurgeStream(req.Name)
	if err != nil {
		return nil, err
	}

	return &nats_admin.Response{Response: "OK"}, nil
}

func (js *Nats) StreamsInfo(ctx context.Context, opts *nats_admin.JsOpts) (*nats_admin.StreamList, error) {
	return js.Streams(ctx, opts)
}

func (js *Nats) Streams(ctx context.Context, _ *nats_admin.JsOpts) (*nats_admin.StreamList, error) {
	ch := js.Jsc.Streams(nats.Context(ctx))
	if ch == nil {
		return nil, fmt.Errorf("не удалось получить список стримов")
	}
	items := make([]*nats_admin.StreamInfos, 0)
	for info := range ch {
		if info == nil {
			continue
		}
		items = append(items, ConvertCfgNats(info))
	}
	return &nats_admin.StreamList{Items: items}, nil
}

func (js *Nats) StreamNames(ctx context.Context, _ *nats_admin.JsOpts) (*nats_admin.StreamNameList, error) {
	ch := js.Jsc.StreamNames(nats.Context(ctx))
	if ch == nil {
		return nil, fmt.Errorf("не удалось получить имена стримов")
	}
	names := make([]string, 0)
	for name := range ch {
		if name == "" {
			continue
		}
		names = append(names, name)
	}
	return &nats_admin.StreamNameList{Names: names}, nil
}

func ConvertCfgTrb(req *nats_admin.StreamConfig) *nats.StreamConfig {
	if req == nil {
		return nil
	}

	stream := &nats.StreamConfig{
		Name:                   req.Name,
		Description:            req.Description,
		Subjects:               req.Subjects,
		Retention:              nats.RetentionPolicy(req.Retention),
		MaxConsumers:           int(req.MaxConsumers),
		MaxMsgs:                req.MaxMsgs,
		MaxBytes:               req.MaxBytes,
		Discard:                nats.DiscardPolicy(req.Discard),
		DiscardNewPerSubject:   req.DiscardNewPerSubject,
		MaxAge:                 time.Duration(req.MaxAge),
		MaxMsgsPerSubject:      req.MaxMsgsPerSubject,
		MaxMsgSize:             req.MaxMsgSize,
		Storage:                nats.StorageType(req.Storage),
		Replicas:               int(req.Replicas),
		NoAck:                  req.NoAck,
		Duplicates:             time.Duration(req.DuplicateWindow),
		Sealed:                 req.Sealed,
		DenyDelete:             req.DenyDelete,
		DenyPurge:              req.DenyPurge,
		AllowRollup:            req.AllowRollup,
		Compression:            nats.StoreCompression(req.Compression),
		FirstSeq:               req.FirstSeq,
		AllowDirect:            req.AllowDirect,
		MirrorDirect:           req.MirrorDirect,
		Metadata:               req.Metadata,
		Template:               req.TemplateOwner,
		AllowMsgTTL:            req.AllowMsgTtl,
		SubjectDeleteMarkerTTL: time.Duration(req.SubjectDeleteMarkerTtl),
		Placement:              convertPlacementToNats(req.Placement),
		Mirror:                 convertMirrorSourceToNats(req.Mirror),
		Sources:                convertSourcesToNats(req.Sources),
		SubjectTransform:       convertSubjectTransformToNats(req.SubjectTransform),
		RePublish:              convertRepublishToNats(req.Republish),
		ConsumerLimits:         convertConsumerLimitsToNats(req.ConsumerLimits),
	}

	return stream
}

func convertStreamConfig(config *nats.StreamConfig) *nats_admin.StreamConfig {
	if config == nil {
		return nil
	}

	return &nats_admin.StreamConfig{
		Name:                   config.Name,
		Description:            config.Description,
		Subjects:               config.Subjects,
		Retention:              int32(config.Retention),
		MaxConsumers:           int32(config.MaxConsumers),
		MaxMsgs:                int64(config.MaxMsgs),
		MaxBytes:               int64(config.MaxBytes),
		Discard:                int32(config.Discard),
		DiscardNewPerSubject:   config.DiscardNewPerSubject,
		MaxAge:                 int64(config.MaxAge),
		MaxMsgsPerSubject:      int64(config.MaxMsgsPerSubject),
		MaxMsgSize:             int32(config.MaxMsgSize),
		Storage:                int32(config.Storage),
		Replicas:               int32(config.Replicas),
		NoAck:                  config.NoAck,
		DuplicateWindow:        int64(config.Duplicates),
		Placement:              convertPlacement(config.Placement),
		Mirror:                 convertStreamSource(config.Mirror),
		Sources:                convertStreamSources(config.Sources),
		Sealed:                 config.Sealed,
		DenyDelete:             config.DenyDelete,
		DenyPurge:              config.DenyPurge,
		AllowRollup:            config.AllowRollup,
		Compression:            int32(config.Compression),
		FirstSeq:               config.FirstSeq,
		SubjectTransform:       convertSubjectTransform(config.SubjectTransform),
		Republish:              convertRepublish(config.RePublish),
		AllowDirect:            config.AllowDirect,
		MirrorDirect:           config.MirrorDirect,
		ConsumerLimits:         convertConsumerLimits(config.ConsumerLimits),
		Metadata:               config.Metadata,
		TemplateOwner:          config.Template,
		AllowMsgTtl:            config.AllowMsgTTL,
		SubjectDeleteMarkerTtl: int64(config.SubjectDeleteMarkerTTL),
	}
}

func convertPlacementToNats(placement *nats_admin.Placement) *nats.Placement {
	if placement == nil {
		return nil
	}
	return &nats.Placement{
		Cluster: placement.Cluster,
		Tags:    placement.Tags,
	}
}

func convertMirrorSourceToNats(mirror *nats_admin.StreamSource) *nats.StreamSource {
	if mirror == nil {
		return nil
	}

	optStartTime := mirror.OptStartTime.AsTime()
	return &nats.StreamSource{
		Name:              mirror.Name,
		OptStartSeq:       mirror.OptStartSeq,
		OptStartTime:      &optStartTime,
		FilterSubject:     mirror.FilterSubject,
		SubjectTransforms: convertSubjectTransformsToNats(mirror.SubjectTransforms),
		External:          convertExternalStreamToNats(mirror.External),
		Domain:            mirror.Domain,
	}
}

func convertSourcesToNats(sources []*nats_admin.StreamSource) []*nats.StreamSource {
	if len(sources) == 0 {
		return nil
	}

	result := make([]*nats.StreamSource, len(sources))
	for i, source := range sources {
		optStartTime := source.OptStartTime.AsTime()
		result[i] = &nats.StreamSource{
			Name:              source.Name,
			OptStartSeq:       source.OptStartSeq,
			OptStartTime:      &optStartTime,
			FilterSubject:     source.FilterSubject,
			SubjectTransforms: convertSubjectTransformsToNats(source.SubjectTransforms),
			External:          convertExternalStreamToNats(source.External),
			Domain:            source.Domain,
		}
	}
	return result
}

func convertSubjectTransformsToNats(transforms []*nats_admin.SubjectTransformConfig) []nats.SubjectTransformConfig {
	if len(transforms) == 0 {
		return nil
	}

	result := make([]nats.SubjectTransformConfig, len(transforms))
	for i, t := range transforms {
		result[i] = nats.SubjectTransformConfig{
			Source:      t.Src,
			Destination: t.Dest,
		}
	}
	return result
}

func convertExternalStreamToNats(external *nats_admin.ExternalStream) *nats.ExternalStream {
	if external == nil {
		return nil
	}
	return &nats.ExternalStream{
		APIPrefix:     external.Api,
		DeliverPrefix: external.Deliver,
	}
}

func convertSubjectTransformToNats(transform *nats_admin.SubjectTransformConfig) *nats.SubjectTransformConfig {
	if transform == nil {
		return nil
	}
	return &nats.SubjectTransformConfig{
		Source:      transform.Src,
		Destination: transform.Dest,
	}
}

func convertRepublishToNats(republish *nats_admin.RePublish) *nats.RePublish {
	if republish == nil {
		return nil
	}
	return &nats.RePublish{
		Source:      republish.Src,
		Destination: republish.Dest,
		HeadersOnly: republish.HeadersOnly,
	}
}

func convertConsumerLimitsToNats(limits *nats_admin.StreamConsumerLimits) nats.StreamConsumerLimits {
	if limits == nil {
		return nats.StreamConsumerLimits{}
	}
	return nats.StreamConsumerLimits{
		InactiveThreshold: time.Duration(limits.InactiveThreshold),
		MaxAckPending:     int(limits.MaxAckPending),
	}
}

func ConvertCfgNats(req *nats.StreamInfo) *nats_admin.StreamInfos {
	if req == nil {
		return nil
	}

	return &nats_admin.StreamInfos{
		Config:     convertStreamConfig(&req.Config),
		Created:    timestamppb.New(req.Created),
		State:      convertStreamState(req.State),
		Cluster:    convertClusterInfo(req.Cluster),
		Mirror:     convertStreamSourceInfo(req.Mirror),
		Sources:    convertStreamSourcesInfo(req.Sources),
		Alternates: convertStreamAlternates(req.Alternates),
	}
}

// Вспомогательные функции для каждого компонента

func convertPlacement(placement *nats.Placement) *nats_admin.Placement {
	if placement == nil {
		return nil
	}
	return &nats_admin.Placement{
		Cluster: placement.Cluster,
		Tags:    placement.Tags,
	}
}

func convertStreamSource(source *nats.StreamSource) *nats_admin.StreamSource {
	if source == nil {
		return nil
	}

	var optStartTime *timestamppb.Timestamp
	if source.OptStartTime != nil {
		optStartTime = timestamppb.New(*source.OptStartTime)
	}

	return &nats_admin.StreamSource{
		Name:              source.Name,
		OptStartSeq:       source.OptStartSeq,
		OptStartTime:      optStartTime,
		FilterSubject:     source.FilterSubject,
		SubjectTransforms: convertSubjectTransforms(source.SubjectTransforms),
		External:          convertExternalStream(source.External),
		Domain:            source.Domain,
	}
}

func convertStreamSources(sources []*nats.StreamSource) []*nats_admin.StreamSource {
	if len(sources) == 0 {
		return nil
	}

	result := make([]*nats_admin.StreamSource, len(sources))
	for i, source := range sources {
		result[i] = convertStreamSource(source)
	}
	return result
}

func convertSubjectTransform(transform *nats.SubjectTransformConfig) *nats_admin.SubjectTransformConfig {
	if transform == nil {
		return nil
	}
	return &nats_admin.SubjectTransformConfig{
		Src:  transform.Source,
		Dest: transform.Destination,
	}
}

func convertSubjectTransforms(transforms []nats.SubjectTransformConfig) []*nats_admin.SubjectTransformConfig {
	if len(transforms) == 0 {
		return nil
	}

	result := make([]*nats_admin.SubjectTransformConfig, len(transforms))
	for i, t := range transforms {
		result[i] = &nats_admin.SubjectTransformConfig{
			Src:  t.Source,
			Dest: t.Destination,
		}
	}
	return result
}

func convertExternalStream(external *nats.ExternalStream) *nats_admin.ExternalStream {
	if external == nil {
		return nil
	}
	return &nats_admin.ExternalStream{
		Api:     external.APIPrefix,
		Deliver: external.DeliverPrefix,
	}
}

func convertRepublish(republish *nats.RePublish) *nats_admin.RePublish {
	if republish == nil {
		return nil
	}
	return &nats_admin.RePublish{
		Src:         republish.Source,
		Dest:        republish.Destination,
		HeadersOnly: republish.HeadersOnly,
	}
}

func convertConsumerLimits(limits nats.StreamConsumerLimits) *nats_admin.StreamConsumerLimits {
	if limits == (nats.StreamConsumerLimits{}) {
		return nil
	}
	return &nats_admin.StreamConsumerLimits{
		InactiveThreshold: int64(limits.InactiveThreshold),
		MaxAckPending:     int64(limits.MaxAckPending),
	}
}

func convertStreamState(state nats.StreamState) *nats_admin.StreamState {
	return &nats_admin.StreamState{
		Msgs:          state.Msgs,
		Bytes:         state.Bytes,
		FirstSeq:      state.FirstSeq,
		FirstTs:       timestamppb.New(state.FirstTime),
		LastSeq:       state.LastSeq,
		LastTs:        timestamppb.New(state.LastTime),
		ConsumerCount: int32(state.Consumers),
		Deleted:       state.Deleted,
		NumDeleted:    int32(state.NumDeleted),
		NumSubjects:   state.NumSubjects,
		Subjects:      state.Subjects,
	}
}

func convertClusterInfo(cluster *nats.ClusterInfo) *nats_admin.ClusterInfo {
	if cluster == nil {
		return nil
	}

	replicas := make([]*nats_admin.PeerInfo, len(cluster.Replicas))
	for i, replica := range cluster.Replicas {
		replicas[i] = &nats_admin.PeerInfo{
			Name:    replica.Name,
			Current: replica.Current,
			Offline: replica.Offline,
			Active:  int64(replica.Active),
			Lag:     replica.Lag,
		}
	}

	return &nats_admin.ClusterInfo{
		Leader:   cluster.Leader,
		Name:     cluster.Name,
		Replicas: replicas,
	}
}

func convertStreamSourceInfo(source *nats.StreamSourceInfo) *nats_admin.StreamSourceInfo {
	if source == nil {
		return nil
	}

	var apiError *nats_admin.APIError
	if source.Error != nil {
		apiError = &nats_admin.APIError{
			Code:        int32(source.Error.Code),
			Description: source.Error.Description,
		}
	}

	return &nats_admin.StreamSourceInfo{
		Name:              source.Name,
		Lag:               source.Lag,
		Active:            int64(source.Active),
		External:          convertExternalStream(source.External),
		Error:             apiError,
		FilterSubject:     source.FilterSubject,
		SubjectTransforms: convertSubjectTransforms(source.SubjectTransforms),
	}
}

func convertStreamSourcesInfo(sources []*nats.StreamSourceInfo) []*nats_admin.StreamSourceInfo {
	if len(sources) == 0 {
		return nil
	}

	result := make([]*nats_admin.StreamSourceInfo, len(sources))
	for i, source := range sources {
		result[i] = convertStreamSourceInfo(source)
	}
	return result
}

func convertStreamAlternates(alternates []*nats.StreamAlternate) []*nats_admin.StreamAlternate {
	if len(alternates) == 0 {
		return nil
	}

	result := make([]*nats_admin.StreamAlternate, len(alternates))
	for i, alt := range alternates {
		result[i] = &nats_admin.StreamAlternate{
			Name:    alt.Name,
			Domain:  alt.Domain,
			Cluster: alt.Cluster,
		}
	}
	return result
}
