package server

import (
	"context"
	"fmt"
	"strings"

	chmgr "github.com/Mar1eena/trb_proto/gen/go/clickhouse"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type processRow struct {
	QueryID     string  `ch:"query_id"`
	User        string  `ch:"user"`
	Elapsed     float64 `ch:"elapsed"`
	ReadRows    uint64  `ch:"read_rows"`
	ReadBytes   uint64  `ch:"read_bytes"`
	MemoryUsage int64   `ch:"memory_usage"`
	Query       string  `ch:"query"`
	ClientName  string  `ch:"client_name"`
	OSUser      string  `ch:"os_user"`
	IsCancelled uint8   `ch:"is_cancelled"`
}

func (s *Server) ListProcesses(ctx context.Context, _ *chmgr.ListProcessesRequest) (*chmgr.ProcessList, error) {
	query := `
SELECT
	query_id,
	user,
	elapsed,
	read_rows,
	read_bytes,
	memory_usage,
	query,
	client_name,
	os_user,
	is_cancelled
FROM system.processes
ORDER BY elapsed DESC`

	var rows []processRow
	if err := s.ch.Select(ctx, &rows, query); err != nil {
		s.log.Error().Err(err).Msg("не удалось получить список активных процессов")
		return nil, mapErr(err)
	}

	items := make([]*chmgr.ProcessInfo, 0, len(rows))
	for i := range rows {
		mem := rows[i].MemoryUsage
		if mem < 0 {
			mem = 0
		}
		items = append(items, &chmgr.ProcessInfo{
			QueryId:        rows[i].QueryID,
			User:           rows[i].User,
			ElapsedSeconds: rows[i].Elapsed,
			RowsRead:       rows[i].ReadRows,
			BytesRead:      rows[i].ReadBytes,
			MemoryUsage:    uint64(mem),
			Query:          rows[i].Query,
			ClientName:     rows[i].ClientName,
			OsUser:         rows[i].OSUser,
			IsCancelled:    rows[i].IsCancelled == 1,
		})
	}

	s.log.Info().Int("count", len(items)).Msg("список процессов ClickHouse получен")
	return &chmgr.ProcessList{Items: items}, nil
}

func (s *Server) KillProcess(ctx context.Context, req *chmgr.KillProcessRequest) (*chmgr.Status, error) {
	if req == nil || strings.TrimSpace(req.GetQueryId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "ID запроса обязателен")
	}

	qid := strings.TrimSpace(req.GetQueryId())
	sql := fmt.Sprintf("KILL QUERY WHERE query_id = %s SYNC", quote(qid))

	out, err := s.exec(ctx, sql)
	if err != nil {
		s.log.Error().Err(err).Str("query_id", qid).Msg("не удалось завершить процесс")
		return nil, err
	}

	s.log.Info().Str("query_id", qid).Msg("процесс успешно завершен")
	return out, nil
}

type diskRow struct {
	Name            string `ch:"name"`
	Path            string `ch:"path"`
	FreeSpace       uint64 `ch:"free_space"`
	TotalSpace      uint64 `ch:"total_space"`
	UnreservedSpace uint64 `ch:"unreserved_space"`
	Type            string `ch:"type"`
}

func (s *Server) ListDisks(ctx context.Context, _ *chmgr.ListDisksRequest) (*chmgr.DiskList, error) {
	query := `
SELECT
	name,
	path,
	free_space,
	total_space,
	unreserved_space,
	type
FROM system.disks
ORDER BY name`

	var rows []diskRow
	if err := s.ch.Select(ctx, &rows, query); err != nil {
		s.log.Error().Err(err).Msg("не удалось получить список дисков")
		return nil, mapErr(err)
	}

	items := make([]*chmgr.DiskInfo, 0, len(rows))
	for i := range rows {
		items = append(items, &chmgr.DiskInfo{
			Name:            rows[i].Name,
			Path:            rows[i].Path,
			FreeSpace:       rows[i].FreeSpace,
			TotalSpace:      rows[i].TotalSpace,
			UnreservedSpace: rows[i].UnreservedSpace,
			Type:            rows[i].Type,
		})
	}

	s.log.Info().Int("count", len(items)).Msg("информация о дисках получена")
	return &chmgr.DiskList{Items: items}, nil
}

type metricRow struct {
	Name        string  `ch:"name"`
	Value       float64 `ch:"value"`
	Description string  `ch:"description"`
}

func (s *Server) GetMetrics(ctx context.Context, _ *chmgr.GetMetricsRequest) (*chmgr.MetricsResponse, error) {
	var metricsRows []metricRow
	_ = s.ch.Select(ctx, &metricsRows, `
SELECT metric AS name, toFloat64(value) AS value, description
FROM system.metrics
ORDER BY metric`)

	var asyncRows []metricRow
	_ = s.ch.Select(ctx, &asyncRows, `
SELECT metric AS name, toFloat64(value) AS value, description
FROM system.asynchronous_metrics
ORDER BY metric`)

	metrics := make([]*chmgr.MetricItem, 0, len(metricsRows))
	for i := range metricsRows {
		metrics = append(metrics, &chmgr.MetricItem{
			Name:        metricsRows[i].Name,
			Value:       metricsRows[i].Value,
			Description: metricsRows[i].Description,
		})
	}

	asyncMetrics := make([]*chmgr.MetricItem, 0, len(asyncRows))
	for i := range asyncRows {
		asyncMetrics = append(asyncMetrics, &chmgr.MetricItem{
			Name:        asyncRows[i].Name,
			Value:       asyncRows[i].Value,
			Description: asyncRows[i].Description,
		})
	}

	s.log.Info().Int("metrics", len(metrics)).Int("async_metrics", len(asyncMetrics)).Msg("метрики ClickHouse получены")
	return &chmgr.MetricsResponse{
		Metrics:      metrics,
		AsyncMetrics: asyncMetrics,
	}, nil
}
