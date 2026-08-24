package server

import (
	"context"
	"sort"

	chmgr "github.com/Mar1eena/trb_proto/gen/go/clickhouse"
)

// Codecs are not exposed via a system.* catalog in ClickHouse; keep a curated server-side list.
var defaultCodecs = []string{
	"NONE",
	"LZ4",
	"LZ4HC(6)",
	"ZSTD(1)",
	"ZSTD(3)",
	"ZSTD(6)",
	"Delta",
	"DoubleDelta",
	"Gorilla",
	"T64",
	"FPC",
	"AES_128_GCM_SIV",
	"AES_256_GCM_SIV",
}

// Common parameterized type forms that are useful in the UI but not listed as families.
var commonTypeExtras = []string{
	"DateTime('UTC')",
	"DateTime64(3)",
	"DateTime64(3, 'UTC')",
	"DateTime64(6)",
	"DateTime64(6, 'UTC')",
	"DateTime64(9)",
	"Decimal(18, 4)",
	"Decimal(38, 8)",
	"FixedString(16)",
	"FixedString(32)",
	"Nullable(String)",
	"Nullable(UInt64)",
	"Nullable(Int64)",
	"Nullable(Float64)",
	"Nullable(DateTime)",
	"Nullable(Date)",
	"Array(String)",
	"Array(UInt64)",
	"Array(Float64)",
	"Map(String, String)",
	"Map(String, UInt64)",
	"Tuple(String, Int64)",
	"LowCardinality(String)",
	"LowCardinality(Nullable(String))",
	"Enum8('a' = 1, 'b' = 2)",
	"Enum16('x' = 100, 'y' = 200)",
	"SimpleAggregateFunction(sum, UInt64)",
	"SimpleAggregateFunction(max, DateTime)",
	"SimpleAggregateFunction(min, Float64)",
	"AggregateFunction(uniq, String)",
}

func (s *Server) GetTableOptions(ctx context.Context, _ *chmgr.TableOptionsRequest) (*chmgr.TableOptionsResponse, error) {
	engines := s.selectNames(ctx, `SELECT name FROM system.table_engines ORDER BY name`)
	families := s.selectNames(ctx, `SELECT name FROM system.data_type_families WHERE alias_to = '' ORDER BY name`)
	settings := s.selectNames(ctx, `SELECT name FROM system.merge_tree_settings ORDER BY name`)

	types := mergeUniqueSorted(append(append([]string{}, commonTypeExtras...), families...))

	s.log.Info().
		Int("engines", len(engines)).
		Int("data_types", len(types)).
		Int("merge_tree_settings", len(settings)).
		Msg("опции создания таблицы загружены из ClickHouse")

	return &chmgr.TableOptionsResponse{
		Engines:           engines,
		DataTypes:         types,
		MergeTreeSettings: settings,
		Codecs:            append([]string{}, defaultCodecs...),
	}, nil
}

func (s *Server) selectNames(ctx context.Context, query string) []string {
	var rows []struct {
		Name string `ch:"name"`
	}
	if err := s.db(ctx).Select(ctx, &rows, query); err != nil {
		s.log.Warn().Err(err).Str("query", query).Msg("не удалось загрузить каталог из ClickHouse")
		return nil
	}
	out := make([]string, 0, len(rows))
	for i := range rows {
		if rows[i].Name != "" {
			out = append(out, rows[i].Name)
		}
	}
	return out
}

func mergeUniqueSorted(items []string) []string {
	seen := make(map[string]struct{}, len(items))
	out := make([]string, 0, len(items))
	for _, item := range items {
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	sort.Strings(out)
	return out
}
