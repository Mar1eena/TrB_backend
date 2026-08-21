package server

import (
	"fmt"
	"sort"
	"strings"

	chmgr "github.com/Mar1eena/trb_proto/gen/go/api/clickhouse"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func CreateDatabaseSQL(spec *chmgr.DatabaseSpec) (string, error) {
	if spec == nil {
		return "", status.Error(codes.InvalidArgument, "тело запроса обязательно")
	}
	name, err := QuoteIdent(spec.GetName(), "name")
	if err != nil {
		return "", err
	}
	var b strings.Builder
	b.WriteString("CREATE DATABASE")
	if spec.GetIfNotExists() {
		b.WriteString(" IF NOT EXISTS")
	}
	b.WriteByte(' ')
	b.WriteString(name)
	on, err := clusterClause(spec.GetCluster())
	if err != nil {
		return "", err
	}
	b.WriteString(on)
	if engine := strings.TrimSpace(spec.GetEngine()); engine != "" {
		eng, err := ident(engine, "engine")
		if err != nil {
			return "", err
		}
		b.WriteString(" ENGINE = ")
		b.WriteString(eng)
	}
	if comment := spec.GetComment(); comment != "" {
		q, err := quoteString(comment)
		if err != nil {
			return "", err
		}
		b.WriteString(" COMMENT ")
		b.WriteString(q)
	}
	return b.String(), nil
}

func DropDatabaseSQL(req *chmgr.DatabaseName) (string, error) {
	if req == nil {
		return "", status.Error(codes.InvalidArgument, "тело запроса обязательно")
	}
	if isSystemDatabase(req.GetName()) {
		return "", status.Error(codes.InvalidArgument, "системную базу нельзя удалить")
	}
	name, err := QuoteIdent(req.GetName(), "name")
	if err != nil {
		return "", err
	}
	var b strings.Builder
	b.WriteString("DROP DATABASE")
	if req.GetIfExists() {
		b.WriteString(" IF EXISTS")
	}
	b.WriteByte(' ')
	b.WriteString(name)
	on, err := clusterClause(req.GetCluster())
	if err != nil {
		return "", err
	}
	b.WriteString(on)
	if req.GetSync() {
		b.WriteString(" SYNC")
	}
	return b.String(), nil
}

func columnDDL(col *chmgr.Column) (string, error) {
	if col == nil {
		return "", status.Error(codes.InvalidArgument, "колонка обязательна")
	}
	name, err := QuoteIdent(col.GetName(), "column.name")
	if err != nil {
		return "", err
	}
	typ, err := requireExpr(col.GetType(), "column.type")
	if err != nil {
		return "", err
	}
	var b strings.Builder
	b.WriteString(name)
	b.WriteByte(' ')
	b.WriteString(typ)
	if kind := strings.TrimSpace(col.GetDefaultKind()); kind != "" {
		k := strings.ToUpper(kind)
		switch k {
		case "DEFAULT", "MATERIALIZED", "ALIAS", "EPHEMERAL":
		default:
			return "", status.Errorf(codes.InvalidArgument, "недопустимый default_kind: %q", kind)
		}
		def, err := Expr(col.GetDefaultExpression(), "column.default_expression")
		if err != nil {
			return "", err
		}
		b.WriteByte(' ')
		b.WriteString(k)
		if def != "" {
			b.WriteByte(' ')
			b.WriteString(def)
		}
	}
	if codec := strings.TrimSpace(col.GetCodec()); codec != "" {
		c, err := Expr(codec, "column.codec")
		if err != nil {
			return "", err
		}
		b.WriteByte(' ')
		if strings.HasPrefix(strings.ToUpper(c), "CODEC") {
			b.WriteString(c)
		} else {
			b.WriteString("CODEC(")
			b.WriteString(c)
			b.WriteByte(')')
		}
	}
	if ttl := strings.TrimSpace(col.GetTtl()); ttl != "" {
		t, err := requireExpr(ttl, "column.ttl")
		if err != nil {
			return "", err
		}
		b.WriteString(" TTL ")
		b.WriteString(t)
	}
	if comment := col.GetComment(); comment != "" {
		q, err := quoteString(comment)
		if err != nil {
			return "", err
		}
		b.WriteString(" COMMENT ")
		b.WriteString(q)
	}
	return b.String(), nil
}

func engineSQL(engine *chmgr.TableEngine) (string, error) {
	if engine == nil || strings.TrimSpace(engine.GetName()) == "" {
		return "", status.Error(codes.InvalidArgument, "engine обязателен")
	}
	name, err := ident(engine.GetName(), "engine.name")
	if err != nil {
		return "", err
	}
	if len(engine.GetParams()) == 0 {
		return "ENGINE = " + name, nil
	}
	parts := make([]string, 0, len(engine.GetParams()))
	for i, p := range engine.GetParams() {
		v, err := requireExpr(p, fmt.Sprintf("engine.params[%d]", i))
		if err != nil {
			return "", err
		}
		parts = append(parts, v)
	}
	return "ENGINE = " + name + "(" + strings.Join(parts, ", ") + ")", nil
}

func CreateTableSQL(spec *chmgr.TableSpec) (string, error) {
	if spec == nil {
		return "", status.Error(codes.InvalidArgument, "тело запроса обязательно")
	}
	if len(spec.GetColumns()) == 0 {
		return "", status.Error(codes.InvalidArgument, "нужна хотя бы одна колонка")
	}
	tbl, err := qualified(spec.GetDatabase(), spec.GetName(), "database", "name")
	if err != nil {
		return "", err
	}
	cols := make([]string, 0, len(spec.GetColumns()))
	for _, col := range spec.GetColumns() {
		ddl, err := columnDDL(col)
		if err != nil {
			return "", err
		}
		cols = append(cols, ddl)
	}
	eng, err := engineSQL(spec.GetEngine())
	if err != nil {
		return "", err
	}
	on, err := clusterClause(spec.GetCluster())
	if err != nil {
		return "", err
	}

	var b strings.Builder
	b.WriteString("CREATE TABLE")
	if spec.GetIfNotExists() {
		b.WriteString(" IF NOT EXISTS")
	}
	b.WriteByte(' ')
	b.WriteString(tbl)
	b.WriteString(on)
	b.WriteString(" (\n  ")
	b.WriteString(strings.Join(cols, ",\n  "))
	b.WriteString("\n) ")
	b.WriteString(eng)

	if v, err := Expr(spec.GetOrderBy(), "order_by"); err != nil {
		return "", err
	} else if v != "" {
		b.WriteString(" ORDER BY ")
		b.WriteString(v)
	}
	if v, err := Expr(spec.GetPartitionBy(), "partition_by"); err != nil {
		return "", err
	} else if v != "" {
		b.WriteString(" PARTITION BY ")
		b.WriteString(v)
	}
	if v, err := Expr(spec.GetPrimaryKey(), "primary_key"); err != nil {
		return "", err
	} else if v != "" {
		b.WriteString(" PRIMARY KEY ")
		b.WriteString(v)
	}
	if v, err := Expr(spec.GetSampleBy(), "sample_by"); err != nil {
		return "", err
	} else if v != "" {
		b.WriteString(" SAMPLE BY ")
		b.WriteString(v)
	}
	if v, err := Expr(spec.GetTtl(), "ttl"); err != nil {
		return "", err
	} else if v != "" {
		b.WriteString(" TTL ")
		b.WriteString(v)
	}
	if settings := spec.GetSettings(); len(settings) > 0 {
		keys := make([]string, 0, len(settings))
		for k := range settings {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(keys))
		for _, k := range keys {
			key, err := ident(k, "settings")
			if err != nil {
				return "", err
			}
			sv, err := settingValue(settings[k])
			if err != nil {
				return "", err
			}
			parts = append(parts, key+" = "+sv)
		}
		b.WriteString(" SETTINGS ")
		b.WriteString(strings.Join(parts, ", "))
	}
	if comment := spec.GetComment(); comment != "" {
		q, err := quoteString(comment)
		if err != nil {
			return "", err
		}
		b.WriteString(" COMMENT ")
		b.WriteString(q)
	}
	return b.String(), nil
}

func dropTableSQL(req *chmgr.TableName) (string, error) {
	if req == nil {
		return "", status.Error(codes.InvalidArgument, "тело запроса обязательно")
	}
	tbl, err := qualified(req.GetDatabase(), req.GetName(), "database", "name")
	if err != nil {
		return "", err
	}
	on, err := clusterClause(req.GetCluster())
	if err != nil {
		return "", err
	}
	var b strings.Builder
	b.WriteString("DROP TABLE")
	if req.GetIfExists() {
		b.WriteString(" IF EXISTS")
	}
	b.WriteByte(' ')
	b.WriteString(tbl)
	b.WriteString(on)
	if req.GetSync() {
		b.WriteString(" SYNC")
	}
	return b.String(), nil
}

func truncateTableSQL(req *chmgr.TableName) (string, error) {
	if req == nil {
		return "", status.Error(codes.InvalidArgument, "тело запроса обязательно")
	}
	tbl, err := qualified(req.GetDatabase(), req.GetName(), "database", "name")
	if err != nil {
		return "", err
	}
	on, err := clusterClause(req.GetCluster())
	if err != nil {
		return "", err
	}
	var b strings.Builder
	b.WriteString("TRUNCATE TABLE")
	if req.GetIfExists() {
		b.WriteString(" IF EXISTS")
	}
	b.WriteByte(' ')
	b.WriteString(tbl)
	b.WriteString(on)
	return b.String(), nil
}

func RenameTableSQL(req *chmgr.RenameTableRequest) (string, error) {
	if req == nil {
		return "", status.Error(codes.InvalidArgument, "тело запроса обязательно")
	}
	from, err := qualified(req.GetDatabase(), req.GetName(), "database", "name")
	if err != nil {
		return "", err
	}
	newDB := req.GetNewDatabase()
	if strings.TrimSpace(newDB) == "" {
		newDB = req.GetDatabase()
	}
	to, err := qualified(newDB, req.GetNewName(), "new_database", "new_name")
	if err != nil {
		return "", err
	}
	on, err := clusterClause(req.GetCluster())
	if err != nil {
		return "", err
	}
	return "RENAME TABLE " + from + " TO " + to + on, nil
}

func OptimizeTableSQL(req *chmgr.OptimizeTableRequest) (string, error) {
	if req == nil {
		return "", status.Error(codes.InvalidArgument, "тело запроса обязательно")
	}
	tbl, err := qualified(req.GetDatabase(), req.GetName(), "database", "name")
	if err != nil {
		return "", err
	}
	on, err := clusterClause(req.GetCluster())
	if err != nil {
		return "", err
	}
	var b strings.Builder
	b.WriteString("OPTIMIZE TABLE ")
	b.WriteString(tbl)
	b.WriteString(on)
	if p := strings.TrimSpace(req.GetPartition()); p != "" {
		part, err := requireExpr(p, "partition")
		if err != nil {
			return "", err
		}
		b.WriteString(" PARTITION ")
		b.WriteString(part)
	}
	if req.GetFinal() {
		b.WriteString(" FINAL")
	}
	if req.GetDeduplicate() {
		b.WriteString(" DEDUPLICATE")
	}
	return b.String(), nil
}

func AddColumnSQL(req *chmgr.AddColumnRequest) (string, error) {
	if req == nil {
		return "", status.Error(codes.InvalidArgument, "тело запроса обязательно")
	}
	tbl, err := qualified(req.GetDatabase(), req.GetTable(), "database", "table")
	if err != nil {
		return "", err
	}
	col, err := columnDDL(req.GetColumn())
	if err != nil {
		return "", err
	}
	on, err := clusterClause(req.GetCluster())
	if err != nil {
		return "", err
	}
	var b strings.Builder
	b.WriteString("ALTER TABLE ")
	b.WriteString(tbl)
	b.WriteString(on)
	b.WriteString(" ADD COLUMN")
	if req.GetIfNotExists() {
		b.WriteString(" IF NOT EXISTS")
	}
	b.WriteByte(' ')
	b.WriteString(col)
	after := strings.TrimSpace(req.GetAfter())
	switch strings.ToUpper(after) {
	case "":
	case "FIRST":
		b.WriteString(" FIRST")
	default:
		q, err := QuoteIdent(after, "after")
		if err != nil {
			return "", err
		}
		b.WriteString(" AFTER ")
		b.WriteString(q)
	}
	return b.String(), nil
}

func DropColumnSQL(req *chmgr.DropColumnRequest) (string, error) {
	if req == nil {
		return "", status.Error(codes.InvalidArgument, "тело запроса обязательно")
	}
	tbl, err := qualified(req.GetDatabase(), req.GetTable(), "database", "table")
	if err != nil {
		return "", err
	}
	col, err := QuoteIdent(req.GetName(), "name")
	if err != nil {
		return "", err
	}
	on, err := clusterClause(req.GetCluster())
	if err != nil {
		return "", err
	}
	var b strings.Builder
	b.WriteString("ALTER TABLE ")
	b.WriteString(tbl)
	b.WriteString(on)
	b.WriteString(" DROP COLUMN")
	if req.GetIfExists() {
		b.WriteString(" IF EXISTS")
	}
	b.WriteByte(' ')
	b.WriteString(col)
	return b.String(), nil
}

func renameColumnSQL(req *chmgr.RenameColumnRequest) (string, error) {
	if req == nil {
		return "", status.Error(codes.InvalidArgument, "тело запроса обязательно")
	}
	tbl, err := qualified(req.GetDatabase(), req.GetTable(), "database", "table")
	if err != nil {
		return "", err
	}
	from, err := QuoteIdent(req.GetName(), "name")
	if err != nil {
		return "", err
	}
	to, err := QuoteIdent(req.GetNewName(), "new_name")
	if err != nil {
		return "", err
	}
	on, err := clusterClause(req.GetCluster())
	if err != nil {
		return "", err
	}
	return "ALTER TABLE " + tbl + on + " RENAME COLUMN " + from + " TO " + to, nil
}

func modifyColumnSQL(req *chmgr.ModifyColumnRequest) (string, error) {
	if req == nil {
		return "", status.Error(codes.InvalidArgument, "тело запроса обязательно")
	}
	tbl, err := qualified(req.GetDatabase(), req.GetTable(), "database", "table")
	if err != nil {
		return "", err
	}
	col, err := columnDDL(req.GetColumn())
	if err != nil {
		return "", err
	}
	on, err := clusterClause(req.GetCluster())
	if err != nil {
		return "", err
	}
	return "ALTER TABLE " + tbl + on + " MODIFY COLUMN " + col, nil
}
