package pkg

import (
	"fmt"
	"strings"

	pgapi "github.com/Mar1eena/trb_proto/gen/go/postgresql"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func CreateDatabaseSQL(spec *pgapi.DatabaseSpec) (string, error) {
	if spec == nil {
		return "", status.Error(codes.InvalidArgument, "тело запроса обязательно")
	}
	name, err := QuoteIdent(spec.GetName(), "name")
	if err != nil {
		return "", err
	}
	var with []string
	if v := strings.TrimSpace(spec.GetOwner()); v != "" {
		q, err := QuoteIdent(v, "owner")
		if err != nil {
			return "", err
		}
		with = append(with, "OWNER = "+q)
	}
	if v := strings.TrimSpace(spec.GetEncoding()); v != "" {
		q, err := QuoteString(v)
		if err != nil {
			return "", err
		}
		with = append(with, "ENCODING = "+q)
	}
	if v := strings.TrimSpace(spec.GetCollation()); v != "" {
		q, err := QuoteString(v)
		if err != nil {
			return "", err
		}
		with = append(with, "LC_COLLATE = "+q)
	}
	if v := strings.TrimSpace(spec.GetCtype()); v != "" {
		q, err := QuoteString(v)
		if err != nil {
			return "", err
		}
		with = append(with, "LC_CTYPE = "+q)
	}
	if v := strings.TrimSpace(spec.GetTemplate()); v != "" {
		q, err := QuoteIdent(v, "template")
		if err != nil {
			return "", err
		}
		with = append(with, "TEMPLATE = "+q)
	}
	if v := strings.TrimSpace(spec.GetTablespace()); v != "" {
		q, err := QuoteIdent(v, "tablespace")
		if err != nil {
			return "", err
		}
		with = append(with, "TABLESPACE = "+q)
	}
	if spec.GetConnectionLimit() != 0 {
		with = append(with, fmt.Sprintf("CONNECTION LIMIT = %d", spec.GetConnectionLimit()))
	}
	sql := "CREATE DATABASE " + name
	if len(with) > 0 {
		sql += " WITH " + strings.Join(with, " ")
	}
	return sql, nil
}

func DropDatabaseSQL(req *pgapi.DatabaseName) (string, error) {
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
	if req.GetForce() {
		b.WriteString(" WITH (FORCE)")
	}
	return b.String(), nil
}

func CreateSchemaSQL(spec *pgapi.SchemaSpec) (string, error) {
	if spec == nil {
		return "", status.Error(codes.InvalidArgument, "тело запроса обязательно")
	}
	if _, err := Ident(spec.GetDatabase(), "database"); err != nil {
		return "", err
	}
	name, err := QuoteIdent(spec.GetName(), "name")
	if err != nil {
		return "", err
	}
	var b strings.Builder
	b.WriteString("CREATE SCHEMA")
	if spec.GetIfNotExists() {
		b.WriteString(" IF NOT EXISTS")
	}
	b.WriteByte(' ')
	b.WriteString(name)
	if v := strings.TrimSpace(spec.GetOwner()); v != "" {
		q, err := QuoteIdent(v, "owner")
		if err != nil {
			return "", err
		}
		b.WriteString(" AUTHORIZATION ")
		b.WriteString(q)
	}
	return b.String(), nil
}

func DropSchemaSQL(req *pgapi.SchemaName) (string, error) {
	if req == nil {
		return "", status.Error(codes.InvalidArgument, "тело запроса обязательно")
	}
	if isSystemSchema(req.GetName()) {
		return "", status.Error(codes.InvalidArgument, "системную схему нельзя удалить")
	}
	if _, err := Ident(req.GetDatabase(), "database"); err != nil {
		return "", err
	}
	name, err := QuoteIdent(req.GetName(), "name")
	if err != nil {
		return "", err
	}
	var b strings.Builder
	b.WriteString("DROP SCHEMA")
	if req.GetIfExists() {
		b.WriteString(" IF EXISTS")
	}
	b.WriteByte(' ')
	b.WriteString(name)
	if req.GetCascade() {
		b.WriteString(" CASCADE")
	}
	return b.String(), nil
}

func columnDDL(col *pgapi.Column) (string, error) {
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
	if col.GetIsIdentity() && strings.TrimSpace(col.GetGeneratedExpression()) != "" {
		return "", status.Error(codes.InvalidArgument, "identity и generated_expression несовместимы")
	}
	var b strings.Builder
	b.WriteString(name)
	b.WriteByte(' ')
	b.WriteString(typ)
	if v := strings.TrimSpace(col.GetCollation()); v != "" {
		q, err := QuoteCollation(v, "column.collation")
		if err != nil {
			return "", err
		}
		b.WriteString(" COLLATE ")
		b.WriteString(q)
	}
	if !col.GetNullable() {
		b.WriteString(" NOT NULL")
	}
	if def := strings.TrimSpace(col.GetDefaultExpression()); def != "" && !col.GetIsIdentity() && strings.TrimSpace(col.GetGeneratedExpression()) == "" {
		e, err := requireExpr(def, "column.default_expression")
		if err != nil {
			return "", err
		}
		b.WriteString(" DEFAULT ")
		b.WriteString(e)
	}
	if gen := strings.TrimSpace(col.GetGeneratedExpression()); gen != "" {
		e, err := requireExpr(gen, "column.generated_expression")
		if err != nil {
			return "", err
		}
		b.WriteString(" GENERATED ALWAYS AS (")
		b.WriteString(e)
		b.WriteString(") STORED")
	}
	if col.GetIsIdentity() {
		kind := strings.ToUpper(strings.TrimSpace(col.GetIdentityGeneration()))
		switch kind {
		case "", "BY DEFAULT":
			b.WriteString(" GENERATED BY DEFAULT AS IDENTITY")
		case "ALWAYS":
			b.WriteString(" GENERATED ALWAYS AS IDENTITY")
		default:
			return "", status.Errorf(codes.InvalidArgument, "недопустимый identity_generation: %q", col.GetIdentityGeneration())
		}
	}
	if col.GetUnique() && !col.GetPrimaryKey() {
		b.WriteString(" UNIQUE")
	}
	return b.String(), nil
}

func commentOn(kind, target, comment string) (string, error) {
	q, err := QuoteString(comment)
	if err != nil {
		return "", err
	}
	return "COMMENT ON " + kind + " " + target + " IS " + q, nil
}

func CreateTableStatements(spec *pgapi.TableSpec) ([]string, error) {
	if spec == nil {
		return nil, status.Error(codes.InvalidArgument, "тело запроса обязательно")
	}
	if len(spec.GetColumns()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "нужна хотя бы одна колонка")
	}
	if spec.GetTemporary() && spec.GetUnlogged() {
		return nil, status.Error(codes.InvalidArgument, "TEMPORARY и UNLOGGED нельзя задать одновременно")
	}
	if _, err := Ident(spec.GetDatabase(), "database"); err != nil {
		return nil, err
	}

	var rel string
	var err error
	if spec.GetTemporary() {
		rel, err = QuoteIdent(spec.GetName(), "name")
	} else {
		rel, err = Qualified(spec.GetSchema(), spec.GetName(), "schema", "name")
	}
	if err != nil {
		return nil, err
	}

	cols := make([]string, 0, len(spec.GetColumns()))
	for _, col := range spec.GetColumns() {
		ddl, err := columnDDL(col)
		if err != nil {
			return nil, err
		}
		cols = append(cols, ddl)
	}

	pk := spec.GetPrimaryKey()
	if len(pk) == 0 {
		for _, col := range spec.GetColumns() {
			if col.GetPrimaryKey() {
				pk = append(pk, col.GetName())
			}
		}
	}
	if len(pk) > 0 {
		list, err := quoteIdentList(pk, "primary_key")
		if err != nil {
			return nil, err
		}
		cols = append(cols, "PRIMARY KEY ("+list+")")
	}

	var b strings.Builder
	b.WriteString("CREATE")
	if spec.GetTemporary() {
		b.WriteString(" TEMPORARY")
	}
	if spec.GetUnlogged() {
		b.WriteString(" UNLOGGED")
	}
	b.WriteString(" TABLE")
	if spec.GetIfNotExists() {
		b.WriteString(" IF NOT EXISTS")
	}
	b.WriteByte(' ')
	b.WriteString(rel)
	b.WriteString(" (\n  ")
	b.WriteString(strings.Join(cols, ",\n  "))
	b.WriteString("\n)")
	if v := strings.TrimSpace(spec.GetPartitionBy()); v != "" {
		e, err := requireExpr(v, "partition_by")
		if err != nil {
			return nil, err
		}
		b.WriteString(" PARTITION BY ")
		b.WriteString(e)
	}
	if v := strings.TrimSpace(spec.GetTablespace()); v != "" {
		q, err := QuoteIdent(v, "tablespace")
		if err != nil {
			return nil, err
		}
		b.WriteString(" TABLESPACE ")
		b.WriteString(q)
	}

	out := []string{b.String()}
	if c := spec.GetComment(); c != "" {
		sql, err := commentOn("TABLE", rel, c)
		if err != nil {
			return nil, err
		}
		out = append(out, sql)
	}
	for _, col := range spec.GetColumns() {
		if col.GetComment() == "" {
			continue
		}
		cn, err := QuoteIdent(col.GetName(), "column.name")
		if err != nil {
			return nil, err
		}
		sql, err := commentOn("COLUMN", rel+"."+cn, col.GetComment())
		if err != nil {
			return nil, err
		}
		out = append(out, sql)
	}
	return out, nil
}

func DropTableSQL(req *pgapi.TableName) (string, error) {
	if req == nil {
		return "", status.Error(codes.InvalidArgument, "тело запроса обязательно")
	}
	if _, err := Ident(req.GetDatabase(), "database"); err != nil {
		return "", err
	}
	rel, err := Qualified(req.GetSchema(), req.GetName(), "schema", "name")
	if err != nil {
		return "", err
	}
	var b strings.Builder
	b.WriteString("DROP TABLE")
	if req.GetIfExists() {
		b.WriteString(" IF EXISTS")
	}
	b.WriteByte(' ')
	b.WriteString(rel)
	if req.GetCascade() {
		b.WriteString(" CASCADE")
	}
	return b.String(), nil
}

func TruncateTableSQL(req *pgapi.TableName) (string, error) {
	if req == nil {
		return "", status.Error(codes.InvalidArgument, "тело запроса обязательно")
	}
	if _, err := Ident(req.GetDatabase(), "database"); err != nil {
		return "", err
	}
	rel, err := Qualified(req.GetSchema(), req.GetName(), "schema", "name")
	if err != nil {
		return "", err
	}
	sql := "TRUNCATE TABLE " + rel
	if req.GetRestartIdentity() {
		sql += " RESTART IDENTITY"
	}
	if req.GetCascade() {
		sql += " CASCADE"
	}
	return sql, nil
}

func RenameTableStatements(req *pgapi.RenameTableRequest) ([]string, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "тело запроса обязательно")
	}
	if _, err := Ident(req.GetDatabase(), "database"); err != nil {
		return nil, err
	}
	rel, err := Qualified(req.GetSchema(), req.GetName(), "schema", "name")
	if err != nil {
		return nil, err
	}
	newName := strings.TrimSpace(req.GetNewName())
	if newName == "" {
		return nil, status.Error(codes.InvalidArgument, "поле new_name обязательно")
	}
	var out []string
	if newName != req.GetName() {
		q, err := QuoteIdent(newName, "new_name")
		if err != nil {
			return nil, err
		}
		out = append(out, "ALTER TABLE "+rel+" RENAME TO "+q)
		rel, err = Qualified(req.GetSchema(), newName, "schema", "new_name")
		if err != nil {
			return nil, err
		}
	}
	if ns := strings.TrimSpace(req.GetNewSchema()); ns != "" && ns != req.GetSchema() {
		q, err := QuoteIdent(ns, "new_schema")
		if err != nil {
			return nil, err
		}
		out = append(out, "ALTER TABLE "+rel+" SET SCHEMA "+q)
	}
	if len(out) == 0 {
		return nil, status.Error(codes.InvalidArgument, "нечего переименовывать")
	}
	return out, nil
}

func VacuumTableSQL(req *pgapi.VacuumTableRequest) (string, error) {
	if req == nil {
		return "", status.Error(codes.InvalidArgument, "тело запроса обязательно")
	}
	if _, err := Ident(req.GetDatabase(), "database"); err != nil {
		return "", err
	}
	rel, err := Qualified(req.GetSchema(), req.GetName(), "schema", "name")
	if err != nil {
		return "", err
	}
	var b strings.Builder
	b.WriteString("VACUUM")
	if req.GetFull() {
		b.WriteString(" FULL")
	}
	if req.GetFreeze() {
		b.WriteString(" FREEZE")
	}
	if req.GetAnalyze() {
		b.WriteString(" ANALYZE")
	}
	b.WriteByte(' ')
	b.WriteString(rel)
	return b.String(), nil
}

func AnalyzeTableSQL(req *pgapi.AnalyzeTableRequest) (string, error) {
	if req == nil {
		return "", status.Error(codes.InvalidArgument, "тело запроса обязательно")
	}
	if _, err := Ident(req.GetDatabase(), "database"); err != nil {
		return "", err
	}
	rel, err := Qualified(req.GetSchema(), req.GetName(), "schema", "name")
	if err != nil {
		return "", err
	}
	return "ANALYZE " + rel, nil
}

func AddColumnStatements(req *pgapi.AddColumnRequest) ([]string, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "тело запроса обязательно")
	}
	if _, err := Ident(req.GetDatabase(), "database"); err != nil {
		return nil, err
	}
	rel, err := Qualified(req.GetSchema(), req.GetTable(), "schema", "table")
	if err != nil {
		return nil, err
	}
	col, err := columnDDL(req.GetColumn())
	if err != nil {
		return nil, err
	}
	var b strings.Builder
	b.WriteString("ALTER TABLE ")
	b.WriteString(rel)
	b.WriteString(" ADD COLUMN")
	if req.GetIfNotExists() {
		b.WriteString(" IF NOT EXISTS")
	}
	b.WriteByte(' ')
	b.WriteString(col)
	out := []string{b.String()}
	if req.GetColumn() != nil && req.GetColumn().GetComment() != "" {
		cn, err := QuoteIdent(req.GetColumn().GetName(), "column.name")
		if err != nil {
			return nil, err
		}
		sql, err := commentOn("COLUMN", rel+"."+cn, req.GetColumn().GetComment())
		if err != nil {
			return nil, err
		}
		out = append(out, sql)
	}
	return out, nil
}

func DropColumnSQL(req *pgapi.DropColumnRequest) (string, error) {
	if req == nil {
		return "", status.Error(codes.InvalidArgument, "тело запроса обязательно")
	}
	if _, err := Ident(req.GetDatabase(), "database"); err != nil {
		return "", err
	}
	rel, err := Qualified(req.GetSchema(), req.GetTable(), "schema", "table")
	if err != nil {
		return "", err
	}
	col, err := QuoteIdent(req.GetName(), "name")
	if err != nil {
		return "", err
	}
	var b strings.Builder
	b.WriteString("ALTER TABLE ")
	b.WriteString(rel)
	b.WriteString(" DROP COLUMN")
	if req.GetIfExists() {
		b.WriteString(" IF EXISTS")
	}
	b.WriteByte(' ')
	b.WriteString(col)
	if req.GetCascade() {
		b.WriteString(" CASCADE")
	}
	return b.String(), nil
}

func RenameColumnSQL(req *pgapi.RenameColumnRequest) (string, error) {
	if req == nil {
		return "", status.Error(codes.InvalidArgument, "тело запроса обязательно")
	}
	if _, err := Ident(req.GetDatabase(), "database"); err != nil {
		return "", err
	}
	rel, err := Qualified(req.GetSchema(), req.GetTable(), "schema", "table")
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
	return "ALTER TABLE " + rel + " RENAME COLUMN " + from + " TO " + to, nil
}

func ModifyColumnStatements(req *pgapi.ModifyColumnRequest) ([]string, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "тело запроса обязательно")
	}
	col := req.GetColumn()
	if col == nil {
		return nil, status.Error(codes.InvalidArgument, "колонка обязательна")
	}
	if _, err := Ident(req.GetDatabase(), "database"); err != nil {
		return nil, err
	}
	rel, err := Qualified(req.GetSchema(), req.GetTable(), "schema", "table")
	if err != nil {
		return nil, err
	}
	cn, err := QuoteIdent(col.GetName(), "column.name")
	if err != nil {
		return nil, err
	}
	typ, err := requireExpr(col.GetType(), "column.type")
	if err != nil {
		return nil, err
	}
	typeSQL := "ALTER COLUMN " + cn + " TYPE " + typ
	if v := strings.TrimSpace(col.GetCollation()); v != "" {
		q, err := QuoteCollation(v, "column.collation")
		if err != nil {
			return nil, err
		}
		typeSQL += " COLLATE " + q
	}
	nullSQL := "ALTER COLUMN " + cn + " SET NOT NULL"
	if col.GetNullable() {
		nullSQL = "ALTER COLUMN " + cn + " DROP NOT NULL"
	}
	actions := []string{typeSQL, nullSQL}
	if def := strings.TrimSpace(col.GetDefaultExpression()); def != "" {
		e, err := requireExpr(def, "column.default_expression")
		if err != nil {
			return nil, err
		}
		actions = append(actions, "ALTER COLUMN "+cn+" SET DEFAULT "+e)
	}
	out := []string{"ALTER TABLE " + rel + " " + strings.Join(actions, ", ")}
	if c := col.GetComment(); c != "" {
		sql, err := commentOn("COLUMN", rel+"."+cn, c)
		if err != nil {
			return nil, err
		}
		out = append(out, sql)
	}
	return out, nil
}

func CreateIndexSQL(spec *pgapi.IndexSpec) (string, error) {
	if spec == nil {
		return "", status.Error(codes.InvalidArgument, "тело запроса обязательно")
	}
	if _, err := Ident(spec.GetDatabase(), "database"); err != nil {
		return "", err
	}
	if len(spec.GetColumns()) == 0 {
		return "", status.Error(codes.InvalidArgument, "нужна хотя бы одна колонка индекса")
	}
	name, err := QuoteIdent(spec.GetName(), "name")
	if err != nil {
		return "", err
	}
	rel, err := Qualified(spec.GetSchema(), spec.GetTable(), "schema", "table")
	if err != nil {
		return "", err
	}
	cols, err := quoteIdentList(spec.GetColumns(), "columns")
	if err != nil {
		return "", err
	}
	var b strings.Builder
	b.WriteString("CREATE")
	if spec.GetUnique() {
		b.WriteString(" UNIQUE")
	}
	b.WriteString(" INDEX")
	if spec.GetConcurrently() {
		b.WriteString(" CONCURRENTLY")
	}
	if spec.GetIfNotExists() {
		b.WriteString(" IF NOT EXISTS")
	}
	b.WriteByte(' ')
	b.WriteString(name)
	b.WriteString(" ON ")
	b.WriteString(rel)
	if m := strings.TrimSpace(spec.GetMethod()); m != "" {
		q, err := Ident(m, "method")
		if err != nil {
			return "", err
		}
		b.WriteString(" USING ")
		b.WriteString(q)
	}
	b.WriteString(" (")
	b.WriteString(cols)
	b.WriteByte(')')
	if inc, err := splitIdents(spec.GetInclude(), "include"); err != nil {
		return "", err
	} else if len(inc) > 0 {
		b.WriteString(" INCLUDE (")
		b.WriteString(strings.Join(inc, ", "))
		b.WriteByte(')')
	}
	if v := strings.TrimSpace(spec.GetTablespace()); v != "" {
		q, err := QuoteIdent(v, "tablespace")
		if err != nil {
			return "", err
		}
		b.WriteString(" TABLESPACE ")
		b.WriteString(q)
	}
	if w := strings.TrimSpace(spec.GetWhere()); w != "" {
		e, err := requireExpr(w, "where")
		if err != nil {
			return "", err
		}
		b.WriteString(" WHERE ")
		b.WriteString(e)
	}
	return b.String(), nil
}

func DropIndexSQL(req *pgapi.IndexName) (string, error) {
	if req == nil {
		return "", status.Error(codes.InvalidArgument, "тело запроса обязательно")
	}
	if _, err := Ident(req.GetDatabase(), "database"); err != nil {
		return "", err
	}
	rel, err := Qualified(req.GetSchema(), req.GetName(), "schema", "name")
	if err != nil {
		return "", err
	}
	if req.GetConcurrently() && req.GetCascade() {
		return "", status.Error(codes.InvalidArgument, "CONCURRENTLY несовместим с CASCADE")
	}
	var b strings.Builder
	b.WriteString("DROP INDEX")
	if req.GetConcurrently() {
		b.WriteString(" CONCURRENTLY")
	}
	if req.GetIfExists() {
		b.WriteString(" IF EXISTS")
	}
	b.WriteByte(' ')
	b.WriteString(rel)
	if req.GetCascade() {
		b.WriteString(" CASCADE")
	}
	return b.String(), nil
}

func ReindexSQL(req *pgapi.ReindexRequest) (string, error) {
	if req == nil {
		return "", status.Error(codes.InvalidArgument, "тело запроса обязательно")
	}
	if _, err := Ident(req.GetDatabase(), "database"); err != nil {
		return "", err
	}
	kind := "TABLE"
	var target string
	var err error
	if name := strings.TrimSpace(req.GetName()); name != "" {
		kind = "INDEX"
		target, err = Qualified(req.GetSchema(), name, "schema", "name")
	} else {
		target, err = Qualified(req.GetSchema(), req.GetTable(), "schema", "table")
	}
	if err != nil {
		return "", err
	}
	sql := "REINDEX " + kind
	if req.GetConcurrently() {
		sql += " CONCURRENTLY"
	}
	return sql + " " + target, nil
}

func PreviewTableDataSQL(req *pgapi.PreviewTableDataRequest) (string, error) {
	if req == nil {
		return "", status.Error(codes.InvalidArgument, "тело запроса обязательно")
	}
	if _, err := Ident(req.GetDatabase(), "database"); err != nil {
		return "", err
	}
	rel, err := Qualified(req.GetSchema(), req.GetTable(), "schema", "table")
	if err != nil {
		return "", err
	}
	var b strings.Builder
	b.WriteString("SELECT * FROM ")
	b.WriteString(rel)
	if w := strings.TrimSpace(req.GetWhere()); w != "" {
		e, err := requireExpr(w, "where")
		if err != nil {
			return "", err
		}
		b.WriteString(" WHERE ")
		b.WriteString(e)
	}
	if o := strings.TrimSpace(req.GetOrderBy()); o != "" {
		e, err := requireExpr(o, "order_by")
		if err != nil {
			return "", err
		}
		b.WriteString(" ORDER BY ")
		b.WriteString(e)
	}
	limit := ClampLimit(int(req.GetLimit()), 50, 500)
	offset := int(req.GetOffset())
	if offset < 0 {
		offset = 0
	}
	b.WriteString(fmt.Sprintf(" LIMIT %d OFFSET %d", limit, offset))
	return b.String(), nil
}

func DropPartitionSQL(req *pgapi.DropPartitionRequest) (string, error) {
	if req == nil {
		return "", status.Error(codes.InvalidArgument, "тело запроса обязательно")
	}
	if _, err := Ident(req.GetDatabase(), "database"); err != nil {
		return "", err
	}
	parent, err := Qualified(req.GetSchema(), req.GetTable(), "schema", "table")
	if err != nil {
		return "", err
	}
	child, err := Qualified(req.GetSchema(), req.GetName(), "schema", "name")
	if err != nil {
		return "", err
	}
	if req.GetDetach() {
		sql := "ALTER TABLE " + parent + " DETACH PARTITION " + child
		if req.GetConcurrently() {
			sql += " CONCURRENTLY"
		}
		return sql, nil
	}
	sql := "DROP TABLE " + child
	if req.GetCascade() {
		sql += " CASCADE"
	}
	return sql, nil
}
