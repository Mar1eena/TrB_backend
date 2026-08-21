package server

import (
	"regexp"
	"strings"
	"unicode"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const maxIdentLen = 128

var (
	identRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	exprRe  = regexp.MustCompile(`^[A-Za-z0-9_(),.\s+\-*/='%"<>!:]+$`)
	numRe   = regexp.MustCompile(`^-?\d+(\.\d+)?$`)
)

func ident(name, field string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", status.Errorf(codes.InvalidArgument, "поле %s обязательно", field)
	}
	if len(name) > maxIdentLen {
		return "", status.Errorf(codes.InvalidArgument, "поле %s слишком длинное", field)
	}
	if !identRe.MatchString(name) {
		return "", status.Errorf(codes.InvalidArgument, "недопустимое имя в %s: %q", field, name)
	}
	return name, nil
}

func QuoteIdent(name, field string) (string, error) {
	id, err := ident(name, field)
	if err != nil {
		return "", err
	}
	return "`" + id + "`", nil
}

func qualified(database, table, dbField, tableField string) (string, error) {
	db, err := QuoteIdent(database, dbField)
	if err != nil {
		return "", err
	}
	tb, err := QuoteIdent(table, tableField)
	if err != nil {
		return "", err
	}
	return db + "." + tb, nil
}

func clusterClause(cluster string) (string, error) {
	cluster = strings.TrimSpace(cluster)
	if cluster == "" {
		return "", nil
	}
	q, err := QuoteIdent(cluster, "cluster")
	if err != nil {
		return "", err
	}
	return " ON CLUSTER " + q, nil
}

func Expr(value, field string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", nil
	}
	if strings.Contains(value, ";") || strings.Contains(value, "--") || strings.Contains(value, "/*") || strings.Contains(value, "*/") {
		return "", status.Errorf(codes.InvalidArgument, "недопустимое выражение в %s", field)
	}
	if !exprRe.MatchString(value) {
		return "", status.Errorf(codes.InvalidArgument, "недопустимое выражение в %s", field)
	}
	return value, nil
}

func requireExpr(value, field string) (string, error) {
	out, err := Expr(value, field)
	if err != nil {
		return "", err
	}
	if out == "" {
		return "", status.Errorf(codes.InvalidArgument, "поле %s обязательно", field)
	}
	return out, nil
}

func quote(s string) string {
	res, err := quoteString(s)
	if err != nil {
		return "'" + strings.ReplaceAll(strings.ReplaceAll(s, "\\", "\\\\"), "'", "\\'") + "'"
	}
	return res
}

func quoteString(s string) (string, error) {
	for _, r := range s {
		if r == 0 || !unicode.IsPrint(r) && !unicode.IsSpace(r) {
			return "", status.Error(codes.InvalidArgument, "недопустимый символ в строке")
		}
	}
	var b strings.Builder
	b.Grow(len(s) + 2)
	b.WriteByte('\'')
	for _, r := range s {
		switch r {
		case '\'':
			b.WriteString("\\'")
		case '\\':
			b.WriteString("\\\\")
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('\'')
	return b.String(), nil
}

func settingValue(v string) (string, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return "", status.Error(codes.InvalidArgument, "пустое значение settings")
	}
	if numRe.MatchString(v) || v == "true" || v == "false" {
		return v, nil
	}
	return quoteString(v)
}

func isSystemDatabase(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "system", "information_schema":
		return true
	default:
		return false
	}
}
