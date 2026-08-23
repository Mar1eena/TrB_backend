package pkg

import (
	"fmt"
	"regexp"
	"strings"
	"unicode"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const maxIdentLen = 128

var (
	identRe     = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	collationRe = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]*$`)
	exprRe      = regexp.MustCompile(`^[A-Za-z0-9_(),.\[\]\s+\-*/='%"<>!:]+$`)
)

func Ident(name, field string) (string, error) {
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
	id, err := Ident(name, field)
	if err != nil {
		return "", err
	}
	return `"` + id + `"`, nil
}

func QuoteCollation(name, field string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", status.Errorf(codes.InvalidArgument, "поле %s обязательно", field)
	}
	if len(name) > maxIdentLen {
		return "", status.Errorf(codes.InvalidArgument, "поле %s слишком длинное", field)
	}
	if !collationRe.MatchString(name) {
		return "", status.Errorf(codes.InvalidArgument, "недопустимое имя в %s: %q", field, name)
	}
	return `"` + name + `"`, nil
}

func Qualified(schema, name, schemaField, nameField string) (string, error) {
	s, err := QuoteIdent(schema, schemaField)
	if err != nil {
		return "", err
	}
	n, err := QuoteIdent(name, nameField)
	if err != nil {
		return "", err
	}
	return s + "." + n, nil
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

func QuoteString(s string) (string, error) {
	for _, r := range s {
		if r == 0 || !unicode.IsPrint(r) && !unicode.IsSpace(r) {
			return "", status.Error(codes.InvalidArgument, "недопустимый символ в строке")
		}
	}
	return "'" + strings.ReplaceAll(s, "'", "''") + "'", nil
}

func quoteIdentList(names []string, field string) (string, error) {
	parts := make([]string, 0, len(names))
	for i, n := range names {
		q, err := QuoteIdent(n, fmt.Sprintf("%s[%d]", field, i))
		if err != nil {
			return "", err
		}
		parts = append(parts, q)
	}
	return strings.Join(parts, ", "), nil
}

func splitIdents(s, field string) ([]string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	chunks := strings.Split(s, ",")
	out := make([]string, 0, len(chunks))
	for i, c := range chunks {
		q, err := QuoteIdent(strings.TrimSpace(c), fmt.Sprintf("%s[%d]", field, i))
		if err != nil {
			return nil, err
		}
		out = append(out, q)
	}
	return out, nil
}

func isSystemDatabase(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "template0", "template1", "postgres":
		return true
	default:
		return false
	}
}

func isSystemSchema(name string) bool {
	n := strings.ToLower(strings.TrimSpace(name))
	if n == "information_schema" {
		return true
	}
	return strings.HasPrefix(n, "pg_")
}

func ClampLimit(n, def, max int) int {
	if n <= 0 {
		return def
	}
	if n > max {
		return max
	}
	return n
}
