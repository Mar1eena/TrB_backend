package admin

import (
	"context"

	pgpkg "github.com/Mar1eena/TrB_V3/internal/services/postgre/pkg"
	pgapi "github.com/Mar1eena/trb_proto/gen/go/postgresql"
)

func (a *Admin) AddColumn(ctx context.Context, req *pgapi.AddColumnRequest) (*pgapi.Status, error) {
	stmts, err := pgpkg.AddColumnStatements(req)
	if err != nil {
		return nil, err
	}
	pool, err := a.poolFor(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	out, err := a.execAll(ctx, pool, stmts)
	if err != nil {
		a.log.Error().Err(err).Str("table", req.GetTable()).Msg("не удалось добавить колонку")
		return nil, err
	}
	a.log.Info().Str("table", req.GetTable()).Str("column", req.GetColumn().GetName()).Msg("колонка добавлена")
	return out, nil
}

func (a *Admin) DropColumn(ctx context.Context, req *pgapi.DropColumnRequest) (*pgapi.Status, error) {
	sql, err := pgpkg.DropColumnSQL(req)
	if err != nil {
		return nil, err
	}
	pool, err := a.poolFor(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	out, err := a.exec(ctx, pool, sql)
	if err != nil {
		a.log.Error().Err(err).Str("table", req.GetTable()).Str("name", req.GetName()).Msg("не удалось удалить колонку")
		return nil, err
	}
	a.log.Info().Str("table", req.GetTable()).Str("name", req.GetName()).Msg("колонка удалена")
	return out, nil
}

func (a *Admin) RenameColumn(ctx context.Context, req *pgapi.RenameColumnRequest) (*pgapi.Status, error) {
	sql, err := pgpkg.RenameColumnSQL(req)
	if err != nil {
		return nil, err
	}
	pool, err := a.poolFor(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	out, err := a.exec(ctx, pool, sql)
	if err != nil {
		a.log.Error().Err(err).Str("table", req.GetTable()).Str("name", req.GetName()).Msg("не удалось переименовать колонку")
		return nil, err
	}
	a.log.Info().Str("table", req.GetTable()).Str("name", req.GetName()).Str("new_name", req.GetNewName()).Msg("колонка переименована")
	return out, nil
}

func (a *Admin) ModifyColumn(ctx context.Context, req *pgapi.ModifyColumnRequest) (*pgapi.Status, error) {
	stmts, err := pgpkg.ModifyColumnStatements(req)
	if err != nil {
		return nil, err
	}
	pool, err := a.poolFor(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	out, err := a.execAll(ctx, pool, stmts)
	if err != nil {
		a.log.Error().Err(err).Str("table", req.GetTable()).Msg("не удалось изменить колонку")
		return nil, err
	}
	a.log.Info().Str("table", req.GetTable()).Str("column", req.GetColumn().GetName()).Msg("колонка изменена")
	return out, nil
}
