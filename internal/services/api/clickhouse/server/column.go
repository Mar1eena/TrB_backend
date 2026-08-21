package server

import (
	"context"

	chmgr "github.com/Mar1eena/trb_proto/gen/go/api/clickhouse"
)

func (s *Server) AddColumn(ctx context.Context, req *chmgr.AddColumnRequest) (*chmgr.Status, error) {
	sql, err := AddColumnSQL(req)
	if err != nil {
		return nil, err
	}
	out, err := s.exec(ctx, sql)
	if err != nil {
		s.log.Error().Err(err).Str("table", req.GetTable()).Msg("не удалось добавить колонку")
		return nil, err
	}
	s.log.Info().Str("table", req.GetTable()).Str("column", req.GetColumn().GetName()).Msg("колонка добавлена")
	return out, nil
}

func (s *Server) DropColumn(ctx context.Context, req *chmgr.DropColumnRequest) (*chmgr.Status, error) {
	sql, err := DropColumnSQL(req)
	if err != nil {
		return nil, err
	}
	out, err := s.exec(ctx, sql)
	if err != nil {
		s.log.Error().Err(err).Str("table", req.GetTable()).Str("name", req.GetName()).Msg("не удалось удалить колонку")
		return nil, err
	}
	s.log.Info().Str("table", req.GetTable()).Str("name", req.GetName()).Msg("колонка удалена")
	return out, nil
}

func (s *Server) RenameColumn(ctx context.Context, req *chmgr.RenameColumnRequest) (*chmgr.Status, error) {
	sql, err := renameColumnSQL(req)
	if err != nil {
		return nil, err
	}
	out, err := s.exec(ctx, sql)
	if err != nil {
		s.log.Error().Err(err).Str("table", req.GetTable()).Str("name", req.GetName()).Msg("не удалось переименовать колонку")
		return nil, err
	}
	s.log.Info().Str("table", req.GetTable()).Str("name", req.GetName()).Str("new_name", req.GetNewName()).Msg("колонка переименована")
	return out, nil
}

func (s *Server) ModifyColumn(ctx context.Context, req *chmgr.ModifyColumnRequest) (*chmgr.Status, error) {
	sql, err := modifyColumnSQL(req)
	if err != nil {
		return nil, err
	}
	out, err := s.exec(ctx, sql)
	if err != nil {
		s.log.Error().Err(err).Str("table", req.GetTable()).Msg("не удалось изменить колонку")
		return nil, err
	}
	s.log.Info().Str("table", req.GetTable()).Str("column", req.GetColumn().GetName()).Msg("колонка изменена")
	return out, nil
}
