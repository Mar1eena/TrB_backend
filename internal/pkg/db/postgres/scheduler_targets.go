package postgres

import (
	"context"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type SchedulerTarget struct {
	UID       string    `json:"uid"`
	Interval  int32     `json:"interval"`
	Enabled   bool      `json:"enabled"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Ticker    string    `json:"ticker,omitempty"`
	Name      string    `json:"name,omitempty"`
	Figi      string    `json:"figi,omitempty"`
}

func ListEnabledTargets(ctx context.Context, pool *pgxpool.Pool) ([]SchedulerTarget, error) {
	rows, err := pool.Query(ctx, `
		SELECT uid, interval, enabled, created_at, updated_at
		FROM hct_scheduler_target
		WHERE enabled = true
		ORDER BY uid, interval`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTargets(rows)
}

func ListTargets(ctx context.Context, pool *pgxpool.Pool) ([]SchedulerTarget, error) {
	rows, err := pool.Query(ctx, `
		SELECT uid, interval, enabled, created_at, updated_at
		FROM hct_scheduler_target
		ORDER BY enabled DESC, uid, interval`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTargets(rows)
}

type SyncInstrument struct {
	UID       string
	Intervals []int32 // enabled intervals only; empty => membership placeholder
}

// SyncTargets replaces the whole scheduler target set inside one transaction.
func SyncTargets(ctx context.Context, pool *pgxpool.Pool, instruments []SyncInstrument) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `DELETE FROM hct_scheduler_target`); err != nil {
		return err
	}

	for _, inst := range instruments {
		uid := strings.TrimSpace(inst.UID)
		if uid == "" {
			continue
		}
		if len(inst.Intervals) == 0 {
			if _, err := tx.Exec(ctx, `
				INSERT INTO hct_scheduler_target (uid, interval, enabled)
				VALUES ($1, 1, false)`, uid); err != nil {
				return err
			}
			continue
		}
		seen := map[int32]struct{}{}
		for _, interval := range inst.Intervals {
			if interval <= 0 {
				continue
			}
			if _, ok := seen[interval]; ok {
				continue
			}
			seen[interval] = struct{}{}
			if _, err := tx.Exec(ctx, `
				INSERT INTO hct_scheduler_target (uid, interval, enabled)
				VALUES ($1, $2, true)`, uid, interval); err != nil {
				return err
			}
		}
	}

	return tx.Commit(ctx)
}

func scanTargets(rows pgx.Rows) ([]SchedulerTarget, error) {
	out := make([]SchedulerTarget, 0)
	for rows.Next() {
		var t SchedulerTarget
		if err := rows.Scan(&t.UID, &t.Interval, &t.Enabled, &t.CreatedAt, &t.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}
