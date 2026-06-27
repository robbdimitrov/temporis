package storage

import (
	"context"
	"log/slog"
	"time"

	"temporis/internal/model"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgresStore(url string) (*PostgresStore, error) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, err
	}
	return &PostgresStore{pool: pool}, nil
}

func (s *PostgresStore) Close() error {
	s.pool.Close()
	return nil
}

func (s *PostgresStore) GetPartitions(ctx context.Context) ([]*model.Partition, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	rows, err := s.pool.Query(ctx, `
		SELECT p.id, t.id::text, t.partition_id, t.interval_ms, t.once
		FROM partitions p
		LEFT JOIN timers t ON p.id = t.partition_id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	partitions := make(map[string]*model.Partition)
	for rows.Next() {
		var pID, tID, tPartitionID *string
		var intervalMs *int64
		var once *bool
		if err := rows.Scan(&pID, &tID, &tPartitionID, &intervalMs, &once); err != nil {
			return nil, err
		}
		if pID == nil {
			continue
		}
		p, exists := partitions[*pID]
		if !exists {
			p = &model.Partition{ID: *pID}
			partitions[*pID] = p
		}
		if tID != nil {
			timerID := *tID // capture for closure
			timer := &model.Timer{
				ID:        timerID,
				Partition: *tPartitionID,
				Interval:  time.Duration(*intervalMs) * time.Millisecond,
				Once:      *once,
				Callback: func() {
					slog.Info("Timer fired", "timer_id", timerID)
				},
			}
			p.Timers = append(p.Timers, timer)
		}
	}
	result := make([]*model.Partition, 0, len(partitions))
	for _, p := range partitions {
		result = append(result, p)
	}
	slog.Info("Loaded partitions", "partition_count", len(result), "timer_count", countTimers(result))
	return result, nil
}

func countTimers(partitions []*model.Partition) int {
	total := 0
	for _, p := range partitions {
		total += len(p.Timers)
	}
	return total
}

// ScheduleOnce atomically records the first pick-up time for a once-timer and
// returns it. Subsequent calls return the already-set value unchanged, so the
// target fire time is stable across partition rebalances.
func (s *PostgresStore) ScheduleOnce(ctx context.Context, timerID string) (time.Time, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var scheduledAt time.Time
	err := s.pool.QueryRow(ctx, `
		UPDATE timers
		SET scheduled_at = COALESCE(scheduled_at, NOW())
		WHERE id::text = $1
		RETURNING scheduled_at
	`, timerID).Scan(&scheduledAt)
	return scheduledAt, err
}

func (s *PostgresStore) ListenForChanges(ctx context.Context, onNotify func()) {
	for {
		if ctx.Err() != nil {
			return
		}

		conn, err := s.pool.Acquire(ctx)
		if err != nil {
			slog.Error("Failed to acquire connection for listen", "error", err)
			time.Sleep(2 * time.Second)
			continue
		}

		_, err = conn.Exec(ctx, "LISTEN config_changed")
		if err != nil {
			slog.Error("Failed to execute LISTEN", "error", err)
			conn.Release()
			time.Sleep(2 * time.Second)
			continue
		}

		slog.Info("Listening for config_changed notifications...")
		for {
			_, err := conn.Conn().WaitForNotification(ctx)
			if err != nil {
				if ctx.Err() != nil {
					conn.Release()
					return
				}
				slog.Error("Error waiting for notification", "error", err)
				break
			}
			slog.Info("Received NOTIFY: config_changed")
			onNotify()
		}
		conn.Release()
		time.Sleep(1 * time.Second)
	}
}
