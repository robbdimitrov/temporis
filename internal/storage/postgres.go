package storage

import (
	"context"
	"log"
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
	// Verify connection
	if err := pool.Ping(ctx); err != nil {
		return nil, err
	}
	return &PostgresStore{pool: pool}, nil
}

func (s *PostgresStore) Close() error {
	s.pool.Close()
	return nil
}

func (s *PostgresStore) GetPartitions() ([]*model.Partition, error) {
	ctx := context.Background()
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
			// Capture the id for the callback closure
			timerID := *tID
			timer := &model.Timer{
				ID:        timerID,
				Partition: *tPartitionID,
				Interval:  time.Duration(*intervalMs) * time.Millisecond,
				Once:      *once,
				Callback: func() {
					log.Printf("Timer %s fired at %v", timerID, time.Now())
				},
			}
			p.Timers = append(p.Timers, timer)
		}
	}
	result := make([]*model.Partition, 0, len(partitions))
	for _, p := range partitions {
		result = append(result, p)
	}
	log.Printf("Loaded %d partitions with total %d timers", len(result), countTimers(result))
	return result, nil
}

func countTimers(partitions []*model.Partition) int {
	total := 0
	for _, p := range partitions {
		total += len(p.Timers)
	}
	return total
}

func (s *PostgresStore) ListenForChanges(ctx context.Context, onNotify func()) {
	for {
		if ctx.Err() != nil {
			return
		}

		conn, err := s.pool.Acquire(ctx)
		if err != nil {
			log.Printf("Failed to acquire connection for listen: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		_, err = conn.Exec(ctx, "LISTEN timers_changed")
		if err != nil {
			log.Printf("Failed to execute LISTEN: %v", err)
			conn.Release()
			time.Sleep(2 * time.Second)
			continue
		}

		log.Println("Listening for timers_changed notifications...")
		for {
			_, err := conn.Conn().WaitForNotification(ctx)
			if err != nil {
				if ctx.Err() != nil {
					conn.Release()
					return
				}
				log.Printf("Error waiting for notification: %v", err)
				break // Break inner loop to reconnect
			}
			log.Println("Received NOTIFY: timers_changed")
			onNotify()
		}
		conn.Release()
		time.Sleep(1 * time.Second) // Backoff before reconnect
	}
}
