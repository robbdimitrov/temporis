package storage

import (
	"database/sql"
	"log"
	"time"
	"temporis/internal/model"

	_ "github.com/lib/pq"
)

type PostgresStore struct {
	db *sql.DB
}

func NewPostgresStore(url string) (*PostgresStore, error) {
	db, err := sql.Open("postgres", url)
	if err != nil {
		return nil, err
	}
	return &PostgresStore{db}, nil
}

func (s *PostgresStore) Close() error {
	return s.db.Close()
}

func (s *PostgresStore) GetPartitions() ([]*model.Partition, error) {
	rows, err := s.db.Query(`
		SELECT p.id, t.id, t.partition_id, t.interval_ms, t.once
		FROM partitions p
		LEFT JOIN timers t ON p.id = t.partition_id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	partitions := make(map[string]*model.Partition)
	for rows.Next() {
		var pID, tID, tPartitionID sql.NullString
		var intervalMs int64
		var once bool
		if err := rows.Scan(&pID, &tID, &tPartitionID, &intervalMs, &once); err != nil {
			return nil, err
		}
		if !pID.Valid {
			continue
		}
		p, exists := partitions[pID.String]
		if !exists {
			p = &model.Partition{ID: pID.String}
			partitions[pID.String] = p
		}
		if tID.Valid {
			timer := &model.Timer{
				ID:        tID.String,
				Partition: tPartitionID.String,
				Interval:  time.Duration(intervalMs) * time.Millisecond,
				Once:      once,
				Callback: func() {
					log.Printf("Timer %s fired at %v", tID.String, time.Now())
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
