package storage

import (
	"database/sql"
	"time"
	"timer-service/internal/model"

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
		var pID, tID, tPartitionID string
		var intervalMs int64
		var once bool
		if err := rows.Scan(&pID, &tID, &tPartitionID, &intervalMs, &once); err != nil {
			return nil, err
		}
		p, exists := partitions[pID]
		if !exists {
			p = &model.Partition{ID: pID}
			partitions[pID] = p
		}
		if tID != "" {
			p.Timers = append(p.Timers, &model.Timer{
				ID:        tID,
				Partition: tPartitionID,
				Interval:  time.Duration(intervalMs) * time.Millisecond,
				Once:      once,
				Callback:  func() { /* Placeholder */ },
			})
		}
	}
	result := make([]*model.Partition, 0, len(partitions))
	for _, p := range partitions {
		result = append(result, p)
	}
	return result, nil
}
