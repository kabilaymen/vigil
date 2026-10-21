package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/kabilaymen/vigil/processor/window"
)

const schema = `
CREATE TABLE IF NOT EXISTS machine_metrics_10s (
	bucket         timestamptz      NOT NULL,
	machine        text             NOT NULL,
	msgs           integer          NOT NULL,
	temp_avg       double precision NOT NULL,
	temp_min       double precision NOT NULL,
	temp_max       double precision NOT NULL,
	throughput_avg double precision NOT NULL,
	PRIMARY KEY (machine, bucket)
);
SELECT create_hypertable('machine_metrics_10s', 'bucket', if_not_exists => TRUE);
SELECT add_retention_policy('machine_metrics_10s', INTERVAL '3 days', if_not_exists => TRUE);

-- Where each partition must be consumed from, written in the same transaction
-- as the aggregates: Kafka offsets are never committed to Kafka itself.
CREATE TABLE IF NOT EXISTS processor_progress (
	topic       text        NOT NULL,
	part        integer     NOT NULL,
	next_offset bigint      NOT NULL,
	last_bucket timestamptz NOT NULL,
	PRIMARY KEY (topic, part)
);
`

type progress struct {
	nextOffset int64
	lastBucket time.Time
}

type store struct {
	pool  *pgxpool.Pool
	topic string
}

func (s *store) migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, schema)
	return err
}

func (s *store) progress(ctx context.Context, parts []int32) (map[int32]progress, error) {
	rows, err := s.pool.Query(ctx, `SELECT part, next_offset, last_bucket FROM processor_progress WHERE topic = $1 AND part = ANY($2)`, s.topic, parts)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int32]progress{}
	for rows.Next() {
		var p int32
		var pr progress
		if err := rows.Scan(&p, &pr.nextOffset, &pr.lastBucket); err != nil {
			return nil, err
		}
		out[p] = pr
	}
	return out, rows.Err()
}

// write stores closed buckets and the offsets to resume from, atomically: after
// a crash the database holds either all of it or none of it, and the stored
// offsets always match the stored rows.
func (s *store) write(ctx context.Context, closed []window.Closed, resume map[int32]int64) (rows int, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	b := &pgx.Batch{}
	for _, c := range closed {
		for _, r := range c.Rows {
			// Upsert: a bucket rebuilt after a crash replaces itself instead of
			// failing on the primary key.
			b.Queue(`INSERT INTO machine_metrics_10s (bucket, machine, msgs, temp_avg, temp_min, temp_max, throughput_avg)
				VALUES ($1, $2, $3, $4, $5, $6, $7)
				ON CONFLICT (machine, bucket) DO UPDATE SET msgs = EXCLUDED.msgs, temp_avg = EXCLUDED.temp_avg,
					temp_min = EXCLUDED.temp_min, temp_max = EXCLUDED.temp_max, throughput_avg = EXCLUDED.throughput_avg`,
				r.Bucket, r.Machine, r.Count, r.TempAvg, r.TempMin, r.TempMax, r.ThroughputAvg)
			rows++
		}
	}
	last := closed[len(closed)-1].Bucket
	for p, off := range resume {
		b.Queue(`INSERT INTO processor_progress (topic, part, next_offset, last_bucket) VALUES ($1, $2, $3, $4)
			ON CONFLICT (topic, part) DO UPDATE SET next_offset = EXCLUDED.next_offset, last_bucket = EXCLUDED.last_bucket`,
			s.topic, p, off, last)
	}
	if err := tx.SendBatch(ctx, b).Close(); err != nil {
		return 0, fmt.Errorf("write %d buckets: %w", len(closed), err)
	}
	return rows, tx.Commit(ctx)
}
