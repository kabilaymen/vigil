package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/kabilaymen/vigil/processor/window"
)

// TestStoreAgainstTimescaleDB needs a database: TEST_DATABASE_URL, e.g.
// postgres://vigil:vigil@localhost:5432/vigil (docker compose up timescaledb).
func TestStoreAgainstTimescaleDB(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	st := &store{pool: pool, topic: "test-" + time.Now().Format("150405.000")}
	for i := 0; i < 2; i++ { // the schema must be idempotent: every start runs it
		if err := st.migrate(ctx); err != nil {
			t.Fatalf("migrate #%d: %v", i+1, err)
		}
	}
	b := time.Now().Truncate(10 * time.Second).Add(-time.Hour)
	machine := st.topic + "-m1"
	write := func(count int, resume map[int32]int64) {
		t.Helper()
		closed := []window.Closed{{Bucket: b, Rows: []window.Row{{Machine: machine, Bucket: b, Count: count, TempAvg: 50, TempMin: 40, TempMax: 60, ThroughputAvg: 100}}}}
		if _, err := st.write(ctx, closed, resume); err != nil {
			t.Fatal(err)
		}
	}
	write(3, map[int32]int64{0: 10, 1: 7})
	write(5, map[int32]int64{0: 12}) // the same bucket rebuilt after a crash replaces itself

	var msgs int
	if err := pool.QueryRow(ctx, `SELECT msgs FROM machine_metrics_10s WHERE machine = $1 AND bucket = $2`, machine, b).Scan(&msgs); err != nil || msgs != 5 {
		t.Fatalf("row after upsert: msgs=%d err=%v", msgs, err)
	}
	prog, err := st.progress(ctx, []int32{0, 1, 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(prog) != 2 || prog[0].nextOffset != 12 || prog[1].nextOffset != 7 || !prog[0].lastBucket.Equal(b) {
		t.Fatalf("progress = %+v", prog)
	}
	pool.Exec(ctx, `DELETE FROM machine_metrics_10s WHERE machine = $1`, machine)
	pool.Exec(ctx, `DELETE FROM processor_progress WHERE topic = $1`, st.topic)
}
