package goflightbench

import (
	"database/sql"
	"flag"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/apache/arrow-go/v18/arrow/flight/flightsql/driver"
)

var (
	flagHost      = flag.String("host", "127.0.0.1", "Flight SQL host")
	flagPort      = flag.Int("port", 0, "Flight SQL port")
	flagRows      = flag.Int("rows", 100000, "Number of rows for benchmark phases")
	flagWorkers   = flag.Int("workers", 200, "Number of concurrent goroutines")
	flagBatchSize = flag.Int("batch-size", 1000, "Batch size for batch insert phase")
	flagCrudIters = flag.Int("crud-iters", 2000, "Iterations for single-operation CRUD latency phase")
)

const (
	benchmarkTable   = "go_flight_bench"
	crudTable        = "go_flight_crud_tmp"
	driverName       = "flightsql"
	driverTimeoutDSN = "120s"
)

type idRange struct {
	start int64
	end   int64
}

func dsn() string {
	return fmt.Sprintf("flightsql://%s:%d?timeout=%s&tls=disabled", *flagHost, *flagPort, driverTimeoutDSN)
}

func printPhaseStart(name string) {
	fmt.Printf("=== PHASE: %s ===\n", name)
}

func printOpMetric(name string, duration time.Duration, ops int64) {
	seconds := duration.Seconds()
	if seconds <= 0 {
		seconds = 1e-9
	}
	avgMicros := float64(duration.Nanoseconds()) / 1000.0 / float64(ops)
	fmt.Printf("%s duration=%s ops=%d ops_per_sec=%.2f avg_us=%.3f\n",
		name, duration, ops, float64(ops)/seconds, avgMicros)
}

func printRowMetric(name string, duration time.Duration, rows int64) {
	seconds := duration.Seconds()
	if seconds <= 0 {
		seconds = 1e-9
	}
	fmt.Printf("%s duration=%s rows=%d rows_per_sec=%.2f\n",
		name, duration, rows, float64(rows)/seconds)
}

func requirePositiveFlag(t *testing.T, value int, name string) {
	t.Helper()
	if value <= 0 {
		t.Fatalf("invalid %s: %d", name, value)
	}
}

func openDB(t *testing.T) *sql.DB {
	t.Helper()
	requirePositiveFlag(t, *flagPort, "port")

	db, err := sql.Open(driverName, dsn())
	if err != nil {
		t.Fatalf("sql.Open failed: %v", err)
	}
	db.SetMaxOpenConns(256)
	db.SetMaxIdleConns(256)
	db.SetConnMaxLifetime(0)

	if err := db.Ping(); err != nil {
		_ = db.Close()
		t.Fatalf("db.Ping failed: %v", err)
	}
	return db
}

func resetBenchmarkTable(db *sql.DB) error {
	if _, err := db.Exec("DROP TABLE IF EXISTS " + benchmarkTable); err != nil {
		return err
	}
	_, err := db.Exec("CREATE TABLE " + benchmarkTable + " (id BIGINT PRIMARY KEY, val BIGINT)")
	return err
}

func partitionRanges(totalRows, workers int) []idRange {
	ranges := make([]idRange, 0, workers)
	if totalRows <= 0 || workers <= 0 {
		return ranges
	}

	base := totalRows / workers
	remainder := totalRows % workers
	cursor := int64(1)
	for worker := 0; worker < workers; worker++ {
		size := base
		if worker < remainder {
			size++
		}
		if size == 0 {
			ranges = append(ranges, idRange{start: 1, end: 0})
			continue
		}
		start := cursor
		end := cursor + int64(size) - 1
		ranges = append(ranges, idRange{start: start, end: end})
		cursor = end + 1
	}
	return ranges
}

func expectedVal(id int64) int64 {
	return id * 10
}

func expectedSum(rows int64) int64 {
	return int64(10) * rows * (rows + 1) / 2
}

func toInt64(value any, fieldName string) (int64, error) {
	switch v := value.(type) {
	case int64:
		return v, nil
	case int32:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int:
		return int64(v), nil
	case uint64:
		if v > math.MaxInt64 {
			return 0, fmt.Errorf("%s out of int64 range: %d", fieldName, v)
		}
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint:
		if v > math.MaxInt64 {
			return 0, fmt.Errorf("%s out of int64 range: %d", fieldName, v)
		}
		return int64(v), nil
	case float64:
		return int64(v), nil
	case float32:
		return int64(v), nil
	case string:
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse %s from string %q: %w", fieldName, v, err)
		}
		return parsed, nil
	case []byte:
		parsed, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse %s from bytes %q: %w", fieldName, string(v), err)
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("unsupported %s type %T", fieldName, value)
	}
}

func buildBatchInsertSQL(startID int64, size int) (string, []any) {
	var builder strings.Builder
	builder.WriteString("INSERT INTO ")
	builder.WriteString(benchmarkTable)
	builder.WriteString(" (id, val) VALUES ")

	args := make([]any, 0, size*2)
	for i := 0; i < size; i++ {
		if i > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString("(?, ?)")
		id := startID + int64(i)
		args = append(args, id, expectedVal(id))
	}
	return builder.String(), args
}

func verifyAggregateTableState(t *testing.T, db *sql.DB, expectedRows int64) {
	t.Helper()

	var gotCountRaw any
	var gotDistinctRaw any
	var gotMinRaw any
	var gotMaxRaw any
	var gotSumRaw any
	err := db.QueryRow(
		"SELECT " +
			"CAST(COUNT(*) AS BIGINT), " +
			"CAST(COUNT(DISTINCT id) AS BIGINT), " +
			"CAST(COALESCE(MIN(id), 0) AS BIGINT), " +
			"CAST(COALESCE(MAX(id), 0) AS BIGINT), " +
			"CAST(COALESCE(SUM(val), 0) AS BIGINT) " +
			"FROM " + benchmarkTable,
	).Scan(&gotCountRaw, &gotDistinctRaw, &gotMinRaw, &gotMaxRaw, &gotSumRaw)
	if err != nil {
		t.Fatalf("aggregate verification query failed: %v", err)
	}
	gotCount, err := toInt64(gotCountRaw, "count")
	if err != nil {
		t.Fatalf("aggregate conversion failed for count: %v", err)
	}
	gotDistinct, err := toInt64(gotDistinctRaw, "count_distinct")
	if err != nil {
		t.Fatalf("aggregate conversion failed for count_distinct: %v", err)
	}
	gotMin, err := toInt64(gotMinRaw, "min")
	if err != nil {
		t.Fatalf("aggregate conversion failed for min: %v", err)
	}
	gotMax, err := toInt64(gotMaxRaw, "max")
	if err != nil {
		t.Fatalf("aggregate conversion failed for max: %v", err)
	}
	gotSum, err := toInt64(gotSumRaw, "sum")
	if err != nil {
		t.Fatalf("aggregate conversion failed for sum: %v", err)
	}
	if gotCount != expectedRows {
		t.Fatalf("row count mismatch: got=%d expected=%d", gotCount, expectedRows)
	}
	if gotDistinct != expectedRows {
		t.Fatalf("distinct id count mismatch: got=%d expected=%d", gotDistinct, expectedRows)
	}
	if expectedRows > 0 {
		if gotMin != 1 || gotMax != expectedRows {
			t.Fatalf("id bounds mismatch: min=%d max=%d expected_min=1 expected_max=%d", gotMin, gotMax, expectedRows)
		}
	}
	expected := expectedSum(expectedRows)
	if gotSum != expected {
		t.Fatalf("sum(val) mismatch: got=%d expected=%d", gotSum, expected)
	}
}

func runCrudSingleOpAutocommit(t *testing.T, db *sql.DB, iters int) {
	t.Helper()
	requirePositiveFlag(t, iters, "crud-iters")

	printPhaseStart("CRUD_SINGLE_OP_AUTOCOMMIT")
	if _, err := db.Exec("DROP TABLE IF EXISTS " + crudTable); err != nil {
		t.Fatalf("cleanup drop failed: %v", err)
	}

	createStart := time.Now()
	if _, err := db.Exec("CREATE TABLE " + crudTable + " (id BIGINT PRIMARY KEY, val BIGINT)"); err != nil {
		t.Fatalf("create table failed: %v", err)
	}
	createDuration := time.Since(createStart)
	printOpMetric("crud_create_table", createDuration, 1)

	var insertTotal time.Duration
	var updateTotal time.Duration
	var selectTotal time.Duration
	var deleteTotal time.Duration

	for i := 1; i <= iters; i++ {
		id := int64(i)
		baseVal := expectedVal(id)

		start := time.Now()
		if _, err := db.Exec("INSERT INTO "+crudTable+" VALUES (?, ?)", id, baseVal); err != nil {
			t.Fatalf("insert failed at id=%d: %v", id, err)
		}
		insertTotal += time.Since(start)

		start = time.Now()
		res, err := db.Exec("UPDATE "+crudTable+" SET val = val + 1 WHERE id = ?", id)
		if err != nil {
			t.Fatalf("update failed at id=%d: %v", id, err)
		}
		affected, err := res.RowsAffected()
		if err != nil {
			t.Fatalf("RowsAffected failed at update id=%d: %v", id, err)
		}
		if affected != 1 {
			t.Fatalf("update affected rows mismatch for id=%d: got=%d", id, affected)
		}
		updateTotal += time.Since(start)

		start = time.Now()
		var gotVal int64
		if err := db.QueryRow("SELECT val FROM "+crudTable+" WHERE id = ?", id).Scan(&gotVal); err != nil {
			t.Fatalf("select failed at id=%d: %v", id, err)
		}
		expected := baseVal + 1
		if gotVal != expected {
			t.Fatalf("selected value mismatch at id=%d: got=%d expected=%d", id, gotVal, expected)
		}
		selectTotal += time.Since(start)

		start = time.Now()
		res, err = db.Exec("DELETE FROM "+crudTable+" WHERE id = ?", id)
		if err != nil {
			t.Fatalf("delete failed at id=%d: %v", id, err)
		}
		affected, err = res.RowsAffected()
		if err != nil {
			t.Fatalf("RowsAffected failed at delete id=%d: %v", id, err)
		}
		if affected != 1 {
			t.Fatalf("delete affected rows mismatch for id=%d: got=%d", id, affected)
		}
		deleteTotal += time.Since(start)
	}

	dropStart := time.Now()
	if _, err := db.Exec("DROP TABLE " + crudTable); err != nil {
		t.Fatalf("drop table failed: %v", err)
	}
	dropDuration := time.Since(dropStart)
	printOpMetric("crud_insert_autocommit", insertTotal, int64(iters))
	printOpMetric("crud_update_autocommit", updateTotal, int64(iters))
	printOpMetric("crud_select_point", selectTotal, int64(iters))
	printOpMetric("crud_delete_autocommit", deleteTotal, int64(iters))
	printOpMetric("crud_drop_table", dropDuration, 1)
}

func runBatchInsert(t *testing.T, db *sql.DB, rows, batchSize int) {
	t.Helper()
	requirePositiveFlag(t, rows, "rows")
	requirePositiveFlag(t, batchSize, "batch-size")

	printPhaseStart("BATCH_INSERT_100K")
	if err := resetBenchmarkTable(db); err != nil {
		t.Fatalf("reset benchmark table failed: %v", err)
	}

	phaseStart := time.Now()
	batchCount := 0
	for start := 1; start <= rows; start += batchSize {
		size := batchSize
		remaining := rows - start + 1
		if remaining < size {
			size = remaining
		}
		sqlText, args := buildBatchInsertSQL(int64(start), size)
		if _, err := db.Exec(sqlText, args...); err != nil {
			t.Fatalf("batch insert failed at start=%d size=%d: %v", start, size, err)
		}
		batchCount++
	}
	duration := time.Since(phaseStart)
	verifyAggregateTableState(t, db, int64(rows))
	printRowMetric("batch_insert_rows", duration, int64(rows))
	printOpMetric("batch_insert_statements", duration, int64(batchCount))
}

func runConcurrentInsert(t *testing.T, db *sql.DB, rows, workers int) {
	t.Helper()
	requirePositiveFlag(t, rows, "rows")
	requirePositiveFlag(t, workers, "workers")

	printPhaseStart("CONCURRENT_INSERT_200")
	if err := resetBenchmarkTable(db); err != nil {
		t.Fatalf("reset benchmark table failed: %v", err)
	}

	ranges := partitionRanges(rows, workers)
	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	phaseStart := time.Now()

	for workerID, rg := range ranges {
		if rg.end < rg.start {
			continue
		}
		wg.Add(1)
		go func(worker int, r idRange) {
			defer wg.Done()
			for id := r.start; id <= r.end; id++ {
				if _, err := db.Exec("INSERT INTO "+benchmarkTable+" VALUES (?, ?)", id, expectedVal(id)); err != nil {
					errCh <- fmt.Errorf("worker=%d id=%d insert failed: %w", worker, id, err)
					return
				}
			}
		}(workerID, rg)
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent insert failed: %v", err)
		}
	}
	duration := time.Since(phaseStart)
	verifyAggregateTableState(t, db, int64(rows))
	printRowMetric("concurrent_insert_rows", duration, int64(rows))
	printOpMetric("concurrent_insert_autocommit_ops", duration, int64(rows))
}

func runOrderedSingleRead(t *testing.T, db *sql.DB, rows int) {
	t.Helper()
	requirePositiveFlag(t, rows, "rows")

	printPhaseStart("SELECT_ORDERED_SINGLE")
	start := time.Now()
	resultRows, err := db.Query("SELECT id, val FROM " + benchmarkTable + " ORDER BY id")
	if err != nil {
		t.Fatalf("ordered single read query failed: %v", err)
	}
	defer resultRows.Close()

	var count int64
	var expectedID int64 = 1
	for resultRows.Next() {
		var id int64
		var val int64
		if err := resultRows.Scan(&id, &val); err != nil {
			t.Fatalf("ordered single read scan failed: %v", err)
		}
		if id != expectedID {
			t.Fatalf("ordered single id mismatch: got=%d expected=%d", id, expectedID)
		}
		expectedVal := expectedVal(id)
		if val != expectedVal {
			t.Fatalf("ordered single val mismatch for id=%d: got=%d expected=%d", id, val, expectedVal)
		}
		expectedID++
		count++
	}
	if err := resultRows.Err(); err != nil {
		t.Fatalf("ordered single rows iteration failed: %v", err)
	}
	if count != int64(rows) {
		t.Fatalf("ordered single row count mismatch: got=%d expected=%d", count, rows)
	}
	duration := time.Since(start)
	printRowMetric("select_ordered_single_rows", duration, int64(rows))
}

func runOrderedConcurrentRead(t *testing.T, db *sql.DB, rows, workers int) {
	t.Helper()
	requirePositiveFlag(t, rows, "rows")
	requirePositiveFlag(t, workers, "workers")

	printPhaseStart("SELECT_ORDERED_CONCURRENT_200")
	ranges := partitionRanges(rows, workers)
	errCh := make(chan error, workers)
	var verifiedRows atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()

	for workerID, rg := range ranges {
		if rg.end < rg.start {
			continue
		}
		wg.Add(1)
		go func(worker int, r idRange) {
			defer wg.Done()
			queryRows, err := db.Query(
				"SELECT id, val FROM "+benchmarkTable+" WHERE id BETWEEN ? AND ? ORDER BY id",
				r.start, r.end,
			)
			if err != nil {
				errCh <- fmt.Errorf("worker=%d query failed: %w", worker, err)
				return
			}
			defer queryRows.Close()

			expected := r.start
			var localCount int64
			for queryRows.Next() {
				var id int64
				var val int64
				if err := queryRows.Scan(&id, &val); err != nil {
					errCh <- fmt.Errorf("worker=%d scan failed: %w", worker, err)
					return
				}
				if id != expected {
					errCh <- fmt.Errorf("worker=%d id mismatch: got=%d expected=%d", worker, id, expected)
					return
				}
				ev := expectedVal(id)
				if val != ev {
					errCh <- fmt.Errorf("worker=%d val mismatch for id=%d: got=%d expected=%d", worker, id, val, ev)
					return
				}
				expected++
				localCount++
			}
			if err := queryRows.Err(); err != nil {
				errCh <- fmt.Errorf("worker=%d rows iteration failed: %w", worker, err)
				return
			}
			expectedCount := r.end - r.start + 1
			if localCount != expectedCount {
				errCh <- fmt.Errorf("worker=%d shard row count mismatch: got=%d expected=%d", worker, localCount, expectedCount)
				return
			}
			verifiedRows.Add(localCount)
		}(workerID, rg)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent ordered read failed: %v", err)
		}
	}

	total := verifiedRows.Load()
	if total != int64(rows) {
		t.Fatalf("concurrent ordered read total mismatch: got=%d expected=%d", total, rows)
	}
	duration := time.Since(start)
	printRowMetric("select_ordered_concurrent_rows", duration, int64(rows))
}

func TestPing(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	var got int
	if err := db.QueryRow("SELECT 1").Scan(&got); err != nil {
		t.Fatalf("SELECT 1 failed: %v", err)
	}
	if got != 1 {
		t.Fatalf("unexpected ping result: got=%d expected=1", got)
	}
}

func TestFlightSQLBenchmarks(t *testing.T) {
	requirePositiveFlag(t, *flagRows, "rows")
	requirePositiveFlag(t, *flagWorkers, "workers")
	requirePositiveFlag(t, *flagBatchSize, "batch-size")
	requirePositiveFlag(t, *flagCrudIters, "crud-iters")

	db := openDB(t)
	defer db.Close()

	runCrudSingleOpAutocommit(t, db, *flagCrudIters)
	runBatchInsert(t, db, *flagRows, *flagBatchSize)
	runConcurrentInsert(t, db, *flagRows, *flagWorkers)
	runOrderedSingleRead(t, db, *flagRows)
	runOrderedConcurrentRead(t, db, *flagRows, *flagWorkers)
}
