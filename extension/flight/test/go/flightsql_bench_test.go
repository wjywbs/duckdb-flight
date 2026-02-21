package goflightbench

import (
	"context"
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
	flagHost        = flag.String("host", "127.0.0.1", "Flight SQL host")
	flagPort        = flag.Int("port", 0, "Flight SQL port")
	flagRows        = flag.Int("rows", 100000, "Number of rows for benchmark phases")
	flagWorkers     = flag.Int("workers", 200, "Number of concurrent goroutines")
	flagBatchSize   = flag.Int("batch-size", 1000, "Batch size for batch insert phase")
	flagCrudIters   = flag.Int("crud-iters", 2000, "Iterations for single-operation CRUD latency phase")
	flagSelectIters = flag.Int("select-iters", 2000, "Iterations for select prepare-mode benchmark phase")
)

const (
	benchmarkTable           = "go_flight_bench"
	txBenchmarkTable         = "go_flight_tx_bench"
	txConflictBenchmarkTable = "go_flight_tx_conflict_bench"
	crudTable                = "go_flight_crud_tmp"
	driverName               = "flightsql"
	driverTimeoutDSN         = "120s"
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

func resetTransactionBenchmarkTable(db *sql.DB) error {
	if _, err := db.Exec("DROP TABLE IF EXISTS " + txBenchmarkTable); err != nil {
		return err
	}
	_, err := db.Exec(
		"CREATE TABLE " + txBenchmarkTable + " (" +
			"id BIGINT PRIMARY KEY, " +
			"worker_id BIGINT, " +
			"val BIGINT)",
	)
	return err
}

func resetTransactionConflictBenchmarkTable(db *sql.DB) error {
	if _, err := db.Exec("DROP TABLE IF EXISTS " + txConflictBenchmarkTable); err != nil {
		return err
	}
	_, err := db.Exec(
		"CREATE TABLE " + txConflictBenchmarkTable + " (" +
			"id BIGINT PRIMARY KEY, " +
			"worker_id BIGINT, " +
			"attempt BIGINT)",
	)
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
		"SELECT "+
			"CAST(COUNT(*) AS BIGINT), "+
			"CAST(COUNT(DISTINCT id) AS BIGINT), "+
			"CAST(COALESCE(MIN(id), 0) AS BIGINT), "+
			"CAST(COALESCE(MAX(id), 0) AS BIGINT), "+
			"CAST(COALESCE(SUM(val), 0) AS BIGINT) "+
			"FROM "+benchmarkTable,
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

	// Use one tx + prepared statement per worker to avoid autocommit-per-row contention.
	printPhaseStart("CONCURRENT_INSERT_200_TX_PREPARED")
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
			ctx := context.Background()
			conn, err := db.Conn(ctx)
			if err != nil {
				errCh <- fmt.Errorf("worker=%d acquire conn failed: %w", worker, err)
				return
			}
			defer conn.Close()

			tx, err := conn.BeginTx(ctx, nil)
			if err != nil {
				errCh <- fmt.Errorf("worker=%d begin tx failed: %w", worker, err)
				return
			}
			stmt, err := tx.PrepareContext(ctx, "INSERT INTO "+benchmarkTable+" VALUES (?, ?)")
			if err != nil {
				_ = tx.Rollback()
				errCh <- fmt.Errorf("worker=%d prepare failed: %w", worker, err)
				return
			}
			defer stmt.Close()

			for id := r.start; id <= r.end; id++ {
				if _, err := stmt.ExecContext(ctx, id, expectedVal(id)); err != nil {
					_ = tx.Rollback()
					errCh <- fmt.Errorf("worker=%d id=%d insert failed: %w", worker, id, err)
					return
				}
			}
			if err := tx.Commit(); err != nil {
				errCh <- fmt.Errorf("worker=%d commit failed: %w", worker, err)
				return
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
	printOpMetric("concurrent_insert_tx_prepared_ops", duration, int64(rows))
}

func runConcurrentTransactionCommitRollback(t *testing.T, db *sql.DB, rows, workers int) {
	t.Helper()
	requirePositiveFlag(t, rows, "rows")
	requirePositiveFlag(t, workers, "workers")

	printPhaseStart("CONCURRENT_TRANSACTIONS_COMMIT_ROLLBACK")
	if err := resetTransactionBenchmarkTable(db); err != nil {
		t.Fatalf("reset transaction benchmark table failed: %v", err)
	}

	ranges := partitionRanges(rows, workers)
	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	var committedRows atomic.Int64
	var rolledBackRows atomic.Int64
	var committedSum atomic.Int64
	var txnCount int64
	start := time.Now()

	for workerID, rg := range ranges {
		if rg.end < rg.start {
			continue
		}
		txnCount++
		wg.Add(1)
		go func(worker int, r idRange) {
			defer wg.Done()
			ctx := context.Background()

			conn, err := db.Conn(ctx)
			if err != nil {
				errCh <- fmt.Errorf("worker=%d acquire conn failed: %w", worker, err)
				return
			}
			defer conn.Close()

			tx, err := conn.BeginTx(ctx, nil)
			if err != nil {
				errCh <- fmt.Errorf("worker=%d begin tx failed: %w", worker, err)
				return
			}

			stmt, err := tx.PrepareContext(ctx, "INSERT INTO "+txBenchmarkTable+" VALUES (?, ?, ?)")
			if err != nil {
				_ = tx.Rollback()
				errCh <- fmt.Errorf("worker=%d prepare insert failed: %w", worker, err)
				return
			}
			defer stmt.Close()

			var localRows int64
			var localSum int64
			for id := r.start; id <= r.end; id++ {
				val := expectedVal(id)
				if _, err := stmt.ExecContext(ctx, id, worker, val); err != nil {
					_ = tx.Rollback()
					errCh <- fmt.Errorf("worker=%d insert failed at id=%d: %w", worker, id, err)
					return
				}
				localRows++
				localSum += val
			}

			// During transaction: the worker must see its own uncommitted rows.
			var inTxCount int64
			if err := tx.QueryRowContext(
				ctx,
				"SELECT CAST(COUNT(*) AS BIGINT) FROM "+txBenchmarkTable+" WHERE worker_id = ?",
				worker,
			).Scan(&inTxCount); err != nil {
				_ = tx.Rollback()
				errCh <- fmt.Errorf("worker=%d in-tx count query failed: %w", worker, err)
				return
			}
			if inTxCount != localRows {
				_ = tx.Rollback()
				errCh <- fmt.Errorf("worker=%d in-tx count mismatch: got=%d expected=%d", worker, inTxCount, localRows)
				return
			}

			// During transaction: outside readers should not see uncommitted rows.
			var outsideCount int64
			if err := db.QueryRow(
				"SELECT CAST(COUNT(*) AS BIGINT) FROM "+txBenchmarkTable+" WHERE worker_id = ?",
				worker,
			).Scan(&outsideCount); err != nil {
				_ = tx.Rollback()
				errCh <- fmt.Errorf("worker=%d outside count query failed: %w", worker, err)
				return
			}
			if outsideCount != 0 {
				_ = tx.Rollback()
				errCh <- fmt.Errorf("worker=%d uncommitted rows visible outside tx: got=%d", worker, outsideCount)
				return
			}

			shouldCommit := worker%2 == 0
			if shouldCommit {
				if err := tx.Commit(); err != nil {
					errCh <- fmt.Errorf("worker=%d commit failed: %w", worker, err)
					return
				}
				committedRows.Add(localRows)
				committedSum.Add(localSum)
			} else {
				if err := tx.Rollback(); err != nil {
					errCh <- fmt.Errorf("worker=%d rollback failed: %w", worker, err)
					return
				}
				rolledBackRows.Add(localRows)
			}

			// After transaction end: verify persisted rows for this worker are correct.
			var afterCount int64
			if err := db.QueryRow(
				"SELECT CAST(COUNT(*) AS BIGINT) FROM "+txBenchmarkTable+" WHERE worker_id = ?",
				worker,
			).Scan(&afterCount); err != nil {
				errCh <- fmt.Errorf("worker=%d post-tx count query failed: %w", worker, err)
				return
			}
			expectedAfter := int64(0)
			if shouldCommit {
				expectedAfter = localRows
			}
			if afterCount != expectedAfter {
				errCh <- fmt.Errorf("worker=%d post-tx count mismatch: got=%d expected=%d", worker, afterCount, expectedAfter)
				return
			}
		}(workerID, rg)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent transaction benchmark failed: %v", err)
		}
	}

	expectedCommittedRows := committedRows.Load()
	expectedRolledBackRows := rolledBackRows.Load()
	var gotCountRaw any
	var gotDistinctRaw any
	var gotSumRaw any
	if err := db.QueryRow(
		"SELECT "+
			"CAST(COUNT(*) AS BIGINT), "+
			"CAST(COUNT(DISTINCT id) AS BIGINT), "+
			"CAST(COALESCE(SUM(val), 0) AS BIGINT) "+
			"FROM "+txBenchmarkTable,
	).Scan(&gotCountRaw, &gotDistinctRaw, &gotSumRaw); err != nil {
		t.Fatalf("final transaction aggregate query failed: %v", err)
	}

	gotCount, err := toInt64(gotCountRaw, "count")
	if err != nil {
		t.Fatalf("final transaction aggregate conversion failed for count: %v", err)
	}
	gotDistinct, err := toInt64(gotDistinctRaw, "count_distinct")
	if err != nil {
		t.Fatalf("final transaction aggregate conversion failed for distinct: %v", err)
	}
	gotSum, err := toInt64(gotSumRaw, "sum")
	if err != nil {
		t.Fatalf("final transaction aggregate conversion failed for sum: %v", err)
	}

	if gotCount != expectedCommittedRows {
		t.Fatalf("final committed row count mismatch: got=%d expected=%d", gotCount, expectedCommittedRows)
	}
	if gotDistinct != expectedCommittedRows {
		t.Fatalf("final committed distinct id mismatch: got=%d expected=%d", gotDistinct, expectedCommittedRows)
	}
	if gotSum != committedSum.Load() {
		t.Fatalf("final committed sum mismatch: got=%d expected=%d", gotSum, committedSum.Load())
	}
	if expectedCommittedRows+expectedRolledBackRows != int64(rows) {
		t.Fatalf("transaction row accounting mismatch: committed=%d rolled_back=%d total_expected=%d",
			expectedCommittedRows, expectedRolledBackRows, rows)
	}

	duration := time.Since(start)
	printOpMetric("concurrent_transactions_total", duration, txnCount)
	printRowMetric("concurrent_transactions_committed_rows", duration, expectedCommittedRows)
	printRowMetric("concurrent_transactions_rolledback_rows", duration, expectedRolledBackRows)
}

func isExpectedConflictError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "conflict") ||
		strings.Contains(msg, "duplicate") ||
		strings.Contains(msg, "constraint")
}

func isExpectedMissingTableError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "does not exist") ||
		strings.Contains(msg, "not found") ||
		strings.Contains(msg, "catalog")
}

func runConcurrentTransactionCreateDropInsertSelect(t *testing.T, db *sql.DB, rows, workers int) {
	t.Helper()
	requirePositiveFlag(t, rows, "rows")
	requirePositiveFlag(t, workers, "workers")

	printPhaseStart("CONCURRENT_TRANSACTIONS_DDL_DML")
	ranges := partitionRanges(rows, workers)
	runID := time.Now().UnixNano()
	tablePrefix := fmt.Sprintf("go_flight_tx_ddl_%d_", runID)

	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	var txnCount int64
	var insertedRows atomic.Int64
	var selectedRows atomic.Int64
	start := time.Now()

	for workerID, rg := range ranges {
		if rg.end < rg.start {
			continue
		}
		txnCount++
		wg.Add(1)
		go func(worker int, r idRange) {
			defer wg.Done()
			ctx := context.Background()
			tableName := fmt.Sprintf("%s%d", tablePrefix, worker)

			conn, err := db.Conn(ctx)
			if err != nil {
				errCh <- fmt.Errorf("worker=%d acquire conn failed: %w", worker, err)
				return
			}
			defer conn.Close()

			tx, err := conn.BeginTx(ctx, nil)
			if err != nil {
				errCh <- fmt.Errorf("worker=%d begin tx failed: %w", worker, err)
				return
			}

			if _, err := tx.ExecContext(
				ctx,
				"CREATE TABLE "+tableName+" (id BIGINT PRIMARY KEY, val BIGINT)",
			); err != nil {
				_ = tx.Rollback()
				errCh <- fmt.Errorf("worker=%d create table failed: %w", worker, err)
				return
			}

			// Outside connection should not observe uncommitted DDL.
			var outsideDdlRaw any
			outsideErr := db.QueryRow(
				"SELECT CAST(COUNT(*) AS BIGINT) FROM " + tableName,
			).Scan(&outsideDdlRaw)
			if outsideErr == nil {
				_ = tx.Rollback()
				errCh <- fmt.Errorf("worker=%d outside query unexpectedly saw uncommitted table", worker)
				return
			}
			if !isExpectedMissingTableError(outsideErr) {
				_ = tx.Rollback()
				errCh <- fmt.Errorf("worker=%d outside query returned unexpected error for uncommitted table: %w", worker, outsideErr)
				return
			}

			stmt, err := tx.PrepareContext(ctx, "INSERT INTO "+tableName+" VALUES (?, ?)")
			if err != nil {
				_ = tx.Rollback()
				errCh <- fmt.Errorf("worker=%d prepare insert failed: %w", worker, err)
				return
			}
			defer stmt.Close()

			var localRows int64
			var localSum int64
			for id := r.start; id <= r.end; id++ {
				val := expectedVal(id)
				if _, err := stmt.ExecContext(ctx, id, val); err != nil {
					_ = tx.Rollback()
					errCh <- fmt.Errorf("worker=%d insert failed at id=%d: %w", worker, id, err)
					return
				}
				localRows++
				localSum += val
			}

			// Arrow Go Flight SQL driver v18.5.1 routes zero-arg Tx.QueryContext through
			// Connection.QueryContext, which currently ignores c.txn. Force parameter binding
			// so database/sql uses prepared statement path bound to the active transaction.
			var inTxCountRaw any
			var inTxSumRaw any
			if err := tx.QueryRowContext(
				ctx,
				"SELECT CAST(COUNT(*) AS BIGINT), CAST(COALESCE(SUM(val), 0) AS BIGINT) FROM "+tableName+
					" WHERE id >= ?",
				r.start,
			).Scan(&inTxCountRaw, &inTxSumRaw); err != nil {
				_ = tx.Rollback()
				errCh <- fmt.Errorf("worker=%d in-tx select aggregate failed: %w", worker, err)
				return
			}
			inTxCount, err := toInt64(inTxCountRaw, "in_tx_count")
			if err != nil {
				_ = tx.Rollback()
				errCh <- fmt.Errorf("worker=%d in-tx count conversion failed: %w", worker, err)
				return
			}
			inTxSum, err := toInt64(inTxSumRaw, "in_tx_sum")
			if err != nil {
				_ = tx.Rollback()
				errCh <- fmt.Errorf("worker=%d in-tx sum conversion failed: %w", worker, err)
				return
			}
			if inTxCount != localRows {
				_ = tx.Rollback()
				errCh <- fmt.Errorf("worker=%d in-tx row count mismatch: got=%d expected=%d", worker, inTxCount, localRows)
				return
			}
			if inTxSum != localSum {
				_ = tx.Rollback()
				errCh <- fmt.Errorf("worker=%d in-tx sum mismatch: got=%d expected=%d", worker, inTxSum, localSum)
				return
			}

			if _, err := tx.ExecContext(ctx, "DROP TABLE "+tableName); err != nil {
				_ = tx.Rollback()
				errCh <- fmt.Errorf("worker=%d drop table failed: %w", worker, err)
				return
			}

			// After DROP in tx, table should be inaccessible in the same tx.
			var droppedCheckRaw any
			droppedCheckErr := tx.QueryRowContext(
				ctx,
				"SELECT CAST(COUNT(*) AS BIGINT) FROM "+tableName,
			).Scan(&droppedCheckRaw)
			if droppedCheckErr == nil {
				_ = tx.Rollback()
				errCh <- fmt.Errorf("worker=%d dropped table still queryable inside tx", worker)
				return
			}
			if !isExpectedMissingTableError(droppedCheckErr) {
				_ = tx.Rollback()
				errCh <- fmt.Errorf("worker=%d unexpected error after drop inside tx: %w", worker, droppedCheckErr)
				return
			}

			if err := tx.Commit(); err != nil {
				errCh <- fmt.Errorf("worker=%d commit failed: %w", worker, err)
				return
			}

			// After commit, table must not exist.
			var postCommitRaw any
			postCommitErr := db.QueryRow(
				"SELECT CAST(COUNT(*) AS BIGINT) FROM " + tableName,
			).Scan(&postCommitRaw)
			if postCommitErr == nil {
				errCh <- fmt.Errorf("worker=%d table exists after commit despite drop", worker)
				return
			}
			if !isExpectedMissingTableError(postCommitErr) {
				errCh <- fmt.Errorf("worker=%d unexpected post-commit missing-table error: %w", worker, postCommitErr)
				return
			}

			insertedRows.Add(localRows)
			selectedRows.Add(localRows)
		}(workerID, rg)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent transaction DDL/DML benchmark failed: %v", err)
		}
	}

	var leftoverTablesRaw any
	if err := db.QueryRow(
		"SELECT CAST(COUNT(*) AS BIGINT) FROM duckdb_tables() WHERE table_name LIKE ?",
		tablePrefix+"%",
	).Scan(&leftoverTablesRaw); err != nil {
		t.Fatalf("leftover table verification query failed: %v", err)
	}
	leftoverTables, err := toInt64(leftoverTablesRaw, "leftover_tables")
	if err != nil {
		t.Fatalf("leftover table conversion failed: %v", err)
	}
	if leftoverTables != 0 {
		t.Fatalf("leftover table count mismatch: got=%d expected=0", leftoverTables)
	}

	if insertedRows.Load() != int64(rows) {
		t.Fatalf("inserted row accounting mismatch: got=%d expected=%d", insertedRows.Load(), rows)
	}
	if selectedRows.Load() != int64(rows) {
		t.Fatalf("selected row accounting mismatch: got=%d expected=%d", selectedRows.Load(), rows)
	}

	duration := time.Since(start)
	printOpMetric("concurrent_tx_ddl_dml_total", duration, txnCount)
	printRowMetric("concurrent_tx_ddl_dml_inserted_rows", duration, insertedRows.Load())
	printRowMetric("concurrent_tx_ddl_dml_selected_rows", duration, selectedRows.Load())
}

func runConcurrentTransactionCommitConflicts(t *testing.T, db *sql.DB, workers int) {
	t.Helper()
	requirePositiveFlag(t, workers, "workers")

	printPhaseStart("CONCURRENT_TRANSACTIONS_COMMIT_CONFLICTS")
	if err := resetTransactionConflictBenchmarkTable(db); err != nil {
		t.Fatalf("reset transaction conflict benchmark table failed: %v", err)
	}

	for id := 1; id <= workers; id++ {
		if _, err := db.Exec(
			"INSERT INTO "+txConflictBenchmarkTable+" VALUES (?, ?, ?)",
			id, -1, 0,
		); err != nil {
			t.Fatalf("seed conflict row failed for id=%d: %v", id, err)
		}
	}

	errCh := make(chan error, workers*2)
	var pairWG sync.WaitGroup
	var committedCount atomic.Int64
	var conflictCount atomic.Int64
	start := time.Now()

	for pairIdx := 0; pairIdx < workers; pairIdx++ {
		pairWG.Add(1)
		go func(pair int) {
			defer pairWG.Done()

			pairID := int64(pair + 1)
			startUpdate := make(chan struct{})
			startCommit := make(chan struct{})
			beginReady := make(chan struct{}, 2)
			var precommitReadyWG sync.WaitGroup
			var contenderWG sync.WaitGroup
			precommitReadyWG.Add(2)
			contenderWG.Add(2)

			runContender := func(contenderOffset int64) {
				defer contenderWG.Done()
				ctx := context.Background()
				workerID := int64(pair*2) + contenderOffset

				conn, err := db.Conn(ctx)
				if err != nil {
					errCh <- fmt.Errorf("pair=%d contender=%d acquire conn failed: %w", pair, contenderOffset, err)
					precommitReadyWG.Done()
					return
				}
				defer conn.Close()

				tx, err := conn.BeginTx(ctx, nil)
				if err != nil {
					errCh <- fmt.Errorf("pair=%d contender=%d begin tx failed: %w", pair, contenderOffset, err)
					precommitReadyWG.Done()
					return
				}

				beginReady <- struct{}{}
				<-startUpdate
				_, updateErr := tx.ExecContext(
					ctx,
					"UPDATE "+txConflictBenchmarkTable+" SET worker_id = ?, attempt = attempt + 1 WHERE id = ?",
					workerID, pairID,
				)
				if updateErr != nil {
					if isExpectedConflictError(updateErr) {
						conflictCount.Add(1)
						_ = tx.Rollback()
						precommitReadyWG.Done()
						return
					}
					_ = tx.Rollback()
					errCh <- fmt.Errorf("pair=%d contender=%d update failed: %w", pair, contenderOffset, updateErr)
					precommitReadyWG.Done()
					return
				}

				var inTxAttempt int64
				if err := tx.QueryRowContext(
					ctx,
					"SELECT CAST(attempt AS BIGINT) FROM "+txConflictBenchmarkTable+" WHERE id = ?",
					pairID,
				).Scan(&inTxAttempt); err != nil {
					_ = tx.Rollback()
					errCh <- fmt.Errorf("pair=%d contender=%d in-tx attempt query failed: %w", pair, contenderOffset, err)
					precommitReadyWG.Done()
					return
				}
				if inTxAttempt != 1 {
					_ = tx.Rollback()
					errCh <- fmt.Errorf("pair=%d contender=%d in-tx attempt mismatch: got=%d expected=1", pair, contenderOffset, inTxAttempt)
					precommitReadyWG.Done()
					return
				}

				precommitReadyWG.Done()
				<-startCommit
				if err := tx.Commit(); err != nil {
					if isExpectedConflictError(err) {
						conflictCount.Add(1)
						return
					}
					errCh <- fmt.Errorf("pair=%d contender=%d commit failed: %w", pair, contenderOffset, err)
					return
				}
				committedCount.Add(1)
			}

			go runContender(0)
			go runContender(1)

			<-beginReady
			<-beginReady
			close(startUpdate)
			precommitReadyWG.Wait()

			var outsideAttempt int64
			var outsideWorker int64
			if err := db.QueryRow(
				"SELECT CAST(attempt AS BIGINT), CAST(worker_id AS BIGINT) FROM "+txConflictBenchmarkTable+" WHERE id = ?",
				pairID,
			).Scan(&outsideAttempt, &outsideWorker); err != nil {
				errCh <- fmt.Errorf("pair=%d outside pre-commit query failed: %w", pair, err)
				close(startCommit)
				contenderWG.Wait()
				return
			}
			if outsideAttempt != 0 || outsideWorker != -1 {
				errCh <- fmt.Errorf("pair=%d outside pre-commit state mismatch: attempt=%d worker_id=%d expected attempt=0 worker_id=-1",
					pair, outsideAttempt, outsideWorker)
				close(startCommit)
				contenderWG.Wait()
				return
			}

			close(startCommit)
			contenderWG.Wait()
		}(pairIdx)
	}

	pairWG.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent transaction commit conflict benchmark failed: %v", err)
		}
	}

	commits := committedCount.Load()
	conflicts := conflictCount.Load()
	expectedCommits := int64(workers)
	expectedConflicts := int64(workers)
	if commits != expectedCommits {
		t.Fatalf("commit conflict benchmark expected %d committed tx, got=%d", expectedCommits, commits)
	}
	if conflicts != expectedConflicts {
		t.Fatalf("commit conflict benchmark expected %d conflict failures, got=%d", expectedConflicts, conflicts)
	}

	var finalCountRaw any
	var finalAttemptSumRaw any
	var finalWinnerCountRaw any
	if err := db.QueryRow(
		"SELECT "+
			"CAST(COUNT(*) AS BIGINT), "+
			"CAST(COALESCE(SUM(attempt), 0) AS BIGINT), "+
			"CAST(COALESCE(SUM(CASE WHEN worker_id >= 0 THEN 1 ELSE 0 END), 0) AS BIGINT) "+
			"FROM "+txConflictBenchmarkTable,
	).Scan(&finalCountRaw, &finalAttemptSumRaw, &finalWinnerCountRaw); err != nil {
		t.Fatalf("final conflict aggregate query failed: %v", err)
	}
	finalCount, err := toInt64(finalCountRaw, "final_count")
	if err != nil {
		t.Fatalf("final conflict final_count conversion failed: %v", err)
	}
	finalAttemptSum, err := toInt64(finalAttemptSumRaw, "final_attempt_sum")
	if err != nil {
		t.Fatalf("final conflict final_attempt_sum conversion failed: %v", err)
	}
	finalWinnerCount, err := toInt64(finalWinnerCountRaw, "final_winner_count")
	if err != nil {
		t.Fatalf("final conflict final_winner_count conversion failed: %v", err)
	}
	if finalCount != int64(workers) {
		t.Fatalf("final conflict table count mismatch: got=%d expected=%d", finalCount, workers)
	}
	if finalAttemptSum != int64(workers) {
		t.Fatalf("final conflict attempt sum mismatch: got=%d expected=%d", finalAttemptSum, workers)
	}
	if finalWinnerCount != int64(workers) {
		t.Fatalf("final conflict winner count mismatch: got=%d expected=%d", finalWinnerCount, workers)
	}

	duration := time.Since(start)
	printOpMetric("concurrent_tx_conflict_total", duration, int64(workers*2))
	printOpMetric("concurrent_tx_conflict_commits", duration, commits)
	printOpMetric("concurrent_tx_conflict_failures", duration, conflicts)
}

func runSelectPrepareModes(t *testing.T, db *sql.DB, rows, iters int) {
	t.Helper()
	requirePositiveFlag(t, rows, "rows")
	requirePositiveFlag(t, iters, "select-iters")

	printPhaseStart("SELECT_PREPARE_MODES")
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("select prepare modes acquire conn failed: %v", err)
	}
	defer conn.Close()

	selectSQL := "SELECT val FROM " + benchmarkTable + " WHERE id = ?"

	start := time.Now()
	for i := 0; i < iters; i++ {
		id := int64(i%rows) + 1
		query := fmt.Sprintf("SELECT val FROM %s WHERE id = %d", benchmarkTable, id)
		var got int64
		if err := conn.QueryRowContext(ctx, query).Scan(&got); err != nil {
			t.Fatalf("select direct no-prepare failed at iter=%d id=%d: %v", i, id, err)
		}
		expected := expectedVal(id)
		if got != expected {
			t.Fatalf("select direct no-prepare mismatch at iter=%d id=%d: got=%d expected=%d", i, id, got, expected)
		}
	}
	directDuration := time.Since(start)
	printOpMetric("select_point_direct_no_prepare", directDuration, int64(iters))

	start = time.Now()
	for i := 0; i < iters; i++ {
		id := int64(i%rows) + 1
		stmt, err := conn.PrepareContext(ctx, selectSQL)
		if err != nil {
			t.Fatalf("select prepare-each prepare failed at iter=%d: %v", i, err)
		}
		var got int64
		queryErr := stmt.QueryRowContext(ctx, id).Scan(&got)
		closeErr := stmt.Close()
		if queryErr != nil {
			t.Fatalf("select prepare-each query failed at iter=%d id=%d: %v", i, id, queryErr)
		}
		if closeErr != nil {
			t.Fatalf("select prepare-each close failed at iter=%d: %v", i, closeErr)
		}
		expected := expectedVal(id)
		if got != expected {
			t.Fatalf("select prepare-each mismatch at iter=%d id=%d: got=%d expected=%d", i, id, got, expected)
		}
	}
	prepareEachDuration := time.Since(start)
	printOpMetric("select_point_prepare_each_time", prepareEachDuration, int64(iters))

	stmt, err := conn.PrepareContext(ctx, selectSQL)
	if err != nil {
		t.Fatalf("select prepare-once prepare failed: %v", err)
	}
	start = time.Now()
	for i := 0; i < iters; i++ {
		id := int64(i%rows) + 1
		var got int64
		if err := stmt.QueryRowContext(ctx, id).Scan(&got); err != nil {
			_ = stmt.Close()
			t.Fatalf("select prepare-once query failed at iter=%d id=%d: %v", i, id, err)
		}
		expected := expectedVal(id)
		if got != expected {
			_ = stmt.Close()
			t.Fatalf("select prepare-once mismatch at iter=%d id=%d: got=%d expected=%d", i, id, got, expected)
		}
	}
	prepareReuseDuration := time.Since(start)
	if err := stmt.Close(); err != nil {
		t.Fatalf("select prepare-once close failed: %v", err)
	}
	printOpMetric("select_point_prepare_once_reuse", prepareReuseDuration, int64(iters))
}

func runConcurrentSelectMode(t *testing.T, db *sql.DB, rows, workers, iters int, metricName string,
	runWorker func(context.Context, *sql.Conn, int, int) error) {
	t.Helper()
	requirePositiveFlag(t, rows, "rows")
	requirePositiveFlag(t, workers, "workers")
	requirePositiveFlag(t, iters, "select-iters")

	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	start := time.Now()

	for worker := 0; worker < workers; worker++ {
		startIter := (worker * iters) / workers
		endIter := ((worker + 1) * iters) / workers
		if endIter <= startIter {
			continue
		}

		wg.Add(1)
		go func(workerID, begin, end int) {
			defer wg.Done()
			ctx := context.Background()
			conn, err := db.Conn(ctx)
			if err != nil {
				errCh <- fmt.Errorf("worker=%d acquire conn failed: %w", workerID, err)
				return
			}
			defer conn.Close()

			if err := runWorker(ctx, conn, begin, end); err != nil {
				errCh <- fmt.Errorf("worker=%d %s failed: %w", workerID, metricName, err)
				return
			}
		}(worker, startIter, endIter)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent select mode failed: %v", err)
		}
	}

	duration := time.Since(start)
	printOpMetric(metricName, duration, int64(iters))
}

func runSelectPrepareModesConcurrent(t *testing.T, db *sql.DB, rows, workers, iters int) {
	t.Helper()
	requirePositiveFlag(t, rows, "rows")
	requirePositiveFlag(t, workers, "workers")
	requirePositiveFlag(t, iters, "select-iters")

	printPhaseStart("SELECT_PREPARE_MODES_CONCURRENT")

	runConcurrentSelectMode(t, db, rows, workers, iters, "select_point_concurrent_direct_no_prepare",
		func(ctx context.Context, conn *sql.Conn, begin, end int) error {
			for i := begin; i < end; i++ {
				id := int64(i%rows) + 1
				query := fmt.Sprintf("SELECT val FROM %s WHERE id = %d", benchmarkTable, id)
				var got int64
				if err := conn.QueryRowContext(ctx, query).Scan(&got); err != nil {
					return fmt.Errorf("iter=%d id=%d query failed: %w", i, id, err)
				}
				expected := expectedVal(id)
				if got != expected {
					return fmt.Errorf("iter=%d id=%d mismatch: got=%d expected=%d", i, id, got, expected)
				}
			}
			return nil
		},
	)

	runConcurrentSelectMode(t, db, rows, workers, iters, "select_point_concurrent_prepare_each_time",
		func(ctx context.Context, conn *sql.Conn, begin, end int) error {
			for i := begin; i < end; i++ {
				id := int64(i%rows) + 1
				stmt, err := conn.PrepareContext(ctx, "SELECT val FROM "+benchmarkTable+" WHERE id = ?")
				if err != nil {
					return fmt.Errorf("iter=%d prepare failed: %w", i, err)
				}
				var got int64
				queryErr := stmt.QueryRowContext(ctx, id).Scan(&got)
				closeErr := stmt.Close()
				if queryErr != nil {
					return fmt.Errorf("iter=%d id=%d query failed: %w", i, id, queryErr)
				}
				if closeErr != nil {
					return fmt.Errorf("iter=%d close failed: %w", i, closeErr)
				}
				expected := expectedVal(id)
				if got != expected {
					return fmt.Errorf("iter=%d id=%d mismatch: got=%d expected=%d", i, id, got, expected)
				}
			}
			return nil
		},
	)

	runConcurrentSelectMode(t, db, rows, workers, iters, "select_point_concurrent_prepare_once_reuse",
		func(ctx context.Context, conn *sql.Conn, begin, end int) error {
			stmt, err := conn.PrepareContext(ctx, "SELECT val FROM "+benchmarkTable+" WHERE id = ?")
			if err != nil {
				return fmt.Errorf("prepare once failed: %w", err)
			}
			defer stmt.Close()

			for i := begin; i < end; i++ {
				id := int64(i%rows) + 1
				var got int64
				if err := stmt.QueryRowContext(ctx, id).Scan(&got); err != nil {
					return fmt.Errorf("iter=%d id=%d query failed: %w", i, id, err)
				}
				expected := expectedVal(id)
				if got != expected {
					return fmt.Errorf("iter=%d id=%d mismatch: got=%d expected=%d", i, id, got, expected)
				}
			}
			return nil
		},
	)
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

func runOrderedConcurrentFullRead(t *testing.T, db *sql.DB, rows, workers int) {
	t.Helper()
	requirePositiveFlag(t, rows, "rows")
	requirePositiveFlag(t, workers, "workers")

	printPhaseStart("SELECT_ORDERED_CONCURRENT_FULL_200")
	errCh := make(chan error, workers)
	var verifiedRows atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()

	for workerID := 0; workerID < workers; workerID++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			queryRows, err := db.Query("SELECT id, val FROM " + benchmarkTable + " ORDER BY id")
			if err != nil {
				errCh <- fmt.Errorf("worker=%d query failed: %w", worker, err)
				return
			}
			defer queryRows.Close()

			expectedID := int64(1)
			var localCount int64
			for queryRows.Next() {
				var id int64
				var val int64
				if err := queryRows.Scan(&id, &val); err != nil {
					errCh <- fmt.Errorf("worker=%d scan failed: %w", worker, err)
					return
				}
				if id != expectedID {
					errCh <- fmt.Errorf("worker=%d id mismatch: got=%d expected=%d", worker, id, expectedID)
					return
				}
				ev := expectedVal(id)
				if val != ev {
					errCh <- fmt.Errorf("worker=%d val mismatch for id=%d: got=%d expected=%d", worker, id, val, ev)
					return
				}
				expectedID++
				localCount++
			}
			if err := queryRows.Err(); err != nil {
				errCh <- fmt.Errorf("worker=%d rows iteration failed: %w", worker, err)
				return
			}
			if localCount != int64(rows) {
				errCh <- fmt.Errorf("worker=%d row count mismatch: got=%d expected=%d", worker, localCount, rows)
				return
			}
			verifiedRows.Add(localCount)
		}(workerID)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent full ordered read failed: %v", err)
		}
	}

	expectedTotal := int64(rows) * int64(workers)
	total := verifiedRows.Load()
	if total != expectedTotal {
		t.Fatalf("concurrent full ordered read total mismatch: got=%d expected=%d", total, expectedTotal)
	}
	duration := time.Since(start)
	printRowMetric("select_ordered_concurrent_full_rows", duration, total)
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
	requirePositiveFlag(t, *flagSelectIters, "select-iters")

	db := openDB(t)
	defer db.Close()

	runCrudSingleOpAutocommit(t, db, *flagCrudIters)
	runBatchInsert(t, db, *flagRows, *flagBatchSize)
	runConcurrentInsert(t, db, *flagRows, *flagWorkers)
	runSelectPrepareModes(t, db, *flagRows, *flagSelectIters)
	runSelectPrepareModesConcurrent(t, db, *flagRows, *flagWorkers, *flagSelectIters)
	runConcurrentTransactionCommitRollback(t, db, *flagRows, *flagWorkers)
	runConcurrentTransactionCreateDropInsertSelect(t, db, *flagRows, *flagWorkers)
	runConcurrentTransactionCommitConflicts(t, db, *flagWorkers)
	runOrderedSingleRead(t, db, *flagRows)
	runOrderedConcurrentRead(t, db, *flagRows, *flagWorkers)
	runOrderedConcurrentFullRead(t, db, *flagRows, *flagWorkers)
}
