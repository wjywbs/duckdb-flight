package goflightbench

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func reproEnabled() bool {
	return os.Getenv("FLIGHTSQL_RUN_REPRO") == "1"
}

func reproIntEnv(name string, defaultValue int) int {
	value := os.Getenv(name)
	if value == "" {
		return defaultValue
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return defaultValue
	}
	return parsed
}

func rawBuildTxWorkerInsertSQL(startID, workerID int64, rows int) string {
	var builder strings.Builder
	builder.WriteString("INSERT INTO ")
	builder.WriteString(txBenchmarkTable)
	builder.WriteString(" VALUES ")
	for i := 0; i < rows; i++ {
		if i > 0 {
			builder.WriteString(", ")
		}
		id := startID + int64(i)
		builder.WriteString("(")
		builder.WriteString(strconv.FormatInt(id, 10))
		builder.WriteString(", ")
		builder.WriteString(strconv.FormatInt(workerID, 10))
		builder.WriteString(", ")
		builder.WriteString(strconv.FormatInt(expectedVal(id), 10))
		builder.WriteString(")")
	}
	return builder.String()
}

func runRawReproIteration(t *testing.T, rowsPerWorker, workers int) error {
	t.Helper()

	ctx := context.Background()
	adminClient := openRawClient(t)
	defer adminClient.Close()

	if err := rawResetTransactionBenchmarkTable(ctx, adminClient); err != nil {
		return fmt.Errorf("reset table failed: %w", err)
	}

	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			workerClient, err := newRawClient(ctx)
			if err != nil {
				errCh <- fmt.Errorf("worker=%d newRawClient failed: %w", worker, err)
				return
			}
			defer workerClient.Close()

			tx, err := workerClient.BeginTransaction(ctx)
			if err != nil {
				errCh <- fmt.Errorf("worker=%d BeginTransaction failed: %w", worker, err)
				return
			}

			insertStmt, err := tx.Prepare(ctx, "INSERT INTO "+txBenchmarkTable+" VALUES (?, ?, ?)")
			if err != nil {
				_ = tx.Rollback(ctx)
				errCh <- fmt.Errorf("worker=%d prepare insert failed: %w", worker, err)
				return
			}

			baseID := int64(worker*rowsPerWorker) + 1
			for i := 0; i < rowsPerWorker; i++ {
				id := baseID + int64(i)
				val := expectedVal(id)
				if err := rawSetPreparedParameters(insertStmt, [][]any{{id, int64(worker), val}}); err != nil {
					_ = tx.Rollback(ctx)
					_ = insertStmt.Close(ctx)
					errCh <- fmt.Errorf("worker=%d bind insert failed: %w", worker, err)
					return
				}
				affected, err := insertStmt.ExecuteUpdate(ctx)
				if err != nil {
					_ = tx.Rollback(ctx)
					_ = insertStmt.Close(ctx)
					errCh <- fmt.Errorf("worker=%d execute insert failed: %w", worker, err)
					return
				}
				if affected != 1 {
					_ = tx.Rollback(ctx)
					_ = insertStmt.Close(ctx)
					errCh <- fmt.Errorf("worker=%d insert affected=%d expected=1", worker, affected)
					return
				}
			}
			if err := insertStmt.Close(ctx); err != nil {
				_ = tx.Rollback(ctx)
				errCh <- fmt.Errorf("worker=%d close insert stmt failed: %w", worker, err)
				return
			}

			shouldCommit := worker%2 == 0
			if shouldCommit {
				if err := tx.Commit(ctx); err != nil {
					errCh <- fmt.Errorf("worker=%d commit failed: %w", worker, err)
					return
				}
			} else {
				if err := tx.Rollback(ctx); err != nil {
					errCh <- fmt.Errorf("worker=%d rollback failed: %w", worker, err)
					return
				}
			}

			expectedCount := int64(0)
			if shouldCommit {
				expectedCount = int64(rowsPerWorker)
			}

			directSQL := fmt.Sprintf("SELECT CAST(COUNT(*) AS BIGINT) FROM %s WHERE worker_id = %d", txBenchmarkTable, worker)
			directCount, err := rawQueryInt64(ctx, workerClient, directSQL)
			if err != nil {
				errCh <- fmt.Errorf("worker=%d direct query failed: %w", worker, err)
				return
			}
			directCountSecond, err := rawQueryInt64(ctx, workerClient, directSQL)
			if err != nil {
				errCh <- fmt.Errorf("worker=%d direct query second failed: %w", worker, err)
				return
			}

			verifyStmt, err := workerClient.Prepare(ctx, "SELECT CAST(COUNT(*) AS BIGINT) FROM "+txBenchmarkTable+" WHERE worker_id = ?")
			if err != nil {
				errCh <- fmt.Errorf("worker=%d prepare verify failed: %w", worker, err)
				return
			}
			preparedCount, prepErr := rawExecutePreparedQueryInt64(ctx, workerClient, verifyStmt, [][]any{{int64(worker)}})
			closeErr := verifyStmt.Close(ctx)
			if prepErr != nil {
				errCh <- fmt.Errorf("worker=%d prepared verify execute failed: %w", worker, prepErr)
				return
			}
			if closeErr != nil {
				errCh <- fmt.Errorf("worker=%d prepared verify close failed: %w", worker, closeErr)
				return
			}

			freshClient, err := newRawClient(ctx)
			if err != nil {
				errCh <- fmt.Errorf("worker=%d new fresh client failed: %w", worker, err)
				return
			}
			freshCount, freshErr := rawQueryInt64(ctx, freshClient, directSQL)
			freshClient.Close()
			if freshErr != nil {
				errCh <- fmt.Errorf("worker=%d fresh client query failed: %w", worker, freshErr)
				return
			}

			if directCount != expectedCount || directCountSecond != expectedCount || preparedCount != expectedCount ||
				freshCount != expectedCount {
				repeatCounts := make([]int64, 0, 3)
				for probe := 0; probe < 3; probe++ {
					probeClient, probeErr := newRawClient(ctx)
					if probeErr != nil {
						errCh <- fmt.Errorf("worker=%d probe client failed: %w", worker, probeErr)
						return
					}
					probeCount, probeErr := rawQueryInt64(ctx, probeClient, directSQL)
					_ = probeClient.Close()
					if probeErr != nil {
						errCh <- fmt.Errorf("worker=%d probe query failed: %w", worker, probeErr)
						return
					}
					repeatCounts = append(repeatCounts, probeCount)
				}
				errCh <- fmt.Errorf(
					"worker=%d mismatch direct=%d direct2=%d prepared=%d fresh=%d probe=%v expected=%d commit=%t",
					worker, directCount, directCountSecond, preparedCount, freshCount, repeatCounts, expectedCount, shouldCommit,
				)
				return
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

func runDatabaseSQLReproIteration(t *testing.T, rowsPerWorker, workers int) error {
	t.Helper()
	db := openDB(t)
	defer db.Close()
	if err := resetTransactionBenchmarkTable(db); err != nil {
		return fmt.Errorf("reset table failed: %w", err)
	}

	ctx := context.Background()
	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := db.Conn(ctx)
			if err != nil {
				errCh <- fmt.Errorf("worker=%d db.Conn failed: %w", worker, err)
				return
			}
			defer conn.Close()

			tx, err := conn.BeginTx(ctx, nil)
			if err != nil {
				errCh <- fmt.Errorf("worker=%d BeginTx failed: %w", worker, err)
				return
			}
			stmt, err := tx.PrepareContext(ctx, "INSERT INTO "+txBenchmarkTable+" VALUES (?, ?, ?)")
			if err != nil {
				_ = tx.Rollback()
				errCh <- fmt.Errorf("worker=%d prepare insert failed: %w", worker, err)
				return
			}

			baseID := int64(worker*rowsPerWorker) + 1
			for i := 0; i < rowsPerWorker; i++ {
				id := baseID + int64(i)
				if _, err := stmt.ExecContext(ctx, id, worker, expectedVal(id)); err != nil {
					_ = stmt.Close()
					_ = tx.Rollback()
					errCh <- fmt.Errorf("worker=%d insert failed: %w", worker, err)
					return
				}
			}
			_ = stmt.Close()

			shouldCommit := worker%2 == 0
			if shouldCommit {
				if err := tx.Commit(); err != nil {
					errCh <- fmt.Errorf("worker=%d commit failed: %w", worker, err)
					return
				}
			} else {
				if err := tx.Rollback(); err != nil {
					errCh <- fmt.Errorf("worker=%d rollback failed: %w", worker, err)
					return
				}
			}

			expectedCount := int64(0)
			if shouldCommit {
				expectedCount = int64(rowsPerWorker)
			}

			directSQL := fmt.Sprintf("SELECT CAST(COUNT(*) AS BIGINT) FROM %s WHERE worker_id = %d", txBenchmarkTable, worker)
			var directCount int64
			if err := conn.QueryRowContext(ctx, directSQL).Scan(&directCount); err != nil {
				errCh <- fmt.Errorf("worker=%d direct query failed: %w", worker, err)
				return
			}

			var preparedCount int64
			if err := conn.QueryRowContext(
				ctx,
				"SELECT CAST(COUNT(*) AS BIGINT) FROM "+txBenchmarkTable+" WHERE worker_id = ?",
				worker,
			).Scan(&preparedCount); err != nil {
				errCh <- fmt.Errorf("worker=%d prepared query failed: %w", worker, err)
				return
			}

			if directCount != expectedCount || preparedCount != expectedCount {
				errCh <- fmt.Errorf(
					"worker=%d mismatch direct=%d prepared=%d expected=%d commit=%t",
					worker, directCount, preparedCount, expectedCount, shouldCommit,
				)
				return
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

func TestRawNonPreparedCountReproducer(t *testing.T) {
	if !reproEnabled() {
		t.Skip("set FLIGHTSQL_RUN_REPRO=1 to run reproducer")
	}
	iters := reproIntEnv("FLIGHTSQL_REPRO_ITERS", 20)
	workers := reproIntEnv("FLIGHTSQL_REPRO_WORKERS", 200)
	rowsPerWorker := reproIntEnv("FLIGHTSQL_REPRO_ROWS_PER_WORKER", 250)

	for iter := 1; iter <= iters; iter++ {
		if err := runRawReproIteration(t, rowsPerWorker, workers); err != nil {
			t.Fatalf("raw reproducer failed on iteration=%d: %v", iter, err)
		}
		t.Logf("raw reproducer iteration %d/%d passed", iter, iters)
	}
	t.Skipf("raw reproducer did not fail in %d iterations (inconclusive)", iters)
}

func TestRawPreparedLiteralCountReproducer(t *testing.T) {
	if !reproEnabled() {
		t.Skip("set FLIGHTSQL_RUN_REPRO=1 to run reproducer")
	}

	ctx := context.Background()
	adminClient := openRawClient(t)
	defer adminClient.Close()

	if err := rawResetTransactionBenchmarkTable(ctx, adminClient); err != nil {
		t.Fatalf("reset table failed: %v", err)
	}

	writerClient, err := newRawClient(ctx)
	if err != nil {
		t.Fatalf("new writer client failed: %v", err)
	}
	defer writerClient.Close()

	preparedClient, err := newRawClient(ctx)
	if err != nil {
		t.Fatalf("new prepared client failed: %v", err)
	}
	defer preparedClient.Close()

	freshClient, err := newRawClient(ctx)
	if err != nil {
		t.Fatalf("new fresh client failed: %v", err)
	}
	defer freshClient.Close()

	if _, err := rawExecuteUpdate(ctx, writerClient, rawBuildTxWorkerInsertSQL(1, 14, 250)); err != nil {
		t.Fatalf("insert target worker failed: %v", err)
	}

	const directSQL = "SELECT CAST(COUNT(*) AS BIGINT) FROM " + txBenchmarkTable + " WHERE worker_id = 14"
	stmt, err := preparedClient.Prepare(ctx, directSQL)
	if err != nil {
		t.Fatalf("prepare literal query failed: %v", err)
	}
	defer stmt.Close(ctx)

	if _, err := rawExecuteUpdate(ctx, writerClient, rawBuildTxWorkerInsertSQL(251, 15, 250)); err != nil {
		t.Fatalf("insert other worker failed: %v", err)
	}

	preparedCount, err := rawExecutePreparedQueryInt64(ctx, preparedClient, stmt, nil)
	if err != nil {
		t.Fatalf("execute prepared literal query failed: %v", err)
	}
	freshCount, err := rawQueryInt64(ctx, freshClient, directSQL)
	if err != nil {
		t.Fatalf("fresh literal query failed: %v", err)
	}

	if preparedCount != freshCount {
		t.Fatalf("prepared literal mismatch prepared=%d fresh=%d expected=250", preparedCount, freshCount)
	}
	if preparedCount != 250 {
		t.Fatalf("unexpected prepared literal count=%d expected=250", preparedCount)
	}
}

func TestDatabaseSQLCountControl(t *testing.T) {
	if !reproEnabled() {
		t.Skip("set FLIGHTSQL_RUN_REPRO=1 to run control")
	}
	iters := reproIntEnv("FLIGHTSQL_REPRO_ITERS", 20)
	workers := reproIntEnv("FLIGHTSQL_REPRO_WORKERS", 200)
	rowsPerWorker := reproIntEnv("FLIGHTSQL_REPRO_ROWS_PER_WORKER", 250)

	for iter := 1; iter <= iters; iter++ {
		if err := runDatabaseSQLReproIteration(t, rowsPerWorker, workers); err != nil {
			t.Fatalf("database/sql control failed on iteration=%d: %v", iter, err)
		}
		t.Logf("database/sql control iteration %d/%d passed", iter, iters)
	}
}
