package goflightbench

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func rawAddr() string {
	return fmt.Sprintf("%s:%d", *flagHost, *flagPort)
}

func newRawClient(ctx context.Context) (*flightsql.Client, error) {
	return flightsql.NewClientCtx(
		ctx,
		rawAddr(),
		nil,
		nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
}

func openRawClient(t *testing.T) *flightsql.Client {
	t.Helper()
	requirePositiveFlag(t, *flagPort, "port")

	ctx := context.Background()
	client, err := newRawClient(ctx)
	if err != nil {
		t.Fatalf("raw flightsql.NewClient failed: %v", err)
	}

	got, err := rawQueryInt64(ctx, client, "SELECT 1")
	if err != nil {
		_ = client.Close()
		t.Fatalf("raw SELECT 1 failed: %v", err)
	}
	if got != 1 {
		_ = client.Close()
		t.Fatalf("raw ping result mismatch: got=%d expected=1", got)
	}
	return client
}

func rawValueAt(col arrow.Array, row int) (any, error) {
	if col.IsNull(row) {
		return nil, nil
	}
	switch arr := col.(type) {
	case *array.Int64:
		return arr.Value(row), nil
	case *array.Int32:
		return int64(arr.Value(row)), nil
	case *array.Int16:
		return int64(arr.Value(row)), nil
	case *array.Int8:
		return int64(arr.Value(row)), nil
	case *array.Uint64:
		v := arr.Value(row)
		if v > math.MaxInt64 {
			return nil, fmt.Errorf("uint64 value out of int64 range: %d", v)
		}
		return int64(v), nil
	case *array.Uint32:
		return int64(arr.Value(row)), nil
	case *array.Uint16:
		return int64(arr.Value(row)), nil
	case *array.Uint8:
		return int64(arr.Value(row)), nil
	case *array.Float64:
		return arr.Value(row), nil
	case *array.Float32:
		return float64(arr.Value(row)), nil
	case *array.String:
		return arr.Value(row), nil
	case *array.LargeString:
		return arr.Value(row), nil
	case *array.Boolean:
		return arr.Value(row), nil
	default:
		return nil, fmt.Errorf("unsupported arrow column type: %T", col)
	}
}

func rawRowsFromInfo(ctx context.Context, client *flightsql.Client, info *flight.FlightInfo) ([][]any, error) {
	if info == nil {
		return nil, fmt.Errorf("nil flight info")
	}
	rows := make([][]any, 0)
	for endpointIdx, endpoint := range info.Endpoint {
		if endpoint == nil || endpoint.Ticket == nil {
			return nil, fmt.Errorf("endpoint %d has no ticket", endpointIdx)
		}
		rdr, err := client.DoGet(ctx, endpoint.Ticket)
		if err != nil {
			return nil, fmt.Errorf("DoGet endpoint %d failed: %w", endpointIdx, err)
		}

		for rdr.Next() {
			rec := rdr.RecordBatch()
			cols := rec.Columns()
			for row := 0; row < int(rec.NumRows()); row++ {
				rowValues := make([]any, len(cols))
				for colIdx, col := range cols {
					v, err := rawValueAt(col, row)
					if err != nil {
						rdr.Release()
						return nil, fmt.Errorf("decode row=%d col=%d failed: %w", row, colIdx, err)
					}
					rowValues[colIdx] = v
				}
				rows = append(rows, rowValues)
			}
		}
		if err := rdr.Err(); err != nil {
			rdr.Release()
			return nil, fmt.Errorf("reader iteration failed on endpoint %d: %w", endpointIdx, err)
		}
		rdr.Release()
	}
	return rows, nil
}

func rawQueryRows(ctx context.Context, client *flightsql.Client, query string) ([][]any, error) {
	info, err := client.Execute(ctx, query)
	if err != nil {
		return nil, err
	}
	return rawRowsFromInfo(ctx, client, info)
}

func rawQueryInt64(ctx context.Context, client *flightsql.Client, query string) (int64, error) {
	rows, err := rawQueryRows(ctx, client, query)
	if err != nil {
		return 0, err
	}
	if len(rows) != 1 || len(rows[0]) < 1 {
		return 0, fmt.Errorf("expected 1 row 1 col, got rows=%d", len(rows))
	}
	return toInt64(rows[0][0], "scalar")
}

func rawQueryInt64Tx(ctx context.Context, client *flightsql.Client, tx *flightsql.Txn, query string) (int64, error) {
	info, err := tx.Execute(ctx, query)
	if err != nil {
		return 0, err
	}
	rows, err := rawRowsFromInfo(ctx, client, info)
	if err != nil {
		return 0, err
	}
	if len(rows) != 1 || len(rows[0]) < 1 {
		return 0, fmt.Errorf("expected 1 row 1 col, got rows=%d", len(rows))
	}
	return toInt64(rows[0][0], "scalar")
}

func rawQueryTwoInt64Tx(ctx context.Context, client *flightsql.Client, tx *flightsql.Txn, query string) (int64, int64, error) {
	info, err := tx.Execute(ctx, query)
	if err != nil {
		return 0, 0, err
	}
	rows, err := rawRowsFromInfo(ctx, client, info)
	if err != nil {
		return 0, 0, err
	}
	if len(rows) != 1 || len(rows[0]) < 2 {
		return 0, 0, fmt.Errorf("expected 1 row 2 cols, got rows=%d", len(rows))
	}
	v1, err := toInt64(rows[0][0], "first")
	if err != nil {
		return 0, 0, err
	}
	v2, err := toInt64(rows[0][1], "second")
	if err != nil {
		return 0, 0, err
	}
	return v1, v2, nil
}

func rawExecuteUpdate(ctx context.Context, client *flightsql.Client, query string) (int64, error) {
	return client.ExecuteUpdate(ctx, query)
}

func rawExecuteUpdateTx(ctx context.Context, tx *flightsql.Txn, query string) (int64, error) {
	return tx.ExecuteUpdate(ctx, query)
}

func rawQuoteString(v string) string {
	return "'" + strings.ReplaceAll(v, "'", "''") + "'"
}

func rawInferParamType(values [][]any, col int) (arrow.DataType, error) {
	for row := 0; row < len(values); row++ {
		v := values[row][col]
		if v == nil {
			continue
		}
		switch v.(type) {
		case int64, int32, int16, int8, int, uint64, uint32, uint16, uint8, uint:
			return arrow.PrimitiveTypes.Int64, nil
		case string:
			return arrow.BinaryTypes.String, nil
		case bool:
			return arrow.FixedWidthTypes.Boolean, nil
		case float64, float32:
			return arrow.PrimitiveTypes.Float64, nil
		default:
			return nil, fmt.Errorf("unsupported parameter type at col=%d: %T", col, v)
		}
	}
	// Default to int64 if all values are NULL.
	return arrow.PrimitiveTypes.Int64, nil
}

func rawAppendParamValue(builder array.Builder, value any) error {
	if value == nil {
		builder.AppendNull()
		return nil
	}

	switch b := builder.(type) {
	case *array.Int64Builder:
		v, err := toInt64(value, "param")
		if err != nil {
			return err
		}
		b.Append(v)
		return nil
	case *array.StringBuilder:
		switch s := value.(type) {
		case string:
			b.Append(s)
			return nil
		default:
			return fmt.Errorf("expected string value, got %T", value)
		}
	case *array.BooleanBuilder:
		v, ok := value.(bool)
		if !ok {
			return fmt.Errorf("expected bool value, got %T", value)
		}
		b.Append(v)
		return nil
	case *array.Float64Builder:
		switch f := value.(type) {
		case float64:
			b.Append(f)
			return nil
		case float32:
			b.Append(float64(f))
			return nil
		default:
			return fmt.Errorf("expected float value, got %T", value)
		}
	default:
		return fmt.Errorf("unsupported builder type: %T", builder)
	}
}

func rawBuildParamRecord(values [][]any) (arrow.RecordBatch, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("parameter rows must not be empty")
	}
	colCount := len(values[0])
	for i := 1; i < len(values); i++ {
		if len(values[i]) != colCount {
			return nil, fmt.Errorf("parameter row width mismatch at row=%d: got=%d expected=%d", i, len(values[i]), colCount)
		}
	}
	fields := make([]arrow.Field, colCount)
	for col := 0; col < colCount; col++ {
		dt, err := rawInferParamType(values, col)
		if err != nil {
			return nil, err
		}
		fields[col] = arrow.Field{
			Name:     fmt.Sprintf("parameter_%d", col+1),
			Type:     dt,
			Nullable: true,
		}
	}
	schema := arrow.NewSchema(fields, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	for row := 0; row < len(values); row++ {
		for col := 0; col < colCount; col++ {
			if err := rawAppendParamValue(builder.Field(col), values[row][col]); err != nil {
				return nil, err
			}
		}
	}
	return builder.NewRecordBatch(), nil
}

func rawSetPreparedParameters(stmt *flightsql.PreparedStatement, values [][]any) error {
	if len(values) == 0 {
		stmt.SetParameters(nil)
		return nil
	}
	rec, err := rawBuildParamRecord(values)
	if err != nil {
		return err
	}
	stmt.SetParameters(rec)
	rec.Release()
	return nil
}

func rawExecutePreparedQueryInt64(ctx context.Context, client *flightsql.Client, stmt *flightsql.PreparedStatement, values [][]any) (int64, error) {
	if err := rawSetPreparedParameters(stmt, values); err != nil {
		return 0, err
	}
	info, err := stmt.Execute(ctx)
	if err != nil {
		return 0, err
	}
	rows, err := rawRowsFromInfo(ctx, client, info)
	if err != nil {
		return 0, err
	}
	if len(rows) != 1 || len(rows[0]) < 1 {
		return 0, fmt.Errorf("expected scalar result, got rows=%d", len(rows))
	}
	return toInt64(rows[0][0], "prepared_scalar")
}

func rawBuildBatchInsertSQL(startID int64, size int) string {
	var builder strings.Builder
	builder.WriteString("INSERT INTO ")
	builder.WriteString(benchmarkTable)
	builder.WriteString(" (id, val) VALUES ")
	for i := 0; i < size; i++ {
		if i > 0 {
			builder.WriteString(", ")
		}
		id := startID + int64(i)
		builder.WriteString("(")
		builder.WriteString(strconv.FormatInt(id, 10))
		builder.WriteString(", ")
		builder.WriteString(strconv.FormatInt(expectedVal(id), 10))
		builder.WriteString(")")
	}
	return builder.String()
}

func rawResetBenchmarkTable(ctx context.Context, client *flightsql.Client) error {
	if _, err := rawExecuteUpdate(ctx, client, "DROP TABLE IF EXISTS "+benchmarkTable); err != nil {
		return err
	}
	_, err := rawExecuteUpdate(ctx, client, "CREATE TABLE "+benchmarkTable+" (id BIGINT PRIMARY KEY, val BIGINT)")
	return err
}

func rawResetTransactionBenchmarkTable(ctx context.Context, client *flightsql.Client) error {
	if _, err := rawExecuteUpdate(ctx, client, "DROP TABLE IF EXISTS "+txBenchmarkTable); err != nil {
		return err
	}
	_, err := rawExecuteUpdate(
		ctx,
		client,
		"CREATE TABLE "+txBenchmarkTable+" (id BIGINT PRIMARY KEY, worker_id BIGINT, val BIGINT)",
	)
	return err
}

func rawResetTransactionConflictBenchmarkTable(ctx context.Context, client *flightsql.Client) error {
	if _, err := rawExecuteUpdate(ctx, client, "DROP TABLE IF EXISTS "+txConflictBenchmarkTable); err != nil {
		return err
	}
	_, err := rawExecuteUpdate(
		ctx,
		client,
		"CREATE TABLE "+txConflictBenchmarkTable+" (id BIGINT PRIMARY KEY, worker_id BIGINT, attempt BIGINT)",
	)
	return err
}

func rawVerifyAggregateTableState(t *testing.T, ctx context.Context, client *flightsql.Client, expectedRows int64) {
	t.Helper()
	rows, err := rawQueryRows(
		ctx,
		client,
		"SELECT "+
			"CAST(COUNT(*) AS BIGINT), "+
			"CAST(COUNT(DISTINCT id) AS BIGINT), "+
			"CAST(COALESCE(MIN(id), 0) AS BIGINT), "+
			"CAST(COALESCE(MAX(id), 0) AS BIGINT), "+
			"CAST(COALESCE(SUM(val), 0) AS BIGINT) "+
			"FROM "+benchmarkTable,
	)
	if err != nil {
		t.Fatalf("aggregate verification query failed: %v", err)
	}
	if len(rows) != 1 || len(rows[0]) < 5 {
		t.Fatalf("aggregate query returned unexpected shape: rows=%d", len(rows))
	}

	gotCount, err := toInt64(rows[0][0], "count")
	if err != nil {
		t.Fatalf("aggregate conversion failed for count: %v", err)
	}
	gotDistinct, err := toInt64(rows[0][1], "count_distinct")
	if err != nil {
		t.Fatalf("aggregate conversion failed for count_distinct: %v", err)
	}
	gotMin, err := toInt64(rows[0][2], "min")
	if err != nil {
		t.Fatalf("aggregate conversion failed for min: %v", err)
	}
	gotMax, err := toInt64(rows[0][3], "max")
	if err != nil {
		t.Fatalf("aggregate conversion failed for max: %v", err)
	}
	gotSum, err := toInt64(rows[0][4], "sum")
	if err != nil {
		t.Fatalf("aggregate conversion failed for sum: %v", err)
	}

	if gotCount != expectedRows {
		t.Fatalf("row count mismatch: got=%d expected=%d", gotCount, expectedRows)
	}
	if gotDistinct != expectedRows {
		t.Fatalf("distinct id count mismatch: got=%d expected=%d", gotDistinct, expectedRows)
	}
	if expectedRows > 0 && (gotMin != 1 || gotMax != expectedRows) {
		t.Fatalf("id bounds mismatch: min=%d max=%d expected_min=1 expected_max=%d", gotMin, gotMax, expectedRows)
	}
	if gotSum != expectedSum(expectedRows) {
		t.Fatalf("sum(val) mismatch: got=%d expected=%d", gotSum, expectedSum(expectedRows))
	}
}

func runRawCrudSingleOpAutocommit(t *testing.T, client *flightsql.Client, iters int) {
	t.Helper()
	requirePositiveFlag(t, iters, "crud-iters")
	ctx := context.Background()

	printPhaseStart("RAW_CRUD_SINGLE_OP_AUTOCOMMIT")
	if _, err := rawExecuteUpdate(ctx, client, "DROP TABLE IF EXISTS "+crudTable); err != nil {
		t.Fatalf("cleanup drop failed: %v", err)
	}

	createStart := time.Now()
	if _, err := rawExecuteUpdate(ctx, client, "CREATE TABLE "+crudTable+" (id BIGINT PRIMARY KEY, val BIGINT)"); err != nil {
		t.Fatalf("create table failed: %v", err)
	}
	createDuration := time.Since(createStart)
	printOpMetric("raw_crud_create_table", createDuration, 1)

	var insertTotal time.Duration
	var updateTotal time.Duration
	var selectTotal time.Duration
	var deleteTotal time.Duration

	for i := 1; i <= iters; i++ {
		id := int64(i)
		baseVal := expectedVal(id)

		start := time.Now()
		if _, err := rawExecuteUpdate(
			ctx,
			client,
			fmt.Sprintf("INSERT INTO %s VALUES (%d, %d)", crudTable, id, baseVal),
		); err != nil {
			t.Fatalf("insert failed at id=%d: %v", id, err)
		}
		insertTotal += time.Since(start)

		start = time.Now()
		affected, err := rawExecuteUpdate(
			ctx,
			client,
			fmt.Sprintf("UPDATE %s SET val = val + 1 WHERE id = %d", crudTable, id),
		)
		if err != nil {
			t.Fatalf("update failed at id=%d: %v", id, err)
		}
		if affected != 1 {
			t.Fatalf("update affected rows mismatch for id=%d: got=%d", id, affected)
		}
		updateTotal += time.Since(start)

		start = time.Now()
		gotVal, err := rawQueryInt64(ctx, client, fmt.Sprintf("SELECT val FROM %s WHERE id = %d", crudTable, id))
		if err != nil {
			t.Fatalf("select failed at id=%d: %v", id, err)
		}
		if gotVal != baseVal+1 {
			t.Fatalf("selected value mismatch at id=%d: got=%d expected=%d", id, gotVal, baseVal+1)
		}
		selectTotal += time.Since(start)

		start = time.Now()
		affected, err = rawExecuteUpdate(
			ctx,
			client,
			fmt.Sprintf("DELETE FROM %s WHERE id = %d", crudTable, id),
		)
		if err != nil {
			t.Fatalf("delete failed at id=%d: %v", id, err)
		}
		if affected != 1 {
			t.Fatalf("delete affected rows mismatch for id=%d: got=%d", id, affected)
		}
		deleteTotal += time.Since(start)
	}

	dropStart := time.Now()
	if _, err := rawExecuteUpdate(ctx, client, "DROP TABLE "+crudTable); err != nil {
		t.Fatalf("drop table failed: %v", err)
	}
	dropDuration := time.Since(dropStart)
	printOpMetric("raw_crud_insert_autocommit", insertTotal, int64(iters))
	printOpMetric("raw_crud_update_autocommit", updateTotal, int64(iters))
	printOpMetric("raw_crud_select_point", selectTotal, int64(iters))
	printOpMetric("raw_crud_delete_autocommit", deleteTotal, int64(iters))
	printOpMetric("raw_crud_drop_table", dropDuration, 1)
}

func runRawBatchInsert(t *testing.T, client *flightsql.Client, rows, batchSize int) {
	t.Helper()
	requirePositiveFlag(t, rows, "rows")
	requirePositiveFlag(t, batchSize, "batch-size")
	ctx := context.Background()

	printPhaseStart("RAW_BATCH_INSERT_100K")
	if err := rawResetBenchmarkTable(ctx, client); err != nil {
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
		sqlText := rawBuildBatchInsertSQL(int64(start), size)
		if _, err := rawExecuteUpdate(ctx, client, sqlText); err != nil {
			t.Fatalf("batch insert failed at start=%d size=%d: %v", start, size, err)
		}
		batchCount++
	}
	duration := time.Since(phaseStart)
	rawVerifyAggregateTableState(t, ctx, client, int64(rows))
	printRowMetric("raw_batch_insert_rows", duration, int64(rows))
	printOpMetric("raw_batch_insert_statements", duration, int64(batchCount))
}

func runRawConcurrentInsert(t *testing.T, client *flightsql.Client, rows, workers int) {
	t.Helper()
	requirePositiveFlag(t, rows, "rows")
	requirePositiveFlag(t, workers, "workers")
	ctx := context.Background()

	printPhaseStart("RAW_CONCURRENT_INSERT_200_TX_PREPARED")
	if err := rawResetBenchmarkTable(ctx, client); err != nil {
		t.Fatalf("reset benchmark table failed: %v", err)
	}

	ranges := partitionRanges(rows, workers)
	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	start := time.Now()

	for workerID, rg := range ranges {
		if rg.end < rg.start {
			continue
		}
		wg.Add(1)
		go func(worker int, r idRange) {
			defer wg.Done()
			ctx := context.Background()

			workerClient, err := newRawClient(ctx)
			if err != nil {
				errCh <- fmt.Errorf("worker=%d create client failed: %w", worker, err)
				return
			}
			defer workerClient.Close()

			tx, err := workerClient.BeginTransaction(ctx)
			if err != nil {
				errCh <- fmt.Errorf("worker=%d begin tx failed: %w", worker, err)
				return
			}

			stmt, err := tx.Prepare(ctx, "INSERT INTO "+benchmarkTable+" VALUES (?, ?)")
			if err != nil {
				_ = tx.Rollback(ctx)
				errCh <- fmt.Errorf("worker=%d prepare insert failed: %w", worker, err)
				return
			}
			defer stmt.Close(ctx)

			for id := r.start; id <= r.end; id++ {
				if err := rawSetPreparedParameters(stmt, [][]any{{id, expectedVal(id)}}); err != nil {
					_ = tx.Rollback(ctx)
					errCh <- fmt.Errorf("worker=%d id=%d bind failed: %w", worker, id, err)
					return
				}
				affected, err := stmt.ExecuteUpdate(ctx)
				if err != nil {
					_ = tx.Rollback(ctx)
					errCh <- fmt.Errorf("worker=%d id=%d insert failed: %w", worker, id, err)
					return
				}
				if affected != 1 {
					_ = tx.Rollback(ctx)
					errCh <- fmt.Errorf("worker=%d id=%d affected mismatch: got=%d expected=1", worker, id, affected)
					return
				}
			}

			if err := tx.Commit(ctx); err != nil {
				errCh <- fmt.Errorf("worker=%d commit failed: %w", worker, err)
				return
			}
		}(workerID, rg)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("raw concurrent insert failed: %v", err)
		}
	}

	duration := time.Since(start)
	rawVerifyAggregateTableState(t, ctx, client, int64(rows))
	printRowMetric("raw_concurrent_insert_rows", duration, int64(rows))
	printOpMetric("raw_concurrent_insert_tx_prepared_ops", duration, int64(rows))
}

func runRawSelectPrepareModes(t *testing.T, client *flightsql.Client, rows, iters int) {
	t.Helper()
	requirePositiveFlag(t, rows, "rows")
	requirePositiveFlag(t, iters, "select-iters")
	ctx := context.Background()

	printPhaseStart("RAW_SELECT_PREPARE_MODES")

	start := time.Now()
	for i := 0; i < iters; i++ {
		id := int64(i%rows) + 1
		got, err := rawQueryInt64(ctx, client, fmt.Sprintf("SELECT val FROM %s WHERE id = %d", benchmarkTable, id))
		if err != nil {
			t.Fatalf("raw select direct failed at iter=%d id=%d: %v", i, id, err)
		}
		if got != expectedVal(id) {
			t.Fatalf("raw select direct mismatch at iter=%d id=%d: got=%d expected=%d", i, id, got, expectedVal(id))
		}
	}
	directDuration := time.Since(start)
	printOpMetric("raw_select_point_direct_no_prepare", directDuration, int64(iters))

	start = time.Now()
	for i := 0; i < iters; i++ {
		id := int64(i%rows) + 1
		stmt, err := client.Prepare(ctx, "SELECT val FROM "+benchmarkTable+" WHERE id = ?")
		if err != nil {
			t.Fatalf("raw select prepare-each prepare failed at iter=%d: %v", i, err)
		}
		got, err := rawExecutePreparedQueryInt64(ctx, client, stmt, [][]any{{id}})
		closeErr := stmt.Close(ctx)
		if err != nil {
			t.Fatalf("raw select prepare-each query failed at iter=%d id=%d: %v", i, id, err)
		}
		if closeErr != nil {
			t.Fatalf("raw select prepare-each close failed at iter=%d: %v", i, closeErr)
		}
		if got != expectedVal(id) {
			t.Fatalf("raw select prepare-each mismatch at iter=%d id=%d: got=%d expected=%d", i, id, got, expectedVal(id))
		}
	}
	prepareEachDuration := time.Since(start)
	printOpMetric("raw_select_point_prepare_each_time", prepareEachDuration, int64(iters))

	stmt, err := client.Prepare(ctx, "SELECT val FROM "+benchmarkTable+" WHERE id = ?")
	if err != nil {
		t.Fatalf("raw select prepare-once prepare failed: %v", err)
	}
	start = time.Now()
	for i := 0; i < iters; i++ {
		id := int64(i%rows) + 1
		got, err := rawExecutePreparedQueryInt64(ctx, client, stmt, [][]any{{id}})
		if err != nil {
			_ = stmt.Close(ctx)
			t.Fatalf("raw select prepare-once query failed at iter=%d id=%d: %v", i, id, err)
		}
		if got != expectedVal(id) {
			_ = stmt.Close(ctx)
			t.Fatalf("raw select prepare-once mismatch at iter=%d id=%d: got=%d expected=%d", i, id, got, expectedVal(id))
		}
	}
	prepareReuseDuration := time.Since(start)
	if err := stmt.Close(ctx); err != nil {
		t.Fatalf("raw select prepare-once close failed: %v", err)
	}
	printOpMetric("raw_select_point_prepare_once_reuse", prepareReuseDuration, int64(iters))
}

func runRawConcurrentSelectMode(t *testing.T, rows, workers, iters int, metricName string,
	runWorker func(context.Context, *flightsql.Client, int, int) error) {
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
			client, err := newRawClient(ctx)
			if err != nil {
				errCh <- fmt.Errorf("worker=%d create client failed: %w", workerID, err)
				return
			}
			defer client.Close()

			if err := runWorker(ctx, client, begin, end); err != nil {
				errCh <- fmt.Errorf("worker=%d %s failed: %w", workerID, metricName, err)
				return
			}
		}(worker, startIter, endIter)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("raw concurrent select mode failed: %v", err)
		}
	}
	duration := time.Since(start)
	printOpMetric(metricName, duration, int64(iters))
}

func runRawSelectPrepareModesConcurrent(t *testing.T, rows, workers, iters int) {
	t.Helper()
	requirePositiveFlag(t, rows, "rows")
	requirePositiveFlag(t, workers, "workers")
	requirePositiveFlag(t, iters, "select-iters")

	printPhaseStart("RAW_SELECT_PREPARE_MODES_CONCURRENT")

	runRawConcurrentSelectMode(t, rows, workers, iters, "raw_select_point_concurrent_direct_no_prepare",
		func(ctx context.Context, client *flightsql.Client, begin, end int) error {
			for i := begin; i < end; i++ {
				id := int64(i%rows) + 1
				got, err := rawQueryInt64(ctx, client, fmt.Sprintf("SELECT val FROM %s WHERE id = %d", benchmarkTable, id))
				if err != nil {
					return fmt.Errorf("iter=%d id=%d query failed: %w", i, id, err)
				}
				if got != expectedVal(id) {
					return fmt.Errorf("iter=%d id=%d mismatch: got=%d expected=%d", i, id, got, expectedVal(id))
				}
			}
			return nil
		},
	)

	runRawConcurrentSelectMode(t, rows, workers, iters, "raw_select_point_concurrent_prepare_each_time",
		func(ctx context.Context, client *flightsql.Client, begin, end int) error {
			for i := begin; i < end; i++ {
				id := int64(i%rows) + 1
				stmt, err := client.Prepare(ctx, "SELECT val FROM "+benchmarkTable+" WHERE id = ?")
				if err != nil {
					return fmt.Errorf("iter=%d prepare failed: %w", i, err)
				}
				got, execErr := rawExecutePreparedQueryInt64(ctx, client, stmt, [][]any{{id}})
				closeErr := stmt.Close(ctx)
				if execErr != nil {
					return fmt.Errorf("iter=%d id=%d query failed: %w", i, id, execErr)
				}
				if closeErr != nil {
					return fmt.Errorf("iter=%d close failed: %w", i, closeErr)
				}
				if got != expectedVal(id) {
					return fmt.Errorf("iter=%d id=%d mismatch: got=%d expected=%d", i, id, got, expectedVal(id))
				}
			}
			return nil
		},
	)

	runRawConcurrentSelectMode(t, rows, workers, iters, "raw_select_point_concurrent_prepare_once_reuse",
		func(ctx context.Context, client *flightsql.Client, begin, end int) error {
			stmt, err := client.Prepare(ctx, "SELECT val FROM "+benchmarkTable+" WHERE id = ?")
			if err != nil {
				return fmt.Errorf("prepare once failed: %w", err)
			}
			defer stmt.Close(ctx)

			for i := begin; i < end; i++ {
				id := int64(i%rows) + 1
				got, err := rawExecutePreparedQueryInt64(ctx, client, stmt, [][]any{{id}})
				if err != nil {
					return fmt.Errorf("iter=%d id=%d query failed: %w", i, id, err)
				}
				if got != expectedVal(id) {
					return fmt.Errorf("iter=%d id=%d mismatch: got=%d expected=%d", i, id, got, expectedVal(id))
				}
			}
			return nil
		},
	)
}

func runRawConcurrentTransactionCommitRollback(t *testing.T, client *flightsql.Client, rows, workers int) {
	t.Helper()
	requirePositiveFlag(t, rows, "rows")
	requirePositiveFlag(t, workers, "workers")
	ctx := context.Background()

	printPhaseStart("RAW_CONCURRENT_TRANSACTIONS_COMMIT_ROLLBACK")
	if err := rawResetTransactionBenchmarkTable(ctx, client); err != nil {
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

			workerClient, err := newRawClient(ctx)
			if err != nil {
				errCh <- fmt.Errorf("worker=%d create client failed: %w", worker, err)
				return
			}
			defer workerClient.Close()

			tx, err := workerClient.BeginTransaction(ctx)
			if err != nil {
				errCh <- fmt.Errorf("worker=%d begin tx failed: %w", worker, err)
				return
			}

			stmt, err := tx.Prepare(ctx, "INSERT INTO "+txBenchmarkTable+" VALUES (?, ?, ?)")
			if err != nil {
				_ = tx.Rollback(ctx)
				errCh <- fmt.Errorf("worker=%d prepare insert failed: %w", worker, err)
				return
			}
			defer stmt.Close(ctx)

			var localRows int64
			var localSum int64
			for id := r.start; id <= r.end; id++ {
				val := expectedVal(id)
				if err := rawSetPreparedParameters(stmt, [][]any{{id, int64(worker), val}}); err != nil {
					_ = tx.Rollback(ctx)
					errCh <- fmt.Errorf("worker=%d bind failed at id=%d: %w", worker, id, err)
					return
				}
				affected, err := stmt.ExecuteUpdate(ctx)
				if err != nil {
					_ = tx.Rollback(ctx)
					errCh <- fmt.Errorf("worker=%d insert failed at id=%d: %w", worker, id, err)
					return
				}
				if affected != 1 {
					_ = tx.Rollback(ctx)
					errCh <- fmt.Errorf("worker=%d insert affected mismatch at id=%d: got=%d expected=1", worker, id, affected)
					return
				}
				localRows++
				localSum += val
			}

			inTxCount, err := rawQueryInt64Tx(
				ctx,
				workerClient,
				tx,
				fmt.Sprintf("SELECT CAST(COUNT(*) AS BIGINT) FROM %s WHERE worker_id = %d", txBenchmarkTable, worker),
			)
			if err != nil {
				_ = tx.Rollback(ctx)
				errCh <- fmt.Errorf("worker=%d in-tx count query failed: %w", worker, err)
				return
			}
			if inTxCount != localRows {
				_ = tx.Rollback(ctx)
				errCh <- fmt.Errorf("worker=%d in-tx count mismatch: got=%d expected=%d", worker, inTxCount, localRows)
				return
			}

			outsideCount, err := rawQueryInt64(
				ctx,
				client,
				fmt.Sprintf("SELECT CAST(COUNT(*) AS BIGINT) FROM %s WHERE worker_id = %d", txBenchmarkTable, worker),
			)
			if err != nil {
				_ = tx.Rollback(ctx)
				errCh <- fmt.Errorf("worker=%d outside count query failed: %w", worker, err)
				return
			}
			if outsideCount != 0 {
				_ = tx.Rollback(ctx)
				errCh <- fmt.Errorf("worker=%d uncommitted rows visible outside tx: got=%d", worker, outsideCount)
				return
			}

			shouldCommit := worker%2 == 0
			if shouldCommit {
				if err := tx.Commit(ctx); err != nil {
					errCh <- fmt.Errorf("worker=%d commit failed: %w", worker, err)
					return
				}
				committedRows.Add(localRows)
				committedSum.Add(localSum)
			} else {
				if err := tx.Rollback(ctx); err != nil {
					errCh <- fmt.Errorf("worker=%d rollback failed: %w", worker, err)
					return
				}
				rolledBackRows.Add(localRows)
			}

			afterCount, err := rawQueryInt64(
				ctx,
				client,
				fmt.Sprintf("SELECT CAST(COUNT(*) AS BIGINT) FROM %s WHERE worker_id = %d", txBenchmarkTable, worker),
			)
			if err != nil {
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
			t.Fatalf("raw concurrent transaction benchmark failed: %v", err)
		}
	}

	expectedCommittedRows := committedRows.Load()
	expectedRolledBackRows := rolledBackRows.Load()
	rowsOut, err := rawQueryRows(
		ctx,
		client,
		"SELECT "+
			"CAST(COUNT(*) AS BIGINT), "+
			"CAST(COUNT(DISTINCT id) AS BIGINT), "+
			"CAST(COALESCE(SUM(val), 0) AS BIGINT) "+
			"FROM "+txBenchmarkTable,
	)
	if err != nil {
		t.Fatalf("final transaction aggregate query failed: %v", err)
	}
	if len(rowsOut) != 1 || len(rowsOut[0]) < 3 {
		t.Fatalf("final aggregate unexpected shape: rows=%d", len(rowsOut))
	}
	gotCount, err := toInt64(rowsOut[0][0], "count")
	if err != nil {
		t.Fatalf("final transaction aggregate conversion failed for count: %v", err)
	}
	gotDistinct, err := toInt64(rowsOut[0][1], "count_distinct")
	if err != nil {
		t.Fatalf("final transaction aggregate conversion failed for distinct: %v", err)
	}
	gotSum, err := toInt64(rowsOut[0][2], "sum")
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
	printOpMetric("raw_concurrent_transactions_total", duration, txnCount)
	printRowMetric("raw_concurrent_transactions_committed_rows", duration, expectedCommittedRows)
	printRowMetric("raw_concurrent_transactions_rolledback_rows", duration, expectedRolledBackRows)
}

func runRawConcurrentTransactionCreateDropInsertSelect(t *testing.T, client *flightsql.Client, rows, workers int) {
	t.Helper()
	requirePositiveFlag(t, rows, "rows")
	requirePositiveFlag(t, workers, "workers")
	ctx := context.Background()

	printPhaseStart("RAW_CONCURRENT_TRANSACTIONS_DDL_DML")
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

			workerClient, err := newRawClient(ctx)
			if err != nil {
				errCh <- fmt.Errorf("worker=%d create client failed: %w", worker, err)
				return
			}
			defer workerClient.Close()

			tx, err := workerClient.BeginTransaction(ctx)
			if err != nil {
				errCh <- fmt.Errorf("worker=%d begin tx failed: %w", worker, err)
				return
			}

			if _, err := rawExecuteUpdateTx(ctx, tx, "CREATE TABLE "+tableName+" (id BIGINT PRIMARY KEY, val BIGINT)"); err != nil {
				_ = tx.Rollback(ctx)
				errCh <- fmt.Errorf("worker=%d create table failed: %w", worker, err)
				return
			}

			if _, err := rawQueryInt64(ctx, client, "SELECT CAST(COUNT(*) AS BIGINT) FROM "+tableName); err == nil {
				_ = tx.Rollback(ctx)
				errCh <- fmt.Errorf("worker=%d outside query unexpectedly saw uncommitted table", worker)
				return
			} else if !isExpectedMissingTableError(err) {
				_ = tx.Rollback(ctx)
				errCh <- fmt.Errorf("worker=%d outside query returned unexpected error for uncommitted table: %w", worker, err)
				return
			}

			stmt, err := tx.Prepare(ctx, "INSERT INTO "+tableName+" VALUES (?, ?)")
			if err != nil {
				_ = tx.Rollback(ctx)
				errCh <- fmt.Errorf("worker=%d prepare insert failed: %w", worker, err)
				return
			}
			defer stmt.Close(ctx)

			var localRows int64
			var localSum int64
			for id := r.start; id <= r.end; id++ {
				val := expectedVal(id)
				if err := rawSetPreparedParameters(stmt, [][]any{{id, val}}); err != nil {
					_ = tx.Rollback(ctx)
					errCh <- fmt.Errorf("worker=%d bind failed at id=%d: %w", worker, id, err)
					return
				}
				affected, err := stmt.ExecuteUpdate(ctx)
				if err != nil {
					_ = tx.Rollback(ctx)
					errCh <- fmt.Errorf("worker=%d insert failed at id=%d: %w", worker, id, err)
					return
				}
				if affected != 1 {
					_ = tx.Rollback(ctx)
					errCh <- fmt.Errorf("worker=%d insert affected mismatch at id=%d: got=%d expected=1", worker, id, affected)
					return
				}
				localRows++
				localSum += val
			}

			inTxCount, inTxSum, err := rawQueryTwoInt64Tx(
				ctx,
				workerClient,
				tx,
				"SELECT CAST(COUNT(*) AS BIGINT), CAST(COALESCE(SUM(val), 0) AS BIGINT) FROM "+tableName,
			)
			if err != nil {
				_ = tx.Rollback(ctx)
				errCh <- fmt.Errorf("worker=%d in-tx select aggregate failed: %w", worker, err)
				return
			}
			if inTxCount != localRows {
				_ = tx.Rollback(ctx)
				errCh <- fmt.Errorf("worker=%d in-tx row count mismatch: got=%d expected=%d", worker, inTxCount, localRows)
				return
			}
			if inTxSum != localSum {
				_ = tx.Rollback(ctx)
				errCh <- fmt.Errorf("worker=%d in-tx sum mismatch: got=%d expected=%d", worker, inTxSum, localSum)
				return
			}

			if _, err := rawExecuteUpdateTx(ctx, tx, "DROP TABLE "+tableName); err != nil {
				_ = tx.Rollback(ctx)
				errCh <- fmt.Errorf("worker=%d drop table failed: %w", worker, err)
				return
			}

			if _, err := rawQueryInt64Tx(ctx, workerClient, tx, "SELECT CAST(COUNT(*) AS BIGINT) FROM "+tableName); err == nil {
				_ = tx.Rollback(ctx)
				errCh <- fmt.Errorf("worker=%d dropped table still queryable inside tx", worker)
				return
			} else if !isExpectedMissingTableError(err) {
				_ = tx.Rollback(ctx)
				errCh <- fmt.Errorf("worker=%d unexpected error after drop inside tx: %w", worker, err)
				return
			}

			if err := tx.Commit(ctx); err != nil {
				errCh <- fmt.Errorf("worker=%d commit failed: %w", worker, err)
				return
			}

			if _, err := rawQueryInt64(ctx, client, "SELECT CAST(COUNT(*) AS BIGINT) FROM "+tableName); err == nil {
				errCh <- fmt.Errorf("worker=%d table exists after commit despite drop", worker)
				return
			} else if !isExpectedMissingTableError(err) {
				errCh <- fmt.Errorf("worker=%d unexpected post-commit missing-table error: %w", worker, err)
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
			t.Fatalf("raw concurrent transaction DDL/DML benchmark failed: %v", err)
		}
	}

	leftoverQuery := "SELECT CAST(COUNT(*) AS BIGINT) FROM duckdb_tables() WHERE table_name LIKE " + rawQuoteString(tablePrefix+"%")
	leftoverTables, err := rawQueryInt64(ctx, client, leftoverQuery)
	if err != nil {
		t.Fatalf("leftover table verification query failed: %v", err)
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
	printOpMetric("raw_concurrent_tx_ddl_dml_total", duration, txnCount)
	printRowMetric("raw_concurrent_tx_ddl_dml_inserted_rows", duration, insertedRows.Load())
	printRowMetric("raw_concurrent_tx_ddl_dml_selected_rows", duration, selectedRows.Load())
}

func runRawConcurrentTransactionCommitConflicts(t *testing.T, client *flightsql.Client, workers int) {
	t.Helper()
	requirePositiveFlag(t, workers, "workers")
	ctx := context.Background()

	printPhaseStart("RAW_CONCURRENT_TRANSACTIONS_COMMIT_CONFLICTS")
	if err := rawResetTransactionConflictBenchmarkTable(ctx, client); err != nil {
		t.Fatalf("reset transaction conflict benchmark table failed: %v", err)
	}

	for id := 1; id <= workers; id++ {
		if _, err := rawExecuteUpdate(
			ctx,
			client,
			fmt.Sprintf("INSERT INTO %s VALUES (%d, -1, 0)", txConflictBenchmarkTable, id),
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

				workerClient, err := newRawClient(ctx)
				if err != nil {
					errCh <- fmt.Errorf("pair=%d contender=%d create client failed: %w", pair, contenderOffset, err)
					precommitReadyWG.Done()
					return
				}
				defer workerClient.Close()

				tx, err := workerClient.BeginTransaction(ctx)
				if err != nil {
					errCh <- fmt.Errorf("pair=%d contender=%d begin tx failed: %w", pair, contenderOffset, err)
					precommitReadyWG.Done()
					return
				}

				beginReady <- struct{}{}
				<-startUpdate
				_, updateErr := rawExecuteUpdateTx(
					ctx,
					tx,
					fmt.Sprintf(
						"UPDATE %s SET worker_id = %d, attempt = attempt + 1 WHERE id = %d",
						txConflictBenchmarkTable, workerID, pairID,
					),
				)
				if updateErr != nil {
					if isExpectedConflictError(updateErr) {
						conflictCount.Add(1)
						_ = tx.Rollback(ctx)
						precommitReadyWG.Done()
						return
					}
					_ = tx.Rollback(ctx)
					errCh <- fmt.Errorf("pair=%d contender=%d update failed: %w", pair, contenderOffset, updateErr)
					precommitReadyWG.Done()
					return
				}

				inTxAttempt, err := rawQueryInt64Tx(
					ctx,
					workerClient,
					tx,
					fmt.Sprintf("SELECT CAST(attempt AS BIGINT) FROM %s WHERE id = %d", txConflictBenchmarkTable, pairID),
				)
				if err != nil {
					_ = tx.Rollback(ctx)
					errCh <- fmt.Errorf("pair=%d contender=%d in-tx attempt query failed: %w", pair, contenderOffset, err)
					precommitReadyWG.Done()
					return
				}
				if inTxAttempt != 1 {
					_ = tx.Rollback(ctx)
					errCh <- fmt.Errorf("pair=%d contender=%d in-tx attempt mismatch: got=%d expected=1", pair, contenderOffset, inTxAttempt)
					precommitReadyWG.Done()
					return
				}

				precommitReadyWG.Done()
				<-startCommit
				if err := tx.Commit(ctx); err != nil {
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

			outRows, err := rawQueryRows(
				ctx,
				client,
				fmt.Sprintf(
					"SELECT CAST(attempt AS BIGINT), CAST(worker_id AS BIGINT) FROM %s WHERE id = %d",
					txConflictBenchmarkTable, pairID,
				),
			)
			if err != nil {
				errCh <- fmt.Errorf("pair=%d outside pre-commit query failed: %w", pair, err)
				close(startCommit)
				contenderWG.Wait()
				return
			}
			if len(outRows) != 1 || len(outRows[0]) < 2 {
				errCh <- fmt.Errorf("pair=%d outside pre-commit unexpected row shape", pair)
				close(startCommit)
				contenderWG.Wait()
				return
			}
			outsideAttempt, err := toInt64(outRows[0][0], "outside_attempt")
			if err != nil {
				errCh <- fmt.Errorf("pair=%d outside attempt conversion failed: %w", pair, err)
				close(startCommit)
				contenderWG.Wait()
				return
			}
			outsideWorker, err := toInt64(outRows[0][1], "outside_worker")
			if err != nil {
				errCh <- fmt.Errorf("pair=%d outside worker conversion failed: %w", pair, err)
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
			t.Fatalf("raw concurrent transaction commit conflict benchmark failed: %v", err)
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

	finalRows, err := rawQueryRows(
		ctx,
		client,
		"SELECT "+
			"CAST(COUNT(*) AS BIGINT), "+
			"CAST(COALESCE(SUM(attempt), 0) AS BIGINT), "+
			"CAST(COALESCE(SUM(CASE WHEN worker_id >= 0 THEN 1 ELSE 0 END), 0) AS BIGINT) "+
			"FROM "+txConflictBenchmarkTable,
	)
	if err != nil {
		t.Fatalf("final conflict aggregate query failed: %v", err)
	}
	if len(finalRows) != 1 || len(finalRows[0]) < 3 {
		t.Fatalf("final conflict aggregate unexpected shape: rows=%d", len(finalRows))
	}
	finalCount, err := toInt64(finalRows[0][0], "final_count")
	if err != nil {
		t.Fatalf("final conflict final_count conversion failed: %v", err)
	}
	finalAttemptSum, err := toInt64(finalRows[0][1], "final_attempt_sum")
	if err != nil {
		t.Fatalf("final conflict final_attempt_sum conversion failed: %v", err)
	}
	finalWinnerCount, err := toInt64(finalRows[0][2], "final_winner_count")
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
	printOpMetric("raw_concurrent_tx_conflict_total", duration, int64(workers*2))
	printOpMetric("raw_concurrent_tx_conflict_commits", duration, commits)
	printOpMetric("raw_concurrent_tx_conflict_failures", duration, conflicts)
}

func rawVerifyOrderedQuery(t *testing.T, ctx context.Context, client *flightsql.Client, query string, expectedStartID, expectedRows int64) int64 {
	t.Helper()
	info, err := client.Execute(ctx, query)
	if err != nil {
		t.Fatalf("ordered query execute failed: %v", err)
	}

	expectedID := expectedStartID
	var count int64
	for endpointIdx, endpoint := range info.Endpoint {
		if endpoint == nil || endpoint.Ticket == nil {
			t.Fatalf("ordered query endpoint %d missing ticket", endpointIdx)
		}
		rdr, err := client.DoGet(ctx, endpoint.Ticket)
		if err != nil {
			t.Fatalf("ordered query DoGet failed: %v", err)
		}
		for rdr.Next() {
			rec := rdr.RecordBatch()
			cols := rec.Columns()
			if len(cols) < 2 {
				rdr.Release()
				t.Fatalf("ordered query expected >=2 columns, got=%d", len(cols))
			}
			for row := 0; row < int(rec.NumRows()); row++ {
				idVal, err := rawValueAt(cols[0], row)
				if err != nil {
					rdr.Release()
					t.Fatalf("ordered query id decode failed: %v", err)
				}
				valVal, err := rawValueAt(cols[1], row)
				if err != nil {
					rdr.Release()
					t.Fatalf("ordered query val decode failed: %v", err)
				}
				id, err := toInt64(idVal, "id")
				if err != nil {
					rdr.Release()
					t.Fatalf("ordered query id conversion failed: %v", err)
				}
				val, err := toInt64(valVal, "val")
				if err != nil {
					rdr.Release()
					t.Fatalf("ordered query val conversion failed: %v", err)
				}
				if id != expectedID {
					rdr.Release()
					t.Fatalf("ordered query id mismatch: got=%d expected=%d", id, expectedID)
				}
				if val != expectedVal(id) {
					rdr.Release()
					t.Fatalf("ordered query val mismatch for id=%d: got=%d expected=%d", id, val, expectedVal(id))
				}
				expectedID++
				count++
			}
		}
		if err := rdr.Err(); err != nil {
			rdr.Release()
			t.Fatalf("ordered query iteration failed: %v", err)
		}
		rdr.Release()
	}
	if count != expectedRows {
		t.Fatalf("ordered query row count mismatch: got=%d expected=%d", count, expectedRows)
	}
	return count
}

func runRawOrderedSingleRead(t *testing.T, client *flightsql.Client, rows int) {
	t.Helper()
	requirePositiveFlag(t, rows, "rows")
	ctx := context.Background()

	printPhaseStart("RAW_SELECT_ORDERED_SINGLE")
	start := time.Now()
	rawVerifyOrderedQuery(t, ctx, client, "SELECT id, val FROM "+benchmarkTable+" ORDER BY id", 1, int64(rows))
	duration := time.Since(start)
	printRowMetric("raw_select_ordered_single_rows", duration, int64(rows))
}

func runRawOrderedConcurrentRead(t *testing.T, rows, workers int) {
	t.Helper()
	requirePositiveFlag(t, rows, "rows")
	requirePositiveFlag(t, workers, "workers")

	printPhaseStart("RAW_SELECT_ORDERED_CONCURRENT_200")
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
			ctx := context.Background()
			client, err := newRawClient(ctx)
			if err != nil {
				errCh <- fmt.Errorf("worker=%d create client failed: %w", worker, err)
				return
			}
			defer client.Close()
			query := fmt.Sprintf(
				"SELECT id, val FROM %s WHERE id BETWEEN %d AND %d ORDER BY id",
				benchmarkTable, r.start, r.end,
			)
			count := rawVerifyOrderedQuery(t, ctx, client, query, r.start, r.end-r.start+1)
			verifiedRows.Add(count)
		}(workerID, rg)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("raw concurrent ordered read failed: %v", err)
		}
	}

	total := verifiedRows.Load()
	if total != int64(rows) {
		t.Fatalf("raw concurrent ordered read total mismatch: got=%d expected=%d", total, rows)
	}
	duration := time.Since(start)
	printRowMetric("raw_select_ordered_concurrent_rows", duration, int64(rows))
}

func runRawOrderedConcurrentFullRead(t *testing.T, rows, workers int) {
	t.Helper()
	requirePositiveFlag(t, rows, "rows")
	requirePositiveFlag(t, workers, "workers")

	printPhaseStart("RAW_SELECT_ORDERED_CONCURRENT_FULL_200")
	errCh := make(chan error, workers)
	var verifiedRows atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()

	for workerID := 0; workerID < workers; workerID++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			ctx := context.Background()
			client, err := newRawClient(ctx)
			if err != nil {
				errCh <- fmt.Errorf("worker=%d create client failed: %w", worker, err)
				return
			}
			defer client.Close()
			count := rawVerifyOrderedQuery(t, ctx, client, "SELECT id, val FROM "+benchmarkTable+" ORDER BY id", 1, int64(rows))
			verifiedRows.Add(count)
		}(workerID)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("raw concurrent full ordered read failed: %v", err)
		}
	}

	total := verifiedRows.Load()
	expectedTotal := int64(rows) * int64(workers)
	if total != expectedTotal {
		t.Fatalf("raw concurrent full ordered read total mismatch: got=%d expected=%d", total, expectedTotal)
	}
	duration := time.Since(start)
	printRowMetric("raw_select_ordered_concurrent_full_rows", duration, total)
}

func TestPingRaw(t *testing.T) {
	client := openRawClient(t)
	defer client.Close()

	got, err := rawQueryInt64(context.Background(), client, "SELECT 1")
	if err != nil {
		t.Fatalf("raw SELECT 1 failed: %v", err)
	}
	if got != 1 {
		t.Fatalf("unexpected raw ping result: got=%d expected=1", got)
	}
}

func TestFlightSQLBenchmarksRaw(t *testing.T) {
	requirePositiveFlag(t, *flagRows, "rows")
	requirePositiveFlag(t, *flagWorkers, "workers")
	requirePositiveFlag(t, *flagBatchSize, "batch-size")
	requirePositiveFlag(t, *flagCrudIters, "crud-iters")
	requirePositiveFlag(t, *flagSelectIters, "select-iters")

	client := openRawClient(t)
	defer client.Close()

	runRawCrudSingleOpAutocommit(t, client, *flagCrudIters)
	runRawBatchInsert(t, client, *flagRows, *flagBatchSize)
	runRawConcurrentInsert(t, client, *flagRows, *flagWorkers)
	runRawSelectPrepareModes(t, client, *flagRows, *flagSelectIters)
	runRawSelectPrepareModesConcurrent(t, *flagRows, *flagWorkers, *flagSelectIters)
	runRawConcurrentTransactionCommitRollback(t, client, *flagRows, *flagWorkers)
	runRawConcurrentTransactionCreateDropInsertSelect(t, client, *flagRows, *flagWorkers)
	runRawConcurrentTransactionCommitConflicts(t, client, *flagWorkers)
	runRawOrderedSingleRead(t, client, *flagRows)
	runRawOrderedConcurrentRead(t, *flagRows, *flagWorkers)
	runRawOrderedConcurrentFullRead(t, *flagRows, *flagWorkers)
}
