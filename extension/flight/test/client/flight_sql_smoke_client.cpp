#include <cerrno>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/flight/api.h"
#include "arrow/flight/sql/client.h"
#include "arrow/result.h"
#include "arrow/status.h"

using arrow::Status;
using arrow::flight::FlightClient;
using arrow::flight::Location;
using arrow::flight::sql::FlightSqlClient;
using arrow::flight::sql::PreparedStatement;

namespace {

struct Options {
	std::string host = "127.0.0.1";
	int32_t port = -1;
	std::string mode;
};

void PrintUsage(const char *program_name) {
	std::cerr << "Usage: " << program_name << " --host <host> --port <port> --mode <ping|crud|metadata|prepared>\n";
}

bool ParsePort(const std::string &value, int32_t &port_out) {
	if (value.empty()) {
		return false;
	}
	char *end_ptr = nullptr;
	errno = 0;
	auto parsed = strtoll(value.c_str(), &end_ptr, 10);
	if (errno != 0 || end_ptr == nullptr || *end_ptr != '\0' || parsed < 1 || parsed > 65535) {
		return false;
	}
	port_out = static_cast<int32_t>(parsed);
	return true;
}

bool ParseArgs(int argc, char **argv, Options &options, std::string &error) {
	for (int i = 1; i < argc; i++) {
		const std::string arg = argv[i];
		if (arg == "--host") {
			if (i + 1 >= argc) {
				error = "--host requires a value";
				return false;
			}
			options.host = argv[++i];
		} else if (arg == "--port") {
			if (i + 1 >= argc) {
				error = "--port requires a value";
				return false;
			}
			if (!ParsePort(argv[++i], options.port)) {
				error = "invalid --port value";
				return false;
			}
		} else if (arg == "--mode") {
			if (i + 1 >= argc) {
				error = "--mode requires a value";
				return false;
			}
			options.mode = argv[++i];
		} else {
			error = "unknown argument: " + arg;
			return false;
		}
	}
	if (options.port == -1) {
		error = "--port is required";
		return false;
	}
	if (options.mode != "ping" && options.mode != "crud" && options.mode != "metadata" && options.mode != "prepared") {
		error = "--mode must be ping, crud, metadata or prepared";
		return false;
	}
	return true;
}

arrow::Result<int64_t> GetIntValue(const std::shared_ptr<arrow::Array> &array, int64_t row) {
	switch (array->type_id()) {
	case arrow::Type::INT8:
		return std::static_pointer_cast<arrow::Int8Array>(array)->Value(row);
	case arrow::Type::INT16:
		return std::static_pointer_cast<arrow::Int16Array>(array)->Value(row);
	case arrow::Type::INT32:
		return std::static_pointer_cast<arrow::Int32Array>(array)->Value(row);
	case arrow::Type::INT64:
		return std::static_pointer_cast<arrow::Int64Array>(array)->Value(row);
	case arrow::Type::UINT8:
		return std::static_pointer_cast<arrow::UInt8Array>(array)->Value(row);
	case arrow::Type::UINT16:
		return std::static_pointer_cast<arrow::UInt16Array>(array)->Value(row);
	case arrow::Type::UINT32:
		return std::static_pointer_cast<arrow::UInt32Array>(array)->Value(row);
	case arrow::Type::UINT64:
		return static_cast<int64_t>(std::static_pointer_cast<arrow::UInt64Array>(array)->Value(row));
	default:
		return Status::TypeError("expected integer array but got ", array->type()->ToString());
	}
}

arrow::Result<std::string> GetStringValue(const std::shared_ptr<arrow::Array> &array, int64_t row) {
	if (array->IsNull(row)) {
		return Status::Invalid("unexpected NULL string value");
	}
	switch (array->type_id()) {
	case arrow::Type::STRING:
		return std::static_pointer_cast<arrow::StringArray>(array)->GetString(row);
	case arrow::Type::LARGE_STRING:
		return std::static_pointer_cast<arrow::LargeStringArray>(array)->GetString(row);
	default:
		return Status::TypeError("expected string array but got ", array->type()->ToString());
	}
}

arrow::Result<int64_t> FindColumnIndex(const std::shared_ptr<arrow::Table> &table, const std::string &name) {
	for (int64_t i = 0; i < table->num_columns(); i++) {
		if (table->field(i)->name() == name) {
			return i;
		}
	}
	return Status::Invalid("column not found: ", name);
}

arrow::Result<std::string> GetTableString(const std::shared_ptr<arrow::Table> &table, int64_t col_idx, int64_t row_idx) {
	if (col_idx >= table->num_columns()) {
		return Status::Invalid("column index out of range");
	}
	const auto &column = table->column(col_idx);
	if (column->num_chunks() != 1) {
		return Status::Invalid("expected one chunk for string column");
	}
	return GetStringValue(column->chunk(0), row_idx);
}

arrow::Result<std::shared_ptr<arrow::Table>> ExecuteQuery(FlightSqlClient &client, const std::string &query) {
	ARROW_ASSIGN_OR_RAISE(auto info, client.Execute({}, query));
	if (info->endpoints().empty()) {
		return Status::Invalid("no endpoints returned for query: ", query);
	}
	ARROW_ASSIGN_OR_RAISE(auto stream, client.DoGet({}, info->endpoints()[0].ticket));
	ARROW_ASSIGN_OR_RAISE(auto table, stream->ToTable());
	return table->CombineChunks();
}

arrow::Result<std::shared_ptr<arrow::Table>> FlightInfoToTable(FlightSqlClient &client, std::unique_ptr<arrow::flight::FlightInfo> info,
                                                                const std::string &context) {
	if (!info || info->endpoints().empty()) {
		return Status::Invalid("no endpoints returned for context: ", context);
	}
	ARROW_ASSIGN_OR_RAISE(auto stream, client.DoGet({}, info->endpoints()[0].ticket));
	ARROW_ASSIGN_OR_RAISE(auto table, stream->ToTable());
	return table->CombineChunks();
}

Status ExecuteUpdate(FlightSqlClient &client, const std::string &query, std::optional<int64_t> expected_rows_changed) {
	ARROW_ASSIGN_OR_RAISE(auto rows_changed, client.ExecuteUpdate({}, query));
	if (expected_rows_changed.has_value() && rows_changed != expected_rows_changed.value()) {
		return Status::Invalid("expected ", expected_rows_changed.value(), " rows changed for query [", query,
		                      "] but got ", rows_changed);
	}
	return Status::OK();
}

arrow::Result<std::shared_ptr<arrow::Array>> BuildIntegerArray(const std::shared_ptr<arrow::DataType> &type,
                                                                const std::vector<int64_t> &values) {
	switch (type->id()) {
	case arrow::Type::INT8: {
		arrow::Int8Builder builder;
		for (auto value : values) {
			ARROW_RETURN_NOT_OK(builder.Append(static_cast<int8_t>(value)));
		}
		std::shared_ptr<arrow::Array> array;
		ARROW_RETURN_NOT_OK(builder.Finish(&array));
		return array;
	}
	case arrow::Type::INT16: {
		arrow::Int16Builder builder;
		for (auto value : values) {
			ARROW_RETURN_NOT_OK(builder.Append(static_cast<int16_t>(value)));
		}
		std::shared_ptr<arrow::Array> array;
		ARROW_RETURN_NOT_OK(builder.Finish(&array));
		return array;
	}
	case arrow::Type::INT32: {
		arrow::Int32Builder builder;
		for (auto value : values) {
			ARROW_RETURN_NOT_OK(builder.Append(static_cast<int32_t>(value)));
		}
		std::shared_ptr<arrow::Array> array;
		ARROW_RETURN_NOT_OK(builder.Finish(&array));
		return array;
	}
	case arrow::Type::INT64: {
		arrow::Int64Builder builder;
		for (auto value : values) {
			ARROW_RETURN_NOT_OK(builder.Append(value));
		}
		std::shared_ptr<arrow::Array> array;
		ARROW_RETURN_NOT_OK(builder.Finish(&array));
		return array;
	}
	case arrow::Type::UINT8: {
		arrow::UInt8Builder builder;
		for (auto value : values) {
			ARROW_RETURN_NOT_OK(builder.Append(static_cast<uint8_t>(value)));
		}
		std::shared_ptr<arrow::Array> array;
		ARROW_RETURN_NOT_OK(builder.Finish(&array));
		return array;
	}
	case arrow::Type::UINT16: {
		arrow::UInt16Builder builder;
		for (auto value : values) {
			ARROW_RETURN_NOT_OK(builder.Append(static_cast<uint16_t>(value)));
		}
		std::shared_ptr<arrow::Array> array;
		ARROW_RETURN_NOT_OK(builder.Finish(&array));
		return array;
	}
	case arrow::Type::UINT32: {
		arrow::UInt32Builder builder;
		for (auto value : values) {
			ARROW_RETURN_NOT_OK(builder.Append(static_cast<uint32_t>(value)));
		}
		std::shared_ptr<arrow::Array> array;
		ARROW_RETURN_NOT_OK(builder.Finish(&array));
		return array;
	}
	case arrow::Type::UINT64: {
		arrow::UInt64Builder builder;
		for (auto value : values) {
			ARROW_RETURN_NOT_OK(builder.Append(static_cast<uint64_t>(value)));
		}
		std::shared_ptr<arrow::Array> array;
		ARROW_RETURN_NOT_OK(builder.Finish(&array));
		return array;
	}
	default:
		return Status::NotImplemented("unsupported integer parameter type: ", type->ToString());
	}
}

arrow::Result<std::shared_ptr<arrow::Array>> BuildStringArray(const std::shared_ptr<arrow::DataType> &type,
                                                               const std::vector<std::string> &values) {
	switch (type->id()) {
	case arrow::Type::STRING: {
		arrow::StringBuilder builder;
		for (auto &value : values) {
			ARROW_RETURN_NOT_OK(builder.Append(value));
		}
		std::shared_ptr<arrow::Array> array;
		ARROW_RETURN_NOT_OK(builder.Finish(&array));
		return array;
	}
	case arrow::Type::LARGE_STRING: {
		arrow::LargeStringBuilder builder;
		for (auto &value : values) {
			ARROW_RETURN_NOT_OK(builder.Append(value));
		}
		std::shared_ptr<arrow::Array> array;
		ARROW_RETURN_NOT_OK(builder.Finish(&array));
		return array;
	}
	default:
		return Status::NotImplemented("unsupported string parameter type: ", type->ToString());
	}
}

Status ClosePreparedStatement(const std::shared_ptr<PreparedStatement> &statement) {
	if (!statement || statement->IsClosed()) {
		return Status::OK();
	}
	return statement->Close();
}

Status RunPing(FlightSqlClient &client) {
	ARROW_ASSIGN_OR_RAISE(auto table, ExecuteQuery(client, "SELECT 1 AS one"));
	if (table->num_columns() != 1 || table->num_rows() != 1) {
		return Status::Invalid("ping query returned unexpected shape: rows=", table->num_rows(),
		                      " cols=", table->num_columns());
	}
	if (table->column(0)->num_chunks() != 1) {
		return Status::Invalid("ping query returned unexpected chunking");
	}
	ARROW_ASSIGN_OR_RAISE(auto value, GetIntValue(table->column(0)->chunk(0), 0));
	if (value != 1) {
		return Status::Invalid("ping query returned unexpected value: ", value);
	}
	return Status::OK();
}

Status RunCrud(FlightSqlClient &client) {
	ARROW_RETURN_NOT_OK(
	    ExecuteUpdate(client, "CREATE TABLE flight_it (id INTEGER, val VARCHAR)", std::nullopt));
	ARROW_RETURN_NOT_OK(
	    ExecuteUpdate(client, "INSERT INTO flight_it VALUES (1, 'a'), (2, 'b'), (3, 'c')", 3));
	ARROW_RETURN_NOT_OK(ExecuteUpdate(client, "UPDATE flight_it SET val = 'bb' WHERE id = 2", 1));

	ARROW_ASSIGN_OR_RAISE(auto table, ExecuteQuery(client, "SELECT id, val FROM flight_it ORDER BY id"));
	if (table->num_columns() != 2 || table->num_rows() != 3) {
		return Status::Invalid("CRUD select returned unexpected shape: rows=", table->num_rows(),
		                      " cols=", table->num_columns());
	}
	if (table->column(0)->num_chunks() != 1 || table->column(1)->num_chunks() != 1) {
		return Status::Invalid("CRUD select returned unexpected chunking");
	}

	const std::vector<int64_t> expected_ids = {1, 2, 3};
	const std::vector<std::string> expected_vals = {"a", "bb", "c"};
	auto id_array = table->column(0)->chunk(0);
	auto val_array = std::dynamic_pointer_cast<arrow::StringArray>(table->column(1)->chunk(0));
	if (!val_array) {
		return Status::TypeError("expected string array for val column but got ",
		                         table->column(1)->chunk(0)->type()->ToString());
	}

	for (int64_t i = 0; i < 3; i++) {
		ARROW_ASSIGN_OR_RAISE(auto id, GetIntValue(id_array, i));
		if (id != expected_ids[static_cast<size_t>(i)]) {
			return Status::Invalid("unexpected id at row ", i, ": ", id);
		}
		if (val_array->IsNull(i)) {
			return Status::Invalid("unexpected NULL val at row ", i);
		}
		auto value = val_array->GetString(i);
		if (value != expected_vals[static_cast<size_t>(i)]) {
			return Status::Invalid("unexpected val at row ", i, ": ", value);
		}
	}

	ARROW_RETURN_NOT_OK(ExecuteUpdate(client, "DROP TABLE flight_it", std::nullopt));
	ARROW_ASSIGN_OR_RAISE(
	    auto count_table,
	    ExecuteQuery(client, "SELECT COUNT(*)::BIGINT AS cnt FROM duckdb_tables() WHERE table_name = 'flight_it'"));
	if (count_table->num_columns() != 1 || count_table->num_rows() != 1 || count_table->column(0)->num_chunks() != 1) {
		return Status::Invalid("post-drop verification returned unexpected shape");
	}
	ARROW_ASSIGN_OR_RAISE(auto count, GetIntValue(count_table->column(0)->chunk(0), 0));
	if (count != 0) {
		return Status::Invalid("flight_it table still exists after DROP TABLE");
	}
	return Status::OK();
}

Status RunMetadata(FlightSqlClient &client) {
	auto cleanup = [&client]() {
		(void)ExecuteUpdate(client, "DROP VIEW IF EXISTS flight_meta_view", std::nullopt);
		(void)ExecuteUpdate(client, "DROP TABLE IF EXISTS flight_meta_tbl", std::nullopt);
	};
	auto fail_with_cleanup = [&](Status status) {
		cleanup();
		return status;
	};

	auto status = ExecuteUpdate(client, "DROP VIEW IF EXISTS flight_meta_view", std::nullopt);
	if (!status.ok()) {
		return status;
	}
	status = ExecuteUpdate(client, "DROP TABLE IF EXISTS flight_meta_tbl", std::nullopt);
	if (!status.ok()) {
		return status;
	}

	status = ExecuteUpdate(client, "CREATE TABLE flight_meta_tbl (id INTEGER, val VARCHAR)", std::nullopt);
	if (!status.ok()) {
		return fail_with_cleanup(std::move(status));
	}
	status = ExecuteUpdate(client, "CREATE VIEW flight_meta_view AS SELECT * FROM flight_meta_tbl", std::nullopt);
	if (!status.ok()) {
		return fail_with_cleanup(std::move(status));
	}

	std::string schema_pattern = "main";
	auto schema_info_result = client.GetDbSchemas({}, nullptr, &schema_pattern);
	if (!schema_info_result.ok()) {
		return fail_with_cleanup(schema_info_result.status());
	}
	auto schemas_table_result =
	    FlightInfoToTable(client, schema_info_result.MoveValueUnsafe(), "GetDbSchemas(main)");
	if (!schemas_table_result.ok()) {
		return fail_with_cleanup(schemas_table_result.status());
	}
	auto schemas_table = schemas_table_result.MoveValueUnsafe();
	auto schema_col_result = FindColumnIndex(schemas_table, "db_schema_name");
	if (!schema_col_result.ok()) {
		return fail_with_cleanup(schema_col_result.status());
	}
	auto schema_col = schema_col_result.MoveValueUnsafe();
	bool found_main_schema = false;
	for (int64_t row = 0; row < schemas_table->num_rows(); row++) {
		auto schema_name_result = GetTableString(schemas_table, schema_col, row);
		if (!schema_name_result.ok()) {
			return fail_with_cleanup(schema_name_result.status());
		}
		if (schema_name_result.MoveValueUnsafe() == "main") {
			found_main_schema = true;
			break;
		}
	}
	if (!found_main_schema) {
		return fail_with_cleanup(Status::Invalid("GetDbSchemas did not return schema 'main'"));
	}

	auto table_types_info_result = client.GetTableTypes({});
	if (!table_types_info_result.ok()) {
		return fail_with_cleanup(table_types_info_result.status());
	}
	auto table_types_table_result =
	    FlightInfoToTable(client, table_types_info_result.MoveValueUnsafe(), "GetTableTypes");
	if (!table_types_table_result.ok()) {
		return fail_with_cleanup(table_types_table_result.status());
	}
	auto table_types_table = table_types_table_result.MoveValueUnsafe();
	auto table_type_col_result = FindColumnIndex(table_types_table, "table_type");
	if (!table_type_col_result.ok()) {
		return fail_with_cleanup(table_type_col_result.status());
	}
	auto table_type_col = table_type_col_result.MoveValueUnsafe();
	std::set<std::string> type_names;
	for (int64_t row = 0; row < table_types_table->num_rows(); row++) {
		auto table_type_result = GetTableString(table_types_table, table_type_col, row);
		if (!table_type_result.ok()) {
			return fail_with_cleanup(table_type_result.status());
		}
		type_names.insert(table_type_result.MoveValueUnsafe());
	}
	if (!type_names.count("TABLE") || !type_names.count("VIEW")) {
		return fail_with_cleanup(
		    Status::Invalid("GetTableTypes did not include expected TABLE/VIEW values"));
	}

	std::string table_pattern = "flight_meta_%";
	auto tables_info_result = client.GetTables({}, nullptr, &schema_pattern, &table_pattern, false, nullptr);
	if (!tables_info_result.ok()) {
		return fail_with_cleanup(tables_info_result.status());
	}
	auto tables_table_result = FlightInfoToTable(client, tables_info_result.MoveValueUnsafe(), "GetTables(pattern)");
	if (!tables_table_result.ok()) {
		return fail_with_cleanup(tables_table_result.status());
	}
	auto tables_table = tables_table_result.MoveValueUnsafe();
	auto table_name_col_result = FindColumnIndex(tables_table, "table_name");
	if (!table_name_col_result.ok()) {
		return fail_with_cleanup(table_name_col_result.status());
	}
	auto table_type_name_col_result = FindColumnIndex(tables_table, "table_type");
	if (!table_type_name_col_result.ok()) {
		return fail_with_cleanup(table_type_name_col_result.status());
	}
	auto table_name_col = table_name_col_result.MoveValueUnsafe();
	auto table_type_name_col = table_type_name_col_result.MoveValueUnsafe();
	bool saw_meta_table = false;
	bool saw_meta_view = false;
	for (int64_t row = 0; row < tables_table->num_rows(); row++) {
		auto table_name_result = GetTableString(tables_table, table_name_col, row);
		if (!table_name_result.ok()) {
			return fail_with_cleanup(table_name_result.status());
		}
		auto table_type_result = GetTableString(tables_table, table_type_name_col, row);
		if (!table_type_result.ok()) {
			return fail_with_cleanup(table_type_result.status());
		}
		auto table_name = table_name_result.MoveValueUnsafe();
		auto table_type = table_type_result.MoveValueUnsafe();
		if (table_name == "flight_meta_tbl" && table_type == "TABLE") {
			saw_meta_table = true;
		} else if (table_name == "flight_meta_view" && table_type == "VIEW") {
			saw_meta_view = true;
		}
	}
	if (!saw_meta_table || !saw_meta_view) {
		return fail_with_cleanup(Status::Invalid(
		    "GetTables did not return expected flight_meta_tbl/flight_meta_view entries"));
	}

	std::vector<std::string> only_tables {"TABLE"};
	auto filtered_tables_info_result =
	    client.GetTables({}, nullptr, &schema_pattern, &table_pattern, false, &only_tables);
	if (!filtered_tables_info_result.ok()) {
		return fail_with_cleanup(filtered_tables_info_result.status());
	}
	auto filtered_table_result =
	    FlightInfoToTable(client, filtered_tables_info_result.MoveValueUnsafe(), "GetTables(table_types)");
	if (!filtered_table_result.ok()) {
		return fail_with_cleanup(filtered_table_result.status());
	}
	auto filtered_tables = filtered_table_result.MoveValueUnsafe();
	auto filtered_table_name_col_result = FindColumnIndex(filtered_tables, "table_name");
	if (!filtered_table_name_col_result.ok()) {
		return fail_with_cleanup(filtered_table_name_col_result.status());
	}
	auto filtered_table_name_col = filtered_table_name_col_result.MoveValueUnsafe();
	bool saw_only_meta_table = false;
	for (int64_t row = 0; row < filtered_tables->num_rows(); row++) {
		auto table_name_result = GetTableString(filtered_tables, filtered_table_name_col, row);
		if (!table_name_result.ok()) {
			return fail_with_cleanup(table_name_result.status());
		}
		auto table_name = table_name_result.MoveValueUnsafe();
		if (table_name == "flight_meta_view") {
			return fail_with_cleanup(
			    Status::Invalid("GetTables table_type filter returned view entry unexpectedly"));
		}
		if (table_name == "flight_meta_tbl") {
			saw_only_meta_table = true;
		}
	}
	if (!saw_only_meta_table) {
		return fail_with_cleanup(
		    Status::Invalid("GetTables table_type filter did not return expected table entry"));
	}

	cleanup();
	return Status::OK();
}

Status RunPrepared(FlightSqlClient &client) {
	std::vector<std::shared_ptr<PreparedStatement>> statements;
	auto cleanup = [&]() {
		for (auto &statement : statements) {
			(void)ClosePreparedStatement(statement);
		}
		(void)ExecuteUpdate(client, "DROP TABLE IF EXISTS flight_prep_it", std::nullopt);
	};
	auto fail_with_cleanup = [&](Status status) {
		cleanup();
		return status;
	};

	auto status = ExecuteUpdate(client, "DROP TABLE IF EXISTS flight_prep_it", std::nullopt);
	if (!status.ok()) {
		return status;
	}
	status = ExecuteUpdate(client, "CREATE TABLE flight_prep_it (id INTEGER, val VARCHAR)", std::nullopt);
	if (!status.ok()) {
		return status;
	}

	auto prepared_insert_result = client.Prepare({}, "INSERT INTO flight_prep_it VALUES (?, ?)");
	if (!prepared_insert_result.ok()) {
		return fail_with_cleanup(prepared_insert_result.status());
	}
	auto prepared_insert = prepared_insert_result.MoveValueUnsafe();
	statements.push_back(prepared_insert);
	auto insert_schema = prepared_insert->parameter_schema();
	if (!insert_schema || insert_schema->num_fields() != 2) {
		return fail_with_cleanup(Status::Invalid("unexpected parameter schema for insert prepared statement"));
	}
	ARROW_ASSIGN_OR_RAISE(auto insert_id_array, BuildIntegerArray(insert_schema->field(0)->type(), {1, 2, 3}));
	ARROW_ASSIGN_OR_RAISE(auto insert_val_array, BuildStringArray(insert_schema->field(1)->type(), {"a", "b", "c"}));
	auto insert_batch = arrow::RecordBatch::Make(insert_schema, 3, {insert_id_array, insert_val_array});
	status = prepared_insert->SetParameters(insert_batch);
	if (!status.ok()) {
		return fail_with_cleanup(status);
	}
	auto insert_rows_result = prepared_insert->ExecuteUpdate({});
	if (!insert_rows_result.ok()) {
		return fail_with_cleanup(insert_rows_result.status());
	}
	if (insert_rows_result.ValueOrDie() != 3) {
		return fail_with_cleanup(
		    Status::Invalid("prepared insert expected 3 affected rows, got ", insert_rows_result.ValueOrDie()));
	}
	status = ClosePreparedStatement(prepared_insert);
	if (!status.ok()) {
		return fail_with_cleanup(status);
	}

	auto prepared_update_result = client.Prepare({}, "UPDATE flight_prep_it SET val = ? WHERE id = ?");
	if (!prepared_update_result.ok()) {
		return fail_with_cleanup(prepared_update_result.status());
	}
	auto prepared_update = prepared_update_result.MoveValueUnsafe();
	statements.push_back(prepared_update);
	auto update_schema = prepared_update->parameter_schema();
	if (!update_schema || update_schema->num_fields() != 2) {
		return fail_with_cleanup(Status::Invalid("unexpected parameter schema for update prepared statement"));
	}
	ARROW_ASSIGN_OR_RAISE(auto update_val_array, BuildStringArray(update_schema->field(0)->type(), {"bb", "cc"}));
	ARROW_ASSIGN_OR_RAISE(auto update_id_array, BuildIntegerArray(update_schema->field(1)->type(), {2, 3}));
	auto update_batch = arrow::RecordBatch::Make(update_schema, 2, {update_val_array, update_id_array});
	status = prepared_update->SetParameters(update_batch);
	if (!status.ok()) {
		return fail_with_cleanup(status);
	}
	auto update_rows_result = prepared_update->ExecuteUpdate({});
	if (!update_rows_result.ok()) {
		return fail_with_cleanup(update_rows_result.status());
	}
	if (update_rows_result.ValueOrDie() != 2) {
		return fail_with_cleanup(
		    Status::Invalid("prepared update expected 2 affected rows, got ", update_rows_result.ValueOrDie()));
	}
	status = ClosePreparedStatement(prepared_update);
	if (!status.ok()) {
		return fail_with_cleanup(status);
	}

	auto prepared_query_result = client.Prepare({}, "SELECT id, val FROM flight_prep_it WHERE id > ? ORDER BY id");
	if (!prepared_query_result.ok()) {
		return fail_with_cleanup(prepared_query_result.status());
	}
	auto prepared_query = prepared_query_result.MoveValueUnsafe();
	statements.push_back(prepared_query);
	auto query_schema = prepared_query->parameter_schema();
	if (!query_schema || query_schema->num_fields() != 1) {
		return fail_with_cleanup(Status::Invalid("unexpected parameter schema for query prepared statement"));
	}
	ARROW_ASSIGN_OR_RAISE(auto query_id_array, BuildIntegerArray(query_schema->field(0)->type(), {1}));
	auto query_batch = arrow::RecordBatch::Make(query_schema, 1, {query_id_array});
	status = prepared_query->SetParameters(query_batch);
	if (!status.ok()) {
		return fail_with_cleanup(status);
	}
	auto query_info_result = prepared_query->Execute({});
	if (!query_info_result.ok()) {
		return fail_with_cleanup(query_info_result.status());
	}
	auto query_info = std::move(query_info_result).ValueOrDie();
	auto query_table_result = FlightInfoToTable(client, std::move(query_info), "Prepared query");
	if (!query_table_result.ok()) {
		return fail_with_cleanup(query_table_result.status());
	}
	auto query_table = query_table_result.MoveValueUnsafe();
	if (query_table->num_columns() != 2 || query_table->num_rows() != 2) {
		return fail_with_cleanup(Status::Invalid("prepared query returned unexpected shape"));
	}
	if (query_table->column(0)->num_chunks() != 1 || query_table->column(1)->num_chunks() != 1) {
		return fail_with_cleanup(Status::Invalid("prepared query returned unexpected chunking"));
	}
	const std::vector<int64_t> expected_ids = {2, 3};
	const std::vector<std::string> expected_vals = {"bb", "cc"};
	for (int64_t row = 0; row < 2; row++) {
		ARROW_ASSIGN_OR_RAISE(auto id, GetIntValue(query_table->column(0)->chunk(0), row));
		if (id != expected_ids[static_cast<size_t>(row)]) {
			return fail_with_cleanup(Status::Invalid("unexpected id in prepared query result: ", id));
		}
		ARROW_ASSIGN_OR_RAISE(auto val, GetStringValue(query_table->column(1)->chunk(0), row));
		if (val != expected_vals[static_cast<size_t>(row)]) {
			return fail_with_cleanup(Status::Invalid("unexpected val in prepared query result: ", val));
		}
	}
	status = ClosePreparedStatement(prepared_query);
	if (!status.ok()) {
		return fail_with_cleanup(status);
	}

	auto prepared_multirow_query_result = client.Prepare({}, "SELECT ?::INTEGER AS x");
	if (!prepared_multirow_query_result.ok()) {
		return fail_with_cleanup(prepared_multirow_query_result.status());
	}
	auto prepared_multirow_query = prepared_multirow_query_result.MoveValueUnsafe();
	statements.push_back(prepared_multirow_query);
	auto multirow_query_schema = prepared_multirow_query->parameter_schema();
	if (!multirow_query_schema || multirow_query_schema->num_fields() != 1) {
		return fail_with_cleanup(
		    Status::Invalid("unexpected parameter schema for single-row enforcement test"));
	}
	ARROW_ASSIGN_OR_RAISE(auto multirow_query_array,
	                      BuildIntegerArray(multirow_query_schema->field(0)->type(), {1, 2}));
	auto multirow_batch = arrow::RecordBatch::Make(multirow_query_schema, 2, {multirow_query_array});
	status = prepared_multirow_query->SetParameters(multirow_batch);
	if (!status.ok()) {
		return fail_with_cleanup(status);
	}
	auto multirow_exec_result = prepared_multirow_query->Execute({});
	if (multirow_exec_result.ok()) {
		return fail_with_cleanup(
		    Status::Invalid("prepared query with multiple parameter rows unexpectedly succeeded"));
	}
	status = ClosePreparedStatement(prepared_multirow_query);
	if (!status.ok()) {
		return fail_with_cleanup(status);
	}

	status = ExecuteUpdate(client, "DROP TABLE flight_prep_it", std::nullopt);
	if (!status.ok()) {
		return fail_with_cleanup(status);
	}
	cleanup();
	return Status::OK();
}

Status RunMain(const Options &options) {
	ARROW_ASSIGN_OR_RAISE(auto location, Location::ForGrpcTcp(options.host, options.port));
	ARROW_ASSIGN_OR_RAISE(auto client, FlightClient::Connect(location));
	FlightSqlClient sql_client(std::move(client));

	Status status;
	if (options.mode == "ping") {
		status = RunPing(sql_client);
	} else if (options.mode == "metadata") {
		status = RunMetadata(sql_client);
	} else if (options.mode == "prepared") {
		status = RunPrepared(sql_client);
	} else {
		status = RunCrud(sql_client);
	}
	auto close_status = sql_client.Close();
	if (!status.ok()) {
		return status;
	}
	return close_status;
}

} // namespace

int main(int argc, char **argv) {
	Options options;
	std::string error;
	if (!ParseArgs(argc, argv, options, error)) {
		std::cerr << "Argument error: " << error << "\n";
		PrintUsage(argv[0]);
		return 2;
	}

	auto status = RunMain(options);
	if (!status.ok()) {
		std::cerr << "Flight SQL smoke client failed: " << status.ToString() << "\n";
		return 1;
	}
	return 0;
}
