#include <cerrno>
#include <chrono>
#include <cctype>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <google/protobuf/any.pb.h>

#include "arrow/api.h"
#include "arrow/flight/api.h"
#include "arrow/flight/serialization_internal.h"
#include "arrow/flight/sql/client.h"
#include "arrow/flight/sql/protocol_internal.h"
#include "arrow/ipc/reader.h"
#include "arrow/io/memory.h"
#include "arrow/result.h"
#include "arrow/status.h"

using arrow::Status;
using arrow::flight::FlightClient;
using arrow::flight::Location;
using arrow::flight::sql::FlightSqlClient;
using arrow::flight::sql::PreparedStatement;
using arrow::flight::sql::Transaction;
namespace flight_sql_pb = arrow::flight::protocol::sql;

namespace {

struct Options {
	std::string host = "127.0.0.1";
	int32_t port = -1;
	std::string mode;
};

void PrintUsage(const char *program_name) {
	std::cerr << "Usage: " << program_name
	          << " --host <host> --port <port> --mode <ping|crud|metadata|prepared|transaction|timeout>\n";
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
	if (options.mode != "ping" && options.mode != "crud" && options.mode != "metadata" && options.mode != "prepared" &&
	    options.mode != "transaction" && options.mode != "timeout") {
		error = "--mode must be ping, crud, metadata, prepared, transaction or timeout";
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

arrow::Result<std::string> GetBinaryValue(const std::shared_ptr<arrow::Array> &array, int64_t row) {
	if (array->IsNull(row)) {
		return Status::Invalid("unexpected NULL binary value");
	}
	switch (array->type_id()) {
	case arrow::Type::BINARY: {
		auto view = std::static_pointer_cast<arrow::BinaryArray>(array)->GetView(row);
		return std::string(view.data(), view.size());
	}
	case arrow::Type::LARGE_BINARY: {
		auto view = std::static_pointer_cast<arrow::LargeBinaryArray>(array)->GetView(row);
		return std::string(view.data(), view.size());
	}
	default:
		return Status::TypeError("expected binary array but got ", array->type()->ToString());
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

arrow::Result<std::string> GetTableBinary(const std::shared_ptr<arrow::Table> &table, int64_t col_idx, int64_t row_idx) {
	if (col_idx >= table->num_columns()) {
		return Status::Invalid("column index out of range");
	}
	const auto &column = table->column(col_idx);
	if (column->num_chunks() != 1) {
		return Status::Invalid("expected one chunk for binary column");
	}
	return GetBinaryValue(column->chunk(0), row_idx);
}

arrow::Result<std::shared_ptr<arrow::Schema>> ParseSerializedSchema(const std::string &schema_bytes) {
	auto buffer = arrow::Buffer::FromString(schema_bytes);
	arrow::io::BufferReader reader(buffer);
	return arrow::ipc::ReadSchema(&reader, nullptr);
}

bool SchemaHasField(const std::shared_ptr<arrow::Schema> &schema, const std::string &field_name) {
	return schema && schema->GetFieldByName(field_name) != nullptr;
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

arrow::Result<std::shared_ptr<arrow::Table>> ExecuteQueryWithOptions(FlightSqlClient &client,
                                                                      const arrow::flight::FlightCallOptions &options,
                                                                      const std::string &query) {
	ARROW_ASSIGN_OR_RAISE(auto info, client.Execute(options, query));
	if (info->endpoints().empty()) {
		return Status::Invalid("no endpoints returned for query: ", query);
	}
	ARROW_ASSIGN_OR_RAISE(auto stream, client.DoGet(options, info->endpoints()[0].ticket));
	ARROW_ASSIGN_OR_RAISE(auto table, stream->ToTable());
	return table->CombineChunks();
}

arrow::Result<std::shared_ptr<arrow::Table>> ExecuteQueryInTransaction(FlightSqlClient &client, const std::string &query,
                                                                        const Transaction &transaction) {
	ARROW_ASSIGN_OR_RAISE(auto info, client.Execute({}, query, transaction));
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

Status ExecuteUpdateInTransaction(FlightSqlClient &client, const std::string &query, const Transaction &transaction,
                                  std::optional<int64_t> expected_rows_changed) {
	ARROW_ASSIGN_OR_RAISE(auto rows_changed, client.ExecuteUpdate({}, query, transaction));
	if (expected_rows_changed.has_value() && rows_changed != expected_rows_changed.value()) {
		return Status::Invalid("expected ", expected_rows_changed.value(), " rows changed for query [", query,
		                      "] but got ", rows_changed);
	}
	return Status::OK();
}

arrow::Result<int64_t> QueryCount(FlightSqlClient &client, const std::string &query) {
	ARROW_ASSIGN_OR_RAISE(auto table, ExecuteQuery(client, query));
	if (table->num_columns() != 1 || table->num_rows() != 1 || table->column(0)->num_chunks() != 1) {
		return Status::Invalid("count query returned unexpected shape");
	}
	return GetIntValue(table->column(0)->chunk(0), 0);
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

bool IsTimeoutStatus(const Status &status) {
	if (status.ok()) {
		return false;
	}
	auto text = status.ToString();
	std::string lowered;
	lowered.reserve(text.size());
	for (auto c : text) {
		lowered.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
	}
	return lowered.find("deadline exceeded") != std::string::npos ||
	       lowered.find("timed out") != std::string::npos || lowered.find("timeout") != std::string::npos ||
	       lowered.find("context deadline exceeded") != std::string::npos ||
	       lowered.find("cancelled") != std::string::npos || lowered.find("canceled") != std::string::npos;
}

template <class T>
arrow::Result<arrow::flight::FlightDescriptor> PackCommandDescriptor(const T &command) {
	arrow::flight::FlightDescriptor descriptor;
	ARROW_RETURN_NOT_OK(arrow::flight::internal::PackProtoCommand(command, &descriptor));
	return descriptor;
}

template <class T>
arrow::Result<std::unique_ptr<arrow::flight::ResultStream>> DoProtoAction(FlightSqlClient &client,
                                                                           const std::string &action_type,
                                                                           const T &action) {
	arrow::flight::Action packed_action;
	ARROW_RETURN_NOT_OK(arrow::flight::internal::PackProtoAction(action_type, action, &packed_action));
	return client.DoAction({}, packed_action);
}

arrow::Result<std::string> CreatePreparedHandleRaw(FlightSqlClient &client, const std::string &query,
                                                   const std::string *transaction_id = nullptr) {
	flight_sql_pb::ActionCreatePreparedStatementRequest request;
	request.set_query(query);
	if (transaction_id && !transaction_id->empty()) {
		request.set_transaction_id(*transaction_id);
	}

	ARROW_ASSIGN_OR_RAISE(auto results, DoProtoAction(client, "CreatePreparedStatement", request));
	ARROW_ASSIGN_OR_RAISE(auto result, results->Next());
	if (!result || !result->body) {
		return Status::Invalid("CreatePreparedStatement returned no payload");
	}

	google::protobuf::Any container;
	if (!container.ParseFromArray(result->body->data(), static_cast<int>(result->body->size()))) {
		return Status::Invalid("Unable to parse Any for ActionCreatePreparedStatementResult");
	}

	flight_sql_pb::ActionCreatePreparedStatementResult response;
	if (!container.UnpackTo(&response)) {
		return Status::Invalid("Unable to unpack ActionCreatePreparedStatementResult");
	}

	ARROW_RETURN_NOT_OK(results->Drain());
	return response.prepared_statement_handle();
}

Status ClosePreparedHandleRaw(FlightSqlClient &client, const std::string &handle) {
	flight_sql_pb::ActionClosePreparedStatementRequest request;
	request.set_prepared_statement_handle(handle);
	ARROW_ASSIGN_OR_RAISE(auto results, DoProtoAction(client, "ClosePreparedStatement", request));
	return results->Drain();
}

arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> GetPreparedFlightInfoRaw(FlightSqlClient &client,
                                                                                    const std::string &handle) {
	flight_sql_pb::CommandPreparedStatementQuery command;
	command.set_prepared_statement_handle(handle);
	ARROW_ASSIGN_OR_RAISE(auto descriptor, PackCommandDescriptor(command));
	return client.GetFlightInfo({}, descriptor);
}

arrow::Result<int64_t> FetchPreparedRowCountRaw(FlightSqlClient &client, const std::string &handle) {
	ARROW_ASSIGN_OR_RAISE(auto info, GetPreparedFlightInfoRaw(client, handle));
	if (!info || info->endpoints().empty()) {
		return Status::Invalid("Prepared query returned no endpoints");
	}
	ARROW_ASSIGN_OR_RAISE(auto stream, client.DoGet({}, info->endpoints()[0].ticket));
	ARROW_ASSIGN_OR_RAISE(auto table, stream->ToTable());
	return table->num_rows();
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
	if (tables_table->num_columns() != 4) {
		return fail_with_cleanup(
		    Status::Invalid("GetTables(include_schema=false) returned unexpected column count: ",
		                    tables_table->num_columns()));
	}
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

	auto tables_with_schema_info_result = client.GetTables({}, nullptr, &schema_pattern, &table_pattern, true, nullptr);
	if (!tables_with_schema_info_result.ok()) {
		return fail_with_cleanup(tables_with_schema_info_result.status());
	}
	auto tables_with_schema_result =
	    FlightInfoToTable(client, tables_with_schema_info_result.MoveValueUnsafe(), "GetTables(include_schema)");
	if (!tables_with_schema_result.ok()) {
		return fail_with_cleanup(tables_with_schema_result.status());
	}
	auto tables_with_schema = tables_with_schema_result.MoveValueUnsafe();
	if (tables_with_schema->num_columns() != 5) {
		return fail_with_cleanup(
		    Status::Invalid("GetTables(include_schema=true) returned unexpected column count: ",
		                    tables_with_schema->num_columns()));
	}
	auto schema_table_name_col_result = FindColumnIndex(tables_with_schema, "table_name");
	if (!schema_table_name_col_result.ok()) {
		return fail_with_cleanup(schema_table_name_col_result.status());
	}
	auto schema_table_type_col_result = FindColumnIndex(tables_with_schema, "table_type");
	if (!schema_table_type_col_result.ok()) {
		return fail_with_cleanup(schema_table_type_col_result.status());
	}
	auto table_schema_col_result = FindColumnIndex(tables_with_schema, "table_schema");
	if (!table_schema_col_result.ok()) {
		return fail_with_cleanup(table_schema_col_result.status());
	}
	auto schema_table_name_col = schema_table_name_col_result.MoveValueUnsafe();
	auto schema_table_type_col = schema_table_type_col_result.MoveValueUnsafe();
	auto table_schema_col = table_schema_col_result.MoveValueUnsafe();
	bool saw_meta_table_with_schema = false;
	bool saw_meta_view_with_schema = false;
	for (int64_t row = 0; row < tables_with_schema->num_rows(); row++) {
		auto table_name_result = GetTableString(tables_with_schema, schema_table_name_col, row);
		if (!table_name_result.ok()) {
			return fail_with_cleanup(table_name_result.status());
		}
		auto table_type_result = GetTableString(tables_with_schema, schema_table_type_col, row);
		if (!table_type_result.ok()) {
			return fail_with_cleanup(table_type_result.status());
		}
		auto table_schema_bytes_result = GetTableBinary(tables_with_schema, table_schema_col, row);
		if (!table_schema_bytes_result.ok()) {
			return fail_with_cleanup(table_schema_bytes_result.status());
		}
		auto table_schema_bytes = table_schema_bytes_result.MoveValueUnsafe();
		if (table_schema_bytes.empty()) {
			return fail_with_cleanup(Status::Invalid("GetTables(include_schema=true) returned empty table_schema"));
		}
		auto parsed_schema_result = ParseSerializedSchema(table_schema_bytes);
		if (!parsed_schema_result.ok()) {
			return fail_with_cleanup(parsed_schema_result.status());
		}
		auto parsed_schema = parsed_schema_result.MoveValueUnsafe();

		auto table_name = table_name_result.MoveValueUnsafe();
		auto table_type = table_type_result.MoveValueUnsafe();
		if (table_name == "flight_meta_tbl" && table_type == "TABLE") {
			saw_meta_table_with_schema = true;
			if (!SchemaHasField(parsed_schema, "id") || !SchemaHasField(parsed_schema, "val")) {
				return fail_with_cleanup(Status::Invalid(
				    "flight_meta_tbl serialized schema missing expected fields"));
			}
		} else if (table_name == "flight_meta_view" && table_type == "VIEW") {
			saw_meta_view_with_schema = true;
			if (!SchemaHasField(parsed_schema, "id") || !SchemaHasField(parsed_schema, "val")) {
				return fail_with_cleanup(Status::Invalid(
				    "flight_meta_view serialized schema missing expected fields"));
			}
		}
	}
	if (!saw_meta_table_with_schema || !saw_meta_view_with_schema) {
		return fail_with_cleanup(Status::Invalid(
		    "GetTables(include_schema=true) did not return expected table/view entries"));
	}

	cleanup();
	return Status::OK();
}

Status RunPrepared(FlightSqlClient &client, const Options &options) {
	const std::string timeout_reset_sql = "CALL set_flight_sql_transaction_timeout_seconds(1800)";
	const std::string timeout_enable_sql = "CALL set_flight_sql_transaction_timeout_seconds(1)";
	std::vector<std::shared_ptr<PreparedStatement>> statements;
	std::vector<std::string> raw_handles;
	auto cleanup = [&]() {
		for (auto &statement : statements) {
			(void)ClosePreparedStatement(statement);
		}
		for (auto &handle : raw_handles) {
			(void)ClosePreparedHandleRaw(client, handle);
		}
		(void)ExecuteUpdate(client, "DROP TABLE IF EXISTS flight_prep_it", std::nullopt);
		(void)ExecuteQuery(client, timeout_reset_sql);
	};
	auto fail_with_cleanup = [&](Status status) {
		cleanup();
		return status;
	};
	auto contextual_fail = [&](const std::string &context, const Status &status) {
		return fail_with_cleanup(Status::Invalid(context, ": ", status.ToString()));
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
	ARROW_ASSIGN_OR_RAISE(auto insert_id_array, BuildIntegerArray(insert_schema->field(0)->type(), {1, 2}));
	ARROW_ASSIGN_OR_RAISE(auto insert_val_array, BuildStringArray(insert_schema->field(1)->type(), {"a", "b"}));
	auto insert_batch = arrow::RecordBatch::Make(insert_schema, 2, {insert_id_array, insert_val_array});
	status = prepared_insert->SetParameters(insert_batch);
	if (!status.ok()) {
		return contextual_fail("prepared insert set parameters", status);
	}
	auto insert_rows_result = prepared_insert->ExecuteUpdate({});
	if (!insert_rows_result.ok()) {
		return contextual_fail("prepared insert execute update", insert_rows_result.status());
	}
	if (insert_rows_result.ValueOrDie() != 2) {
		return fail_with_cleanup(
		    Status::Invalid("prepared insert expected 2 affected rows, got ", insert_rows_result.ValueOrDie()));
	}

	ARROW_ASSIGN_OR_RAISE(auto insert_id_array_second, BuildIntegerArray(insert_schema->field(0)->type(), {3}));
	ARROW_ASSIGN_OR_RAISE(auto insert_val_array_second, BuildStringArray(insert_schema->field(1)->type(), {"c"}));
	auto insert_batch_second = arrow::RecordBatch::Make(insert_schema, 1, {insert_id_array_second, insert_val_array_second});
	status = prepared_insert->SetParameters(insert_batch_second);
	if (!status.ok()) {
		return contextual_fail("prepared insert second set parameters", status);
	}
	auto insert_rows_result_second = prepared_insert->ExecuteUpdate({});
	if (!insert_rows_result_second.ok()) {
		return contextual_fail("prepared insert second execute update", insert_rows_result_second.status());
	}
	if (insert_rows_result_second.ValueOrDie() != 1) {
		return fail_with_cleanup(
		    Status::Invalid("second prepared insert expected 1 affected row, got ", insert_rows_result_second.ValueOrDie()));
	}
	status = ClosePreparedStatement(prepared_insert);
	if (!status.ok()) {
		return contextual_fail("prepared insert close", status);
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
	ARROW_ASSIGN_OR_RAISE(auto update_val_array, BuildStringArray(update_schema->field(0)->type(), {"bb"}));
	ARROW_ASSIGN_OR_RAISE(auto update_id_array, BuildIntegerArray(update_schema->field(1)->type(), {2}));
	auto update_batch = arrow::RecordBatch::Make(update_schema, 1, {update_val_array, update_id_array});
	status = prepared_update->SetParameters(update_batch);
	if (!status.ok()) {
		return contextual_fail("prepared update set parameters", status);
	}
	auto update_rows_result = prepared_update->ExecuteUpdate({});
	if (!update_rows_result.ok()) {
		return contextual_fail("prepared update execute update", update_rows_result.status());
	}
	if (update_rows_result.ValueOrDie() != 1) {
		return fail_with_cleanup(
		    Status::Invalid("prepared update expected 1 affected row, got ", update_rows_result.ValueOrDie()));
	}

	ARROW_ASSIGN_OR_RAISE(auto update_val_array_second, BuildStringArray(update_schema->field(0)->type(), {"cc"}));
	ARROW_ASSIGN_OR_RAISE(auto update_id_array_second, BuildIntegerArray(update_schema->field(1)->type(), {3}));
	auto update_batch_second = arrow::RecordBatch::Make(update_schema, 1, {update_val_array_second, update_id_array_second});
	status = prepared_update->SetParameters(update_batch_second);
	if (!status.ok()) {
		return contextual_fail("prepared update second set parameters", status);
	}
	auto update_rows_result_second = prepared_update->ExecuteUpdate({});
	if (!update_rows_result_second.ok()) {
		return contextual_fail("prepared update second execute update", update_rows_result_second.status());
	}
	if (update_rows_result_second.ValueOrDie() != 1) {
		return fail_with_cleanup(
		    Status::Invalid("second prepared update expected 1 affected row, got ", update_rows_result_second.ValueOrDie()));
	}
	status = ClosePreparedStatement(prepared_update);
	if (!status.ok()) {
		return contextual_fail("prepared update close", status);
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
		return contextual_fail("prepared query set parameters", status);
	}
	auto query_info_result = prepared_query->Execute({});
	if (!query_info_result.ok()) {
		return contextual_fail("prepared query execute", query_info_result.status());
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

	ARROW_ASSIGN_OR_RAISE(auto query_id_array_second, BuildIntegerArray(query_schema->field(0)->type(), {0}));
	auto query_batch_second = arrow::RecordBatch::Make(query_schema, 1, {query_id_array_second});
	status = prepared_query->SetParameters(query_batch_second);
	if (!status.ok()) {
		return contextual_fail("prepared query second set parameters", status);
	}
	auto query_info_result_second = prepared_query->Execute({});
	if (!query_info_result_second.ok()) {
		return contextual_fail("prepared query second execute", query_info_result_second.status());
	}
	auto query_info_second = std::move(query_info_result_second).ValueOrDie();
	auto query_table_result_second = FlightInfoToTable(client, std::move(query_info_second), "Prepared query (second)");
	if (!query_table_result_second.ok()) {
		return fail_with_cleanup(query_table_result_second.status());
	}
	auto query_table_second = query_table_result_second.MoveValueUnsafe();
	if (query_table_second->num_columns() != 2 || query_table_second->num_rows() != 3) {
		return fail_with_cleanup(Status::Invalid("second prepared query returned unexpected shape"));
	}

	status = ClosePreparedStatement(prepared_query);
	if (!status.ok()) {
		return contextual_fail("prepared query close", status);
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
		return contextual_fail("prepared single-row enforcement set parameters", status);
	}
	auto multirow_exec_result = prepared_multirow_query->Execute({});
	if (multirow_exec_result.ok()) {
		return fail_with_cleanup(
		    Status::Invalid("prepared query with multiple parameter rows unexpectedly succeeded"));
	}
	status = ClosePreparedStatement(prepared_multirow_query);
	if (!status.ok()) {
		return contextual_fail("prepared single-row enforcement close", status);
	}

	auto malformed_handle_result = GetPreparedFlightInfoRaw(client, std::string("bad"));
	if (malformed_handle_result.ok()) {
		return fail_with_cleanup(Status::Invalid("malformed prepared handle unexpectedly succeeded"));
	}
	if (malformed_handle_result.status().ToString().find("Invalid prepared statement handle encoding") ==
	    std::string::npos) {
		return fail_with_cleanup(Status::Invalid("malformed handle returned unexpected error: ",
		                                        malformed_handle_result.status().ToString()));
	}

	ARROW_ASSIGN_OR_RAISE(auto closed_handle, CreatePreparedHandleRaw(client, "SELECT 42"));
	raw_handles.push_back(closed_handle);
	status = ClosePreparedHandleRaw(client, closed_handle);
	if (!status.ok()) {
		return contextual_fail("close raw prepared handle", status);
	}
	raw_handles.pop_back();
	auto closed_reuse_result = GetPreparedFlightInfoRaw(client, closed_handle);
	if (closed_reuse_result.ok()) {
		return fail_with_cleanup(Status::Invalid("closed prepared handle unexpectedly remained executable"));
	}
	if (closed_reuse_result.status().ToString().find("Prepared statement not found") == std::string::npos) {
		return fail_with_cleanup(Status::Invalid("closed-handle reuse returned unexpected error: ",
		                                        closed_reuse_result.status().ToString()));
	}

	ARROW_ASSIGN_OR_RAISE(auto shared_handle,
	                      CreatePreparedHandleRaw(client, "SELECT i::BIGINT AS i FROM range(25000) t(i)"));
	raw_handles.push_back(shared_handle);
	ARROW_ASSIGN_OR_RAISE(auto location, Location::ForGrpcTcp(options.host, options.port));
	ARROW_ASSIGN_OR_RAISE(auto flight_client_one, FlightClient::Connect(location));
	ARROW_ASSIGN_OR_RAISE(auto flight_client_two, FlightClient::Connect(location));
	FlightSqlClient client_one(std::move(flight_client_one));
	FlightSqlClient client_two(std::move(flight_client_two));

	Status first_concurrent_status = Status::OK();
	Status second_concurrent_status = Status::OK();
	int64_t first_concurrent_rows = -1;
	int64_t second_concurrent_rows = -1;
	std::thread first_query_thread([&]() {
		auto row_count_result = FetchPreparedRowCountRaw(client_one, shared_handle);
		if (!row_count_result.ok()) {
			first_concurrent_status = row_count_result.status();
			return;
		}
		first_concurrent_rows = row_count_result.ValueOrDie();
	});
	std::this_thread::sleep_for(std::chrono::milliseconds(25));
	std::thread second_query_thread([&]() {
		auto row_count_result = FetchPreparedRowCountRaw(client_two, shared_handle);
		if (!row_count_result.ok()) {
			second_concurrent_status = row_count_result.status();
			return;
		}
		second_concurrent_rows = row_count_result.ValueOrDie();
	});
	first_query_thread.join();
	second_query_thread.join();
	auto close_one_status = client_one.Close();
	auto close_two_status = client_two.Close();
	if (!close_one_status.ok()) {
		return contextual_fail("close concurrent client one", close_one_status);
	}
	if (!close_two_status.ok()) {
		return contextual_fail("close concurrent client two", close_two_status);
	}
	if (!first_concurrent_status.ok()) {
		return fail_with_cleanup(first_concurrent_status);
	}
	if (!second_concurrent_status.ok()) {
		return fail_with_cleanup(second_concurrent_status);
	}
	if (first_concurrent_rows != 25000 || second_concurrent_rows != 25000) {
		return fail_with_cleanup(
		    Status::Invalid("concurrent prepared queries returned unexpected row counts: ", first_concurrent_rows,
		                    ", ", second_concurrent_rows));
	}

	ARROW_ASSIGN_OR_RAISE(auto timeout_enable_result, ExecuteQuery(client, timeout_enable_sql));
	if (!timeout_enable_result || timeout_enable_result->num_rows() != 1) {
		return fail_with_cleanup(Status::Invalid("failed to set transaction timeout to 1 second"));
	}
	ARROW_ASSIGN_OR_RAISE(auto timeout_handle, CreatePreparedHandleRaw(client, "SELECT 123 AS v"));
	raw_handles.push_back(timeout_handle);
	ARROW_ASSIGN_OR_RAISE(auto timeout_before_expiry, GetPreparedFlightInfoRaw(client, timeout_handle));
	if (!timeout_before_expiry || timeout_before_expiry->endpoints().empty()) {
		return fail_with_cleanup(Status::Invalid("timed prepared handle returned no endpoints before expiry"));
	}
	std::this_thread::sleep_for(std::chrono::milliseconds(3500));
	auto timeout_after_expiry = GetPreparedFlightInfoRaw(client, timeout_handle);
	if (timeout_after_expiry.ok()) {
		return fail_with_cleanup(Status::Invalid("timed-out prepared handle unexpectedly remained valid"));
	}
	if (timeout_after_expiry.status().ToString().find("Prepared statement not found") == std::string::npos) {
		return fail_with_cleanup(Status::Invalid("unexpected timed-out prepared-handle error: ",
		                                        timeout_after_expiry.status().ToString()));
	}
	raw_handles.pop_back();

	status = ExecuteUpdate(client, "DROP TABLE flight_prep_it", std::nullopt);
	if (!status.ok()) {
		return contextual_fail("drop flight_prep_it", status);
	}
	cleanup();
	return Status::OK();
}

Status RunTimeout(FlightSqlClient &client) {
	const std::string long_running_query =
	    "SELECT COUNT(*)::BIGINT FROM range(300000000) t(i) WHERE hash(i) % 2 = 0";

	arrow::flight::FlightCallOptions short_timeout;
	short_timeout.timeout = arrow::flight::TimeoutDuration {0.05};
	auto timeout_query_result = ExecuteQueryWithOptions(client, short_timeout, long_running_query);
	if (timeout_query_result.ok()) {
		return Status::Invalid("timeout mode expected long-running query to fail with short deadline");
	}
	if (!IsTimeoutStatus(timeout_query_result.status())) {
		return Status::Invalid("timeout mode long-running query returned non-timeout status: ",
		                      timeout_query_result.status().ToString());
	}

	arrow::flight::FlightCallOptions generous_timeout;
	generous_timeout.timeout = arrow::flight::TimeoutDuration {5.0};
	ARROW_ASSIGN_OR_RAISE(auto control_table, ExecuteQueryWithOptions(client, generous_timeout, "SELECT 1 AS one"));
	if (control_table->num_columns() != 1 || control_table->num_rows() != 1 || control_table->column(0)->num_chunks() != 1) {
		return Status::Invalid("timeout mode control query returned unexpected shape");
	}
	ARROW_ASSIGN_OR_RAISE(auto control_value, GetIntValue(control_table->column(0)->chunk(0), 0));
	if (control_value != 1) {
		return Status::Invalid("timeout mode control query returned unexpected value: ", control_value);
	}
	return Status::OK();
}

Status RunTransaction(FlightSqlClient &client) {
	const std::string create_table_sql = "CREATE TABLE flight_tx_it (id INTEGER, val VARCHAR)";
	const std::string drop_table_sql = "DROP TABLE IF EXISTS flight_tx_it";
	const std::string timeout_reset_sql = "CALL set_flight_sql_transaction_timeout_seconds(1800)";
	const std::string timeout_enable_sql = "CALL set_flight_sql_transaction_timeout_seconds(1)";
	const std::string timeout_disable_sql = "CALL set_flight_sql_transaction_timeout_seconds(0)";

	auto cleanup = [&client, &drop_table_sql, &timeout_reset_sql]() {
		(void)ExecuteUpdate(client, drop_table_sql, std::nullopt);
		(void)ExecuteQuery(client, timeout_reset_sql);
	};
	auto fail_with_cleanup = [&](Status status) {
		cleanup();
		return status;
	};

	ARROW_RETURN_NOT_OK(ExecuteUpdate(client, drop_table_sql, std::nullopt));
	ARROW_RETURN_NOT_OK(ExecuteUpdate(client, create_table_sql, std::nullopt));

	ARROW_ASSIGN_OR_RAISE(auto rollback_tx, client.BeginTransaction({}));
	ARROW_RETURN_NOT_OK(ExecuteUpdateInTransaction(client, "INSERT INTO flight_tx_it VALUES (1, 'a'), (2, 'b')", rollback_tx, 2));
	ARROW_ASSIGN_OR_RAISE(auto rollback_visible, ExecuteQueryInTransaction(
	                                              client,
	                                              "SELECT COUNT(*)::BIGINT AS cnt FROM flight_tx_it WHERE id IN (1, 2)",
	                                              rollback_tx));
	if (rollback_visible->num_columns() != 1 || rollback_visible->num_rows() != 1 || rollback_visible->column(0)->num_chunks() != 1) {
		return fail_with_cleanup(Status::Invalid("transaction rollback visibility query returned unexpected shape"));
	}
	ARROW_ASSIGN_OR_RAISE(auto rollback_visible_count, GetIntValue(rollback_visible->column(0)->chunk(0), 0));
	if (rollback_visible_count != 2) {
		return fail_with_cleanup(Status::Invalid("expected rollback transaction to see 2 rows, got ", rollback_visible_count));
	}
	ARROW_ASSIGN_OR_RAISE(auto outside_before_rollback, QueryCount(client, "SELECT COUNT(*)::BIGINT AS cnt FROM flight_tx_it"));
	if (outside_before_rollback != 0) {
		return fail_with_cleanup(Status::Invalid("uncommitted rows were visible outside rollback transaction"));
	}
	auto rollback_status = client.Rollback({}, rollback_tx);
	if (!rollback_status.ok()) {
		return fail_with_cleanup(rollback_status);
	}
	if (client.Rollback({}, rollback_tx).ok() || client.Commit({}, rollback_tx).ok()) {
		return fail_with_cleanup(Status::Invalid("transaction handle remained valid after rollback"));
	}
	ARROW_ASSIGN_OR_RAISE(auto outside_after_rollback, QueryCount(client, "SELECT COUNT(*)::BIGINT AS cnt FROM flight_tx_it"));
	if (outside_after_rollback != 0) {
		return fail_with_cleanup(Status::Invalid("rollback did not discard uncommitted rows"));
	}

	ARROW_ASSIGN_OR_RAISE(auto commit_tx, client.BeginTransaction({}));
	ARROW_RETURN_NOT_OK(ExecuteUpdateInTransaction(client, "INSERT INTO flight_tx_it VALUES (3, 'c')", commit_tx, 1));
	auto commit_status = client.Commit({}, commit_tx);
	if (!commit_status.ok()) {
		return fail_with_cleanup(commit_status);
	}
	if (client.Commit({}, commit_tx).ok() || client.Rollback({}, commit_tx).ok()) {
		return fail_with_cleanup(Status::Invalid("transaction handle remained valid after commit"));
	}
	ARROW_ASSIGN_OR_RAISE(auto outside_after_commit,
	                      QueryCount(client, "SELECT COUNT(*)::BIGINT AS cnt FROM flight_tx_it WHERE id = 3"));
	if (outside_after_commit != 1) {
		return fail_with_cleanup(Status::Invalid("committed row was not visible after commit"));
	}

	ARROW_ASSIGN_OR_RAISE(auto prepared_tx, client.BeginTransaction({}));
	ARROW_ASSIGN_OR_RAISE(auto prepared_stmt, client.Prepare({}, "INSERT INTO flight_tx_it VALUES (?, ?)", prepared_tx));
	auto parameter_schema = prepared_stmt->parameter_schema();
	if (!parameter_schema || parameter_schema->num_fields() != 2) {
		return fail_with_cleanup(Status::Invalid("unexpected parameter schema for tx-bound prepared statement"));
	}
	ARROW_ASSIGN_OR_RAISE(auto prep_id_array, BuildIntegerArray(parameter_schema->field(0)->type(), {10}));
	ARROW_ASSIGN_OR_RAISE(auto prep_val_array, BuildStringArray(parameter_schema->field(1)->type(), {"tx-prepared"}));
	auto prep_batch = arrow::RecordBatch::Make(parameter_schema, 1, {prep_id_array, prep_val_array});
	auto set_parameters_status = prepared_stmt->SetParameters(prep_batch);
	if (!set_parameters_status.ok()) {
		return fail_with_cleanup(set_parameters_status);
	}
	ARROW_ASSIGN_OR_RAISE(auto prepared_rows_changed, prepared_stmt->ExecuteUpdate({}));
	if (prepared_rows_changed != 1) {
		return fail_with_cleanup(Status::Invalid("tx-bound prepared statement expected 1 affected row, got ",
		                                        prepared_rows_changed));
	}
	ARROW_ASSIGN_OR_RAISE(auto raw_prepared_handle,
	                      CreatePreparedHandleRaw(client, "SELECT id FROM flight_tx_it WHERE id = 10",
	                                              &prepared_tx.transaction_id()));
	ARROW_ASSIGN_OR_RAISE(auto raw_prepared_before_end, GetPreparedFlightInfoRaw(client, raw_prepared_handle));
	if (!raw_prepared_before_end || raw_prepared_before_end->endpoints().empty()) {
		return fail_with_cleanup(Status::Invalid("raw tx-bound prepared statement returned no endpoints"));
	}
	auto prepared_close_status = prepared_stmt->Close();
	if (!prepared_close_status.ok()) {
		return fail_with_cleanup(prepared_close_status);
	}
	auto prepared_rollback_status = client.Rollback({}, prepared_tx);
	if (!prepared_rollback_status.ok()) {
		return fail_with_cleanup(prepared_rollback_status);
	}
	auto raw_prepared_after_end = GetPreparedFlightInfoRaw(client, raw_prepared_handle);
	if (raw_prepared_after_end.ok()) {
		return fail_with_cleanup(Status::Invalid("raw tx-bound prepared statement remained valid after transaction end"));
	}
	if (raw_prepared_after_end.status().ToString().find("Prepared statement not found") == std::string::npos) {
		return fail_with_cleanup(Status::Invalid("unexpected error for tx-bound prepared invalidation: ",
		                                        raw_prepared_after_end.status().ToString()));
	}
	ARROW_ASSIGN_OR_RAISE(auto rolled_back_prepared_row,
	                      QueryCount(client, "SELECT COUNT(*)::BIGINT AS cnt FROM flight_tx_it WHERE id = 10"));
	if (rolled_back_prepared_row != 0) {
		return fail_with_cleanup(Status::Invalid("prepared-in-transaction rollback did not discard inserted row"));
	}

	ARROW_ASSIGN_OR_RAISE(auto timeout_enable_result, ExecuteQuery(client, timeout_enable_sql));
	if (!timeout_enable_result || timeout_enable_result->num_rows() != 1) {
		return fail_with_cleanup(Status::Invalid("failed to set transaction timeout to 1 second"));
	}

	ARROW_ASSIGN_OR_RAISE(auto linked_tx, client.BeginTransaction({}));
	ARROW_ASSIGN_OR_RAISE(auto linked_prepared_handle,
	                      CreatePreparedHandleRaw(client, "SELECT id FROM flight_tx_it WHERE id = 3",
	                                              &linked_tx.transaction_id()));
	for (int i = 0; i < 6; i++) {
		ARROW_ASSIGN_OR_RAISE(auto keepalive_result,
		                      ExecuteQueryInTransaction(client, "SELECT COUNT(*)::BIGINT AS cnt FROM flight_tx_it", linked_tx));
		if (!keepalive_result || keepalive_result->num_rows() != 1) {
			return fail_with_cleanup(Status::Invalid("linked transaction keepalive query failed"));
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(600));
	}
	ARROW_ASSIGN_OR_RAISE(auto linked_prepared_info_before_end, GetPreparedFlightInfoRaw(client, linked_prepared_handle));
	if (!linked_prepared_info_before_end || linked_prepared_info_before_end->endpoints().empty()) {
		return fail_with_cleanup(Status::Invalid("linked transaction prepared handle unexpectedly invalidated"));
	}
	auto linked_rollback_status = client.Rollback({}, linked_tx);
	if (!linked_rollback_status.ok()) {
		return fail_with_cleanup(Status::Invalid("linked transaction rollback failed: ",
		                                        linked_rollback_status.ToString()));
	}
	auto linked_prepared_info_after_end = GetPreparedFlightInfoRaw(client, linked_prepared_handle);
	if (linked_prepared_info_after_end.ok()) {
		return fail_with_cleanup(Status::Invalid("linked transaction prepared handle remained valid after transaction end"));
	}
	if (linked_prepared_info_after_end.status().ToString().find("Prepared statement not found") == std::string::npos) {
		return fail_with_cleanup(Status::Invalid("unexpected linked transaction prepared-handle error: ",
		                                        linked_prepared_info_after_end.status().ToString()));
	}

	ARROW_ASSIGN_OR_RAISE(auto timeout_tx, client.BeginTransaction({}));
	ARROW_RETURN_NOT_OK(ExecuteUpdateInTransaction(client, "INSERT INTO flight_tx_it VALUES (20, 'timeout')", timeout_tx, 1));
	std::this_thread::sleep_for(std::chrono::milliseconds(3500));

	auto timeout_commit_status = client.Commit({}, timeout_tx);
	if (timeout_commit_status.ok()) {
		return fail_with_cleanup(Status::Invalid("timeout transaction unexpectedly committed"));
	}
	if (timeout_commit_status.ToString().find("Transaction not found") == std::string::npos) {
		return fail_with_cleanup(Status::Invalid("transaction timeout did not trigger as expected: ",
		                                        timeout_commit_status.ToString()));
	}
	ARROW_ASSIGN_OR_RAISE(auto timeout_row_count,
	                      QueryCount(client, "SELECT COUNT(*)::BIGINT AS cnt FROM flight_tx_it WHERE id = 20"));
	if (timeout_row_count != 0) {
		return fail_with_cleanup(Status::Invalid("timed-out transaction row was visible after timeout rollback"));
	}

	ARROW_ASSIGN_OR_RAISE(auto timeout_disable_result, ExecuteQuery(client, timeout_disable_sql));
	if (!timeout_disable_result || timeout_disable_result->num_rows() != 1) {
		return fail_with_cleanup(Status::Invalid("failed to disable transaction timeout"));
	}
	ARROW_ASSIGN_OR_RAISE(auto no_timeout_tx, client.BeginTransaction({}));
	ARROW_RETURN_NOT_OK(ExecuteUpdateInTransaction(client, "INSERT INTO flight_tx_it VALUES (30, 'no-timeout')", no_timeout_tx, 1));
	std::this_thread::sleep_for(std::chrono::milliseconds(1500));
	auto no_timeout_commit_status = client.Commit({}, no_timeout_tx);
	if (!no_timeout_commit_status.ok()) {
		return fail_with_cleanup(Status::Invalid("transaction committed failed while timeout was disabled: ",
		                                        no_timeout_commit_status.ToString()));
	}
	ARROW_ASSIGN_OR_RAISE(auto no_timeout_row_count,
	                      QueryCount(client, "SELECT COUNT(*)::BIGINT AS cnt FROM flight_tx_it WHERE id = 30"));
	if (no_timeout_row_count != 1) {
		return fail_with_cleanup(Status::Invalid("transaction did not commit while timeout was disabled"));
	}

	ARROW_ASSIGN_OR_RAISE(auto timeout_reset_result, ExecuteQuery(client, timeout_reset_sql));
	if (!timeout_reset_result || timeout_reset_result->num_rows() != 1) {
		return fail_with_cleanup(Status::Invalid("failed to reset transaction timeout"));
	}
	ARROW_RETURN_NOT_OK(ExecuteUpdate(client, drop_table_sql, std::nullopt));
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
		status = RunPrepared(sql_client, options);
	} else if (options.mode == "transaction") {
		status = RunTransaction(sql_client);
	} else if (options.mode == "timeout") {
		status = RunTimeout(sql_client);
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
