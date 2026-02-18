#include "duckdb_flight_sql_server.hpp"

#include <algorithm>
#include <utility>

#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/result_arrow_wrapper.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_parameter_data.hpp"

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/flight/sql/server.h"

namespace duckdb {
namespace flight {

using arrow::Result;
using arrow::Schema;
using arrow::Status;
using arrow::flight::FlightDataStream;
using arrow::flight::FlightDescriptor;
using arrow::flight::FlightEndpoint;
using arrow::flight::FlightInfo;
using arrow::flight::FlightMessageReader;
using arrow::flight::FlightMetadataWriter;
using arrow::flight::RecordBatchStream;
using arrow::flight::ServerCallContext;
using arrow::flight::SchemaResult;
using arrow::flight::Ticket;
using arrow::flight::sql::ActionClosePreparedStatementRequest;
using arrow::flight::sql::ActionCreatePreparedStatementRequest;
using arrow::flight::sql::ActionCreatePreparedStatementResult;
using arrow::flight::sql::CreateStatementQueryTicket;
using arrow::flight::sql::GetDbSchemas;
using arrow::flight::sql::GetTables;
using arrow::flight::sql::PreparedStatementQuery;
using arrow::flight::sql::PreparedStatementUpdate;
using arrow::flight::sql::SqlInfoOptions;
using arrow::flight::sql::SqlInfoResult;
using arrow::flight::sql::SqlSchema;
using arrow::flight::sql::StatementQuery;
using arrow::flight::sql::StatementQueryTicket;
using arrow::flight::sql::StatementUpdate;

namespace {

struct PreparedParameterDefinition {
	std::string name;
	LogicalType type;
	idx_t position;
};

static Result<int64_t> ExtractChangedRows(QueryResult &result) {
	int64_t rows_changed = 0;
	if (result.properties.return_type == StatementReturnType::CHANGED_ROWS) {
		auto chunk = result.Fetch();
		if (chunk && chunk->size() == 1 && chunk->ColumnCount() == 1) {
			rows_changed = chunk->GetValue(0, 0).GetValue<int64_t>();
		}
	}
	return rows_changed;
}

static Result<Value> ArrowCellToDuckDBValue(const std::shared_ptr<arrow::Array> &array, int64_t row_idx,
                                            const LogicalType &expected_type) {
	if (array->IsNull(row_idx)) {
		if (expected_type.id() == LogicalTypeId::INVALID) {
			return Value(LogicalType::SQLNULL);
		}
		return Value(expected_type);
	}

	switch (array->type_id()) {
	case arrow::Type::BOOL:
		return Value::BOOLEAN(std::static_pointer_cast<arrow::BooleanArray>(array)->Value(row_idx));
	case arrow::Type::INT8:
		return Value::TINYINT(std::static_pointer_cast<arrow::Int8Array>(array)->Value(row_idx));
	case arrow::Type::INT16:
		return Value::SMALLINT(std::static_pointer_cast<arrow::Int16Array>(array)->Value(row_idx));
	case arrow::Type::INT32:
		return Value::INTEGER(std::static_pointer_cast<arrow::Int32Array>(array)->Value(row_idx));
	case arrow::Type::INT64:
		return Value::BIGINT(std::static_pointer_cast<arrow::Int64Array>(array)->Value(row_idx));
	case arrow::Type::UINT8:
		return Value::UTINYINT(std::static_pointer_cast<arrow::UInt8Array>(array)->Value(row_idx));
	case arrow::Type::UINT16:
		return Value::USMALLINT(std::static_pointer_cast<arrow::UInt16Array>(array)->Value(row_idx));
	case arrow::Type::UINT32:
		return Value::UINTEGER(std::static_pointer_cast<arrow::UInt32Array>(array)->Value(row_idx));
	case arrow::Type::UINT64:
		return Value::UBIGINT(std::static_pointer_cast<arrow::UInt64Array>(array)->Value(row_idx));
	case arrow::Type::FLOAT:
		return Value::FLOAT(std::static_pointer_cast<arrow::FloatArray>(array)->Value(row_idx));
	case arrow::Type::DOUBLE:
		return Value::DOUBLE(std::static_pointer_cast<arrow::DoubleArray>(array)->Value(row_idx));
	case arrow::Type::STRING:
		return Value(std::static_pointer_cast<arrow::StringArray>(array)->GetString(row_idx));
	case arrow::Type::LARGE_STRING:
		return Value(std::static_pointer_cast<arrow::LargeStringArray>(array)->GetString(row_idx));
	default:
		return Status::NotImplemented("Unsupported Arrow parameter type: ", array->type()->ToString());
	}
}

static Result<case_insensitive_map_t<BoundParameterData>>
BindBatchRow(const std::shared_ptr<arrow::RecordBatch> &batch, int64_t row_idx,
             const std::vector<PreparedParameterDefinition> &ordered_parameters) {
	if (batch->num_columns() != NumericCast<int64_t>(ordered_parameters.size())) {
		return Status::Invalid("Parameter column count mismatch: expected ", ordered_parameters.size(), ", got ",
		                       batch->num_columns());
	}

	case_insensitive_map_t<BoundParameterData> named_values;
	for (idx_t col_idx = 0; col_idx < ordered_parameters.size(); col_idx++) {
		const auto &parameter = ordered_parameters[col_idx];
		ARROW_ASSIGN_OR_RAISE(auto value, ArrowCellToDuckDBValue(batch->column(NumericCast<int64_t>(col_idx)), row_idx,
		                                                          parameter.type));
		if (parameter.type.id() == LogicalTypeId::INVALID) {
			named_values[parameter.name] = BoundParameterData(std::move(value));
		} else {
			named_values[parameter.name] = BoundParameterData(std::move(value), parameter.type);
		}
	}
	return named_values;
}

static Result<std::vector<case_insensitive_map_t<BoundParameterData>>>
ReadBoundParameterRows(FlightMessageReader *reader, const std::vector<PreparedParameterDefinition> &ordered_parameters) {
	std::vector<case_insensitive_map_t<BoundParameterData>> rows;
	while (true) {
		ARROW_ASSIGN_OR_RAISE(auto chunk, reader->Next());
		if (!chunk.data) {
			break;
		}
		if (ordered_parameters.empty()) {
			if (chunk.data->num_columns() != 0 || chunk.data->num_rows() != 0) {
				return Status::Invalid("Prepared statement has no parameters but parameter rows were provided");
			}
			continue;
		}
		if (chunk.data->num_columns() != NumericCast<int64_t>(ordered_parameters.size())) {
			return Status::Invalid("Parameter column count mismatch: expected ", ordered_parameters.size(), ", got ",
			                       chunk.data->num_columns());
		}
		for (int64_t row_idx = 0; row_idx < chunk.data->num_rows(); row_idx++) {
			ARROW_ASSIGN_OR_RAISE(auto named_values, BindBatchRow(chunk.data, row_idx, ordered_parameters));
			rows.push_back(std::move(named_values));
		}
	}
	return rows;
}

static std::vector<PreparedParameterDefinition> BuildOrderedParameters(PreparedStatement &prepared) {
	auto expected_types = prepared.GetExpectedParameterTypes();
	std::vector<PreparedParameterDefinition> ordered_parameters;
	ordered_parameters.reserve(prepared.named_param_map.size());
	for (auto &entry : prepared.named_param_map) {
		LogicalType parameter_type = LogicalType::SQLNULL;
		auto type_entry = expected_types.find(entry.first);
		if (type_entry != expected_types.end() && type_entry->second.id() != LogicalTypeId::INVALID) {
			parameter_type = type_entry->second;
		}
		ordered_parameters.push_back({entry.first, std::move(parameter_type), entry.second});
	}
	std::sort(ordered_parameters.begin(), ordered_parameters.end(),
	          [](const PreparedParameterDefinition &lhs, const PreparedParameterDefinition &rhs) {
		          if (lhs.position != rhs.position) {
			          return lhs.position < rhs.position;
		          }
		          return lhs.name < rhs.name;
	          });
	return ordered_parameters;
}

} // namespace

struct DuckDBFlightSqlServer::PreparedStatementState {
	std::string query;
	std::vector<PreparedParameterDefinition> ordered_parameters;
	std::shared_ptr<Schema> dataset_schema;
	std::optional<case_insensitive_map_t<BoundParameterData>> query_bound_parameters;
	std::mutex mutex;
};

DuckDBFlightSqlServer::DuckDBFlightSqlServer(shared_ptr<DatabaseInstance> db_instance) : db(std::move(db_instance)) {
	RegisterSqlInfo(SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_NAME, SqlInfoResult(std::string("duckdb-flight-sql")));
	RegisterSqlInfo(SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_VERSION, SqlInfoResult(std::string(DuckDB::LibraryVersion())));
	RegisterSqlInfo(SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_ARROW_VERSION, SqlInfoResult(std::string("unknown")));
	RegisterSqlInfo(SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_READ_ONLY, SqlInfoResult(false));
	RegisterSqlInfo(SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_SQL, SqlInfoResult(true));
	RegisterSqlInfo(SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_SUBSTRAIT, SqlInfoResult(false));
	RegisterSqlInfo(SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_TRANSACTION,
	                SqlInfoResult(int64_t(SqlInfoOptions::SqlSupportedTransaction::SQL_SUPPORTED_TRANSACTION_NONE)));
	RegisterSqlInfo(SqlInfoOptions::SqlInfo::SQL_DDL_CATALOG, SqlInfoResult(true));
	RegisterSqlInfo(SqlInfoOptions::SqlInfo::SQL_DDL_SCHEMA, SqlInfoResult(true));
	RegisterSqlInfo(SqlInfoOptions::SqlInfo::SQL_DDL_TABLE, SqlInfoResult(true));
	RegisterSqlInfo(SqlInfoOptions::SqlInfo::SQL_IDENTIFIER_CASE,
	                SqlInfoResult(int64_t(SqlInfoOptions::SqlSupportedCaseSensitivity::SQL_CASE_SENSITIVITY_CASE_INSENSITIVE)));
	RegisterSqlInfo(SqlInfoOptions::SqlInfo::SQL_IDENTIFIER_QUOTE_CHAR, SqlInfoResult(std::string("\"")));
	RegisterSqlInfo(SqlInfoOptions::SqlInfo::SQL_QUOTED_IDENTIFIER_CASE,
	                SqlInfoResult(int64_t(SqlInfoOptions::SqlSupportedCaseSensitivity::SQL_CASE_SENSITIVITY_CASE_INSENSITIVE)));
	RegisterSqlInfo(SqlInfoOptions::SqlInfo::SQL_ALL_TABLES_ARE_SELECTABLE, SqlInfoResult(true));
	RegisterSqlInfo(SqlInfoOptions::SqlInfo::SQL_NULL_ORDERING,
	                SqlInfoResult(int64_t(SqlInfoOptions::SqlNullOrdering::SQL_NULLS_SORTED_AT_END)));
	RegisterSqlInfo(SqlInfoOptions::SqlInfo::SQL_SEARCH_STRING_ESCAPE, SqlInfoResult(std::string("\\")));
}

Result<ActionCreatePreparedStatementResult> DuckDBFlightSqlServer::CreatePreparedStatement(
    const ServerCallContext & /*context*/, const ActionCreatePreparedStatementRequest &request) {
	Connection conn(*db);
	auto prepared = conn.Prepare(request.query);
	if (!prepared || prepared->HasError()) {
		return Status::Invalid(prepared ? prepared->GetError() : "Unknown DuckDB prepared statement failure");
	}

	auto ordered_parameters = BuildOrderedParameters(*prepared);

	auto client_properties = conn.context->GetClientProperties();
	ARROW_ASSIGN_OR_RAISE(auto dataset_schema,
	                      DuckDBSchemaToArrow(prepared->GetTypes(), prepared->GetNames(), client_properties));

	vector<LogicalType> parameter_types;
	vector<std::string> parameter_names;
	parameter_types.reserve(ordered_parameters.size());
	parameter_names.reserve(ordered_parameters.size());
	for (auto &parameter : ordered_parameters) {
		parameter_types.push_back(parameter.type);
		parameter_names.push_back(parameter.name);
	}
	ARROW_ASSIGN_OR_RAISE(auto parameter_schema, DuckDBSchemaToArrow(parameter_types, parameter_names, client_properties));

	auto state = std::make_shared<PreparedStatementState>();
	state->query = request.query;
	state->ordered_parameters = std::move(ordered_parameters);
	state->dataset_schema = dataset_schema;
	state->query_bound_parameters.reset();

	auto handle = GeneratePreparedHandle();
	{
		std::lock_guard<std::mutex> guard(prepared_statements_mutex);
		prepared_statements[handle] = state;
	}

	return ActionCreatePreparedStatementResult {std::move(dataset_schema), std::move(parameter_schema), std::move(handle)};
}

Status DuckDBFlightSqlServer::ClosePreparedStatement(const ServerCallContext & /*context*/,
                                                     const ActionClosePreparedStatementRequest &request) {
	std::lock_guard<std::mutex> guard(prepared_statements_mutex);
	auto erased_count = prepared_statements.erase(request.prepared_statement_handle);
	if (erased_count == 0) {
		return Status::Invalid("Prepared statement not found");
	}
	return Status::OK();
}

Result<std::unique_ptr<FlightInfo>> DuckDBFlightSqlServer::GetFlightInfoForSchema(const FlightDescriptor &descriptor,
                                                                                   const std::shared_ptr<Schema> &schema,
                                                                                   bool ordered) {
	std::vector<FlightEndpoint> endpoints {FlightEndpoint {Ticket {descriptor.cmd}, {}, std::nullopt, ""}};
	ARROW_ASSIGN_OR_RAISE(auto info, FlightInfo::Make(*schema, descriptor, endpoints, -1, -1, ordered));
	return std::make_unique<FlightInfo>(std::move(info));
}

Result<std::shared_ptr<DuckDBFlightSqlServer::PreparedStatementState>>
DuckDBFlightSqlServer::LookupPreparedStatement(const std::string &handle) {
	std::lock_guard<std::mutex> guard(prepared_statements_mutex);
	auto entry = prepared_statements.find(handle);
	if (entry == prepared_statements.end()) {
		return Status::Invalid("Prepared statement not found");
	}
	return entry->second;
}

std::string DuckDBFlightSqlServer::GeneratePreparedHandle() {
	auto next = prepared_statement_counter.fetch_add(1, std::memory_order_relaxed) + 1;
	return StringUtil::Format("duckdb_flight_prepared_%llu", static_cast<unsigned long long>(next));
}

Result<std::shared_ptr<Schema>> DuckDBFlightSqlServer::DuckDBSchemaToArrow(const vector<LogicalType> &types,
                                                                            const vector<std::string> &names,
                                                                            ClientProperties client_properties) {
	ArrowSchema schema;
	schema.Init();
	ArrowConverter::ToArrowSchema(&schema, types, names, client_properties);
	auto schema_result = arrow::ImportSchema(reinterpret_cast<struct ArrowSchema *>(&schema));
	if (schema.release) {
		schema.release(&schema);
	}
	if (!schema_result.ok()) {
		return schema_result.status();
	}
	return schema_result.ValueOrDie();
}

Result<std::unique_ptr<FlightDataStream>> DuckDBFlightSqlServer::ResultToFlightStream(unique_ptr<QueryResult> result,
                                                                                       idx_t batch_size) {
	auto *stream_wrapper = new ResultArrowArrayStreamWrapper(std::move(result), batch_size);
	auto *stream = reinterpret_cast<ArrowArrayStream *>(&stream_wrapper->stream);
	auto reader_result = arrow::ImportRecordBatchReader(stream);
	if (!reader_result.ok()) {
		if (stream_wrapper->stream.release) {
			stream_wrapper->stream.release(&stream_wrapper->stream);
		} else {
			delete stream_wrapper;
		}
		return reader_result.status();
	}
	return std::make_unique<RecordBatchStream>(std::move(reader_result).ValueOrDie());
}

Result<std::unique_ptr<FlightDataStream>> DuckDBFlightSqlServer::StreamSQL(const std::string &sql, idx_t batch_size) {
	Connection conn(*db);
	auto result = conn.SendQuery(sql);
	if (!result || result->HasError()) {
		return Status::Invalid(result ? result->GetError() : "Unknown DuckDB query failure");
	}
	return ResultToFlightStream(std::move(result), batch_size);
}

Result<std::unique_ptr<FlightInfo>> DuckDBFlightSqlServer::GetFlightInfoStatement(const ServerCallContext & /*context*/,
                                                                                   const StatementQuery &command,
                                                                                   const FlightDescriptor &descriptor) {
	Connection conn(*db);
	auto result = conn.SendQuery(command.query);
	if (!result || result->HasError()) {
		return Status::Invalid(result ? result->GetError() : "Unknown DuckDB query failure");
	}

	ARROW_ASSIGN_OR_RAISE(auto schema_result,
	                      DuckDBSchemaToArrow(result->types, result->names, result->client_properties));

	ARROW_ASSIGN_OR_RAISE(auto ticket_str, CreateStatementQueryTicket(command.query));
	std::vector<FlightEndpoint> endpoints {FlightEndpoint {Ticket {std::move(ticket_str)}, {}, std::nullopt, ""}};
	ARROW_ASSIGN_OR_RAISE(auto info, FlightInfo::Make(*schema_result, descriptor, endpoints, -1, -1, false));
	return std::make_unique<FlightInfo>(std::move(info));
}

Result<std::unique_ptr<FlightDataStream>> DuckDBFlightSqlServer::DoGetStatement(const ServerCallContext & /*context*/,
                                                                                const StatementQueryTicket &command) {
	return StreamSQL(command.statement_handle);
}

Result<std::unique_ptr<FlightInfo>> DuckDBFlightSqlServer::GetFlightInfoPreparedStatement(
    const ServerCallContext & /*context*/, const PreparedStatementQuery &command, const FlightDescriptor &descriptor) {
	ARROW_ASSIGN_OR_RAISE(auto state, LookupPreparedStatement(command.prepared_statement_handle));
	std::shared_ptr<Schema> dataset_schema;
	{
		std::lock_guard<std::mutex> guard(state->mutex);
		dataset_schema = state->dataset_schema;
	}
	return GetFlightInfoForSchema(descriptor, dataset_schema);
}

Result<std::unique_ptr<SchemaResult>> DuckDBFlightSqlServer::GetSchemaPreparedStatement(
    const ServerCallContext & /*context*/, const PreparedStatementQuery &command,
    const FlightDescriptor & /*descriptor*/) {
	ARROW_ASSIGN_OR_RAISE(auto state, LookupPreparedStatement(command.prepared_statement_handle));
	std::shared_ptr<Schema> dataset_schema;
	{
		std::lock_guard<std::mutex> guard(state->mutex);
		dataset_schema = state->dataset_schema;
	}
	return SchemaResult::Make(*dataset_schema);
}

Result<std::unique_ptr<FlightDataStream>> DuckDBFlightSqlServer::DoGetPreparedStatement(
    const ServerCallContext & /*context*/, const PreparedStatementQuery &command) {
	ARROW_ASSIGN_OR_RAISE(auto state, LookupPreparedStatement(command.prepared_statement_handle));

	std::string query;
	std::vector<PreparedParameterDefinition> ordered_parameters;
	std::optional<case_insensitive_map_t<BoundParameterData>> query_bound_parameters;
	{
		std::lock_guard<std::mutex> guard(state->mutex);
		query = state->query;
		ordered_parameters = state->ordered_parameters;
		query_bound_parameters = state->query_bound_parameters;
	}

	if (!ordered_parameters.empty() && !query_bound_parameters.has_value()) {
		return Status::Invalid("No parameter binding found for prepared statement query");
	}

	Connection conn(*db);
	auto prepared = conn.Prepare(query);
	if (!prepared || prepared->HasError()) {
		return Status::Invalid(prepared ? prepared->GetError() : "Unknown DuckDB prepared statement failure");
	}

	case_insensitive_map_t<BoundParameterData> named_values;
	if (query_bound_parameters.has_value()) {
		named_values = query_bound_parameters.value();
	}
	auto result = prepared->Execute(named_values, true);
	if (!result || result->HasError()) {
		return Status::Invalid(result ? result->GetError() : "Unknown DuckDB query failure");
	}
	return ResultToFlightStream(std::move(result));
}

Status DuckDBFlightSqlServer::DoPutPreparedStatementQuery(const ServerCallContext & /*context*/,
                                                          const PreparedStatementQuery &command,
                                                          FlightMessageReader *reader,
                                                          FlightMetadataWriter *writer) {
	ARROW_ASSIGN_OR_RAISE(auto state, LookupPreparedStatement(command.prepared_statement_handle));

	std::vector<PreparedParameterDefinition> ordered_parameters;
	{
		std::lock_guard<std::mutex> guard(state->mutex);
		ordered_parameters = state->ordered_parameters;
	}

	ARROW_ASSIGN_OR_RAISE(auto bound_rows, ReadBoundParameterRows(reader, ordered_parameters));
	if (bound_rows.size() > 1) {
		return Status::Invalid("Prepared statement query expects at most one bound parameter row");
	}

	{
		std::lock_guard<std::mutex> guard(state->mutex);
		if (ordered_parameters.empty()) {
			state->query_bound_parameters = case_insensitive_map_t<BoundParameterData> {};
		} else if (bound_rows.empty()) {
			state->query_bound_parameters.reset();
		} else {
			state->query_bound_parameters = std::move(bound_rows[0]);
		}
	}
	(void)writer;
	return Status::OK();
}

Result<int64_t> DuckDBFlightSqlServer::DoPutPreparedStatementUpdate(const ServerCallContext & /*context*/,
                                                                    const PreparedStatementUpdate &command,
                                                                    FlightMessageReader *reader) {
	ARROW_ASSIGN_OR_RAISE(auto state, LookupPreparedStatement(command.prepared_statement_handle));

	std::string query;
	std::vector<PreparedParameterDefinition> ordered_parameters;
	{
		std::lock_guard<std::mutex> guard(state->mutex);
		query = state->query;
		ordered_parameters = state->ordered_parameters;
	}

	Connection conn(*db);
	auto prepared = conn.Prepare(query);
	if (!prepared || prepared->HasError()) {
		return Status::Invalid(prepared ? prepared->GetError() : "Unknown DuckDB prepared statement failure");
	}

	int64_t rows_changed = 0;
	if (ordered_parameters.empty()) {
		case_insensitive_map_t<BoundParameterData> empty_parameters;
		auto result = prepared->Execute(empty_parameters, false);
		if (!result || result->HasError()) {
			return Status::Invalid(result ? result->GetError() : "Unknown DuckDB query failure");
		}
		ARROW_ASSIGN_OR_RAISE(rows_changed, ExtractChangedRows(*result));
		return rows_changed;
	}

	ARROW_ASSIGN_OR_RAISE(auto bound_rows, ReadBoundParameterRows(reader, ordered_parameters));
	if (bound_rows.empty()) {
		return Status::Invalid("Prepared statement update expected at least one bound parameter row");
	}

	for (auto &named_values : bound_rows) {
		auto result = prepared->Execute(named_values, false);
		if (!result || result->HasError()) {
			return Status::Invalid(result ? result->GetError() : "Unknown DuckDB query failure");
		}
		ARROW_ASSIGN_OR_RAISE(auto changed_rows, ExtractChangedRows(*result));
		rows_changed += changed_rows;
	}
	return rows_changed;
}

Result<int64_t> DuckDBFlightSqlServer::DoPutCommandStatementUpdate(const ServerCallContext & /*context*/,
                                                                  const StatementUpdate &command) {
	Connection conn(*db);
	auto result = conn.Query(command.query);
	if (!result || result->HasError()) {
		return Status::Invalid(result ? result->GetError() : "Unknown DuckDB query failure");
	}

	ARROW_ASSIGN_OR_RAISE(auto rows_changed, ExtractChangedRows(*result));
	return rows_changed;
}

Result<std::unique_ptr<FlightInfo>> DuckDBFlightSqlServer::GetFlightInfoCatalogs(const ServerCallContext & /*context*/,
                                                                                 const FlightDescriptor &descriptor) {
	return GetFlightInfoForSchema(descriptor, SqlSchema::GetCatalogsSchema());
}

Result<std::unique_ptr<FlightDataStream>> DuckDBFlightSqlServer::DoGetCatalogs(const ServerCallContext & /*context*/) {
	return StreamSQL("SELECT database_name AS catalog_name FROM duckdb_databases() ORDER BY catalog_name");
}

Result<std::unique_ptr<FlightInfo>> DuckDBFlightSqlServer::GetFlightInfoSchemas(const ServerCallContext & /*context*/,
                                                                                const GetDbSchemas & /*command*/,
                                                                                const FlightDescriptor &descriptor) {
	return GetFlightInfoForSchema(descriptor, SqlSchema::GetDbSchemasSchema());
}

Result<std::unique_ptr<FlightDataStream>> DuckDBFlightSqlServer::DoGetDbSchemas(const ServerCallContext & /*context*/,
                                                                                const GetDbSchemas &command) {
	std::string sql =
	    "SELECT database_name AS catalog_name, schema_name AS db_schema_name "
	    "FROM duckdb_schemas() WHERE 1=1";
	if (command.catalog.has_value()) {
		sql += StringUtil::Format(" AND database_name = '%s'", StringUtil::Replace(command.catalog.value(), "'", "''"));
	}
	if (command.db_schema_filter_pattern.has_value()) {
		sql += StringUtil::Format(" AND schema_name LIKE '%s'", StringUtil::Replace(command.db_schema_filter_pattern.value(), "'", "''"));
	}
	sql += " ORDER BY catalog_name, db_schema_name";
	return StreamSQL(sql);
}

Result<std::unique_ptr<FlightInfo>> DuckDBFlightSqlServer::GetFlightInfoTables(const ServerCallContext & /*context*/,
                                                                               const GetTables &command,
                                                                               const FlightDescriptor &descriptor) {
	if (command.include_schema) {
		return Status::NotImplemented("GetTables(include_schema=true) is not implemented in this version");
	}
	return GetFlightInfoForSchema(descriptor, SqlSchema::GetTablesSchema());
}

Result<std::unique_ptr<FlightDataStream>> DuckDBFlightSqlServer::DoGetTables(const ServerCallContext & /*context*/,
                                                                             const GetTables &command) {
	if (command.include_schema) {
		return Status::NotImplemented("DoGetTables(include_schema=true) is not implemented in this version");
	}

	std::string sql =
	    "WITH all_tables AS ("
	    "  SELECT database_name AS catalog_name, schema_name AS db_schema_name, table_name, 'TABLE' AS table_type"
	    "  FROM duckdb_tables()"
	    "  UNION ALL "
	    "  SELECT database_name AS catalog_name, schema_name AS db_schema_name, view_name AS table_name, 'VIEW' AS table_type"
	    "  FROM duckdb_views()"
	    ") SELECT catalog_name, db_schema_name, table_name, table_type FROM all_tables WHERE 1=1";

	if (command.catalog.has_value()) {
		sql += StringUtil::Format(" AND catalog_name = '%s'", StringUtil::Replace(command.catalog.value(), "'", "''"));
	}
	if (command.db_schema_filter_pattern.has_value()) {
		sql += StringUtil::Format(" AND db_schema_name LIKE '%s'",
		                         StringUtil::Replace(command.db_schema_filter_pattern.value(), "'", "''"));
	}
	if (command.table_name_filter_pattern.has_value()) {
		sql += StringUtil::Format(" AND table_name LIKE '%s'",
		                         StringUtil::Replace(command.table_name_filter_pattern.value(), "'", "''"));
	}
	if (!command.table_types.empty()) {
		sql += " AND table_type IN (";
		for (idx_t i = 0; i < command.table_types.size(); i++) {
			if (i > 0) {
				sql += ", ";
			}
			sql += StringUtil::Format("'%s'", StringUtil::Replace(command.table_types[i], "'", "''"));
		}
		sql += ")";
	}
	sql += " ORDER BY catalog_name, db_schema_name, table_name";
	return StreamSQL(sql);
}

Result<std::unique_ptr<FlightInfo>> DuckDBFlightSqlServer::GetFlightInfoTableTypes(const ServerCallContext & /*context*/,
                                                                                   const FlightDescriptor &descriptor) {
	return GetFlightInfoForSchema(descriptor, SqlSchema::GetTableTypesSchema());
}

Result<std::unique_ptr<FlightDataStream>> DuckDBFlightSqlServer::DoGetTableTypes(const ServerCallContext & /*context*/) {
	return StreamSQL("SELECT table_type FROM (VALUES ('TABLE'), ('VIEW')) t(table_type)");
}

} // namespace flight
} // namespace duckdb
