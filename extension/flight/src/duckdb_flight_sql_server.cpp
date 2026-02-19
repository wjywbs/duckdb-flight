#include "duckdb_flight_sql_server.hpp"

#include <algorithm>
#include <cstdint>
#include <functional>
#include <limits>
#include <optional>
#include <utility>

#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/result_arrow_wrapper.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/exception.hpp"
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
using arrow::flight::sql::ActionBeginSavepointRequest;
using arrow::flight::sql::ActionBeginSavepointResult;
using arrow::flight::sql::ActionBeginTransactionRequest;
using arrow::flight::sql::ActionBeginTransactionResult;
using arrow::flight::sql::ActionClosePreparedStatementRequest;
using arrow::flight::sql::ActionCreatePreparedStatementRequest;
using arrow::flight::sql::ActionCreatePreparedStatementResult;
using arrow::flight::sql::ActionEndSavepointRequest;
using arrow::flight::sql::ActionEndTransactionRequest;
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

struct DecodedStatementHandle {
	std::string query;
	std::optional<uint64_t> transaction_id;
};

static uint64_t CurrentTimeMillis() {
	return NumericCast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
	                                 std::chrono::steady_clock::now().time_since_epoch())
	                                 .count());
}

static std::string EncodeHandleLE(const uint64_t handle_id) {
	std::string encoded(8, '\0');
	for (idx_t i = 0; i < 8; i++) {
		encoded[i] = static_cast<char>((handle_id >> (i * 8)) & 0xFF);
	}
	return encoded;
}

static Result<uint64_t> DecodeHandleLE(const std::string &encoded_handle, const char *error_message) {
	if (encoded_handle.size() != 8) {
		return Status::Invalid(error_message);
	}
	uint64_t value = 0;
	for (idx_t i = 0; i < 8; i++) {
		const auto byte_value = static_cast<uint8_t>(encoded_handle[i]);
		value |= static_cast<uint64_t>(byte_value) << (i * 8);
	}
	return value;
}

static std::string EncodeStatementHandle(const std::string &sql, const std::optional<uint64_t> &transaction_id) {
	std::string encoded;
	encoded.reserve(1 + sql.size() + (transaction_id.has_value() ? 8 : 0));
	encoded.push_back(transaction_id.has_value() ? '\x01' : '\x00');
	if (transaction_id.has_value()) {
		encoded += EncodeHandleLE(transaction_id.value());
	}
	encoded += sql;
	return encoded;
}

static Result<DecodedStatementHandle> DecodeStatementHandle(const std::string &statement_handle) {
	if (statement_handle.empty()) {
		return Status::Invalid("Invalid statement ticket encoding");
	}
	DecodedStatementHandle decoded;
	const auto flag = static_cast<uint8_t>(statement_handle[0]);
	if (flag == 0) {
		decoded.query = statement_handle.substr(1);
		return decoded;
	}
	if (flag != 1 || statement_handle.size() < 9) {
		return Status::Invalid("Invalid statement ticket encoding");
	}
	ARROW_ASSIGN_OR_RAISE(decoded.transaction_id, DecodeHandleLE(statement_handle.substr(1, 8), "Invalid statement ticket encoding"));
	decoded.query = statement_handle.substr(9);
	return decoded;
}

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

class LockedFlightDataStream : public FlightDataStream {
public:
	LockedFlightDataStream(std::unique_ptr<FlightDataStream> inner_p, std::unique_lock<std::shared_mutex> lock_p,
	                       std::shared_ptr<void> state_guard_p)
	    : inner(std::move(inner_p)), lock(std::move(lock_p)), state_guard(std::move(state_guard_p)) {
	}

	~LockedFlightDataStream() override {
		Finalize();
	}

	std::shared_ptr<Schema> schema() override {
		return inner->schema();
	}

	Result<arrow::flight::FlightPayload> GetSchemaPayload() override {
		return inner->GetSchemaPayload();
	}

	Result<arrow::flight::FlightPayload> Next() override {
		return inner->Next();
	}

	Status Close() override {
		auto status = inner->Close();
		Finalize();
		return status;
	}

private:
	void Finalize() {
		if (finalized) {
			return;
		}
		finalized = true;
		if (lock.owns_lock()) {
			lock.unlock();
		}
	}

	std::unique_ptr<FlightDataStream> inner;
	std::unique_lock<std::shared_mutex> lock;
	std::shared_ptr<void> state_guard;
	bool finalized = false;
};

} // namespace

struct DuckDBFlightSqlServer::TransactionState {
	unique_ptr<Connection> connection;
	std::unordered_set<uint64_t> owned_prepared_handles;
	std::shared_mutex mutex;
	std::atomic<uint64_t> last_activity_ms {0};

	void UpdateActivityTime() {
		last_activity_ms.store(CurrentTimeMillis(), std::memory_order_relaxed);
	}
};

struct DuckDBFlightSqlServer::PreparedStatementState {
	std::vector<PreparedParameterDefinition> ordered_parameters;
	std::shared_ptr<Schema> dataset_schema;
	std::optional<case_insensitive_map_t<BoundParameterData>> query_bound_parameters;
	unique_ptr<Connection> connection;
	unique_ptr<PreparedStatement> prepared;
	std::optional<uint64_t> transaction_owner;
	std::shared_mutex mutex;
};

DuckDBFlightSqlServer::DuckDBFlightSqlServer(shared_ptr<DatabaseInstance> db_instance) : db(std::move(db_instance)) {
	RegisterSqlInfo(SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_NAME, SqlInfoResult(std::string("duckdb-flight-sql")));
	RegisterSqlInfo(SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_VERSION, SqlInfoResult(std::string(DuckDB::LibraryVersion())));
	RegisterSqlInfo(SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_ARROW_VERSION, SqlInfoResult(std::string(ARROW_VERSION_STRING)));
	RegisterSqlInfo(SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_READ_ONLY, SqlInfoResult(false));
	RegisterSqlInfo(SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_SQL, SqlInfoResult(true));
	RegisterSqlInfo(SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_SUBSTRAIT, SqlInfoResult(false));
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
	UpdateTransactionSqlInfo();
}

DuckDBFlightSqlServer::~DuckDBFlightSqlServer() {
}

void DuckDBFlightSqlServer::StartFlightSqlState() {
	bool expected = false;
	if (!flight_sql_state_started.compare_exchange_strong(expected, true)) {
		return;
	}
	StartTransactionSweeper();
}

Status DuckDBFlightSqlServer::ShutdownFlightSqlState() {
	bool expected = true;
	if (!flight_sql_state_started.compare_exchange_strong(expected, false)) {
		return Status::OK();
	}
	StopTransactionSweeper();
	RollbackAllTransactions();
	return Status::OK();
}

void DuckDBFlightSqlServer::SetTransactionTimeoutSeconds(int64_t timeout_seconds) {
	transaction_timeout_seconds.store(timeout_seconds, std::memory_order_relaxed);
	UpdateTransactionSqlInfo();
	sweeper_cv.notify_all();
}

int64_t DuckDBFlightSqlServer::GetTransactionTimeoutSeconds() const {
	return transaction_timeout_seconds.load(std::memory_order_relaxed);
}

Result<ActionBeginTransactionResult> DuckDBFlightSqlServer::BeginTransaction(const ServerCallContext & /*context*/,
                                                                             const ActionBeginTransactionRequest & /*request*/) {
	auto transaction_state = std::make_shared<TransactionState>();
	transaction_state->connection = make_uniq<Connection>(*db);
	try {
		transaction_state->connection->BeginTransaction();
	} catch (std::exception &ex) {
		return Status::Invalid(ex.what());
	}
	transaction_state->UpdateActivityTime();

	auto handle = GenerateTransactionHandle();
	ARROW_ASSIGN_OR_RAISE(auto transaction_id, DecodeTransactionHandle(handle));
	{
		std::lock_guard<std::mutex> guard(transactions_mutex);
		transactions[transaction_id] = transaction_state;
	}
	return ActionBeginTransactionResult {std::move(handle)};
}

Status DuckDBFlightSqlServer::EndTransaction(const ServerCallContext & /*context*/,
                                             const ActionEndTransactionRequest &request) {
	ARROW_ASSIGN_OR_RAISE(auto transaction_id, DecodeTransactionHandle(request.transaction_id));
	std::shared_ptr<TransactionState> transaction_state;
	{
		std::lock_guard<std::mutex> guard(transactions_mutex);
		auto entry = transactions.find(transaction_id);
		if (entry == transactions.end()) {
			return Status::Invalid("Transaction not found");
		}
		transaction_state = entry->second;
		transactions.erase(entry);
	}

	const auto commit = request.action == ActionEndTransactionRequest::kCommit;
	return FinalizeTransaction(transaction_state, commit);
}

Result<ActionBeginSavepointResult> DuckDBFlightSqlServer::BeginSavepoint(const ServerCallContext & /*context*/,
                                                                          const ActionBeginSavepointRequest & /*request*/) {
	return Status::NotImplemented("BeginSavepoint is not implemented");
}

Status DuckDBFlightSqlServer::EndSavepoint(const ServerCallContext & /*context*/,
                                           const ActionEndSavepointRequest & /*request*/) {
	return Status::NotImplemented("EndSavepoint is not implemented");
}

Result<ActionCreatePreparedStatementResult> DuckDBFlightSqlServer::CreatePreparedStatement(
    const ServerCallContext & /*context*/, const ActionCreatePreparedStatementRequest &request) {
	auto state = std::make_shared<PreparedStatementState>();
	std::shared_ptr<TransactionState> transaction_state;
	std::unique_lock<std::shared_mutex> transaction_lock;

	if (!request.transaction_id.empty()) {
		ARROW_ASSIGN_OR_RAISE(auto transaction_id, DecodeTransactionHandle(request.transaction_id));
		ARROW_ASSIGN_OR_RAISE(transaction_state, LookupTransaction(transaction_id));
		transaction_lock = std::unique_lock<std::shared_mutex>(transaction_state->mutex);
		state->transaction_owner = transaction_id;
		state->prepared = transaction_state->connection->Prepare(request.query);
	} else {
		state->connection = make_uniq<Connection>(*db);
		state->prepared = state->connection->Prepare(request.query);
	}

	if (!state->prepared || state->prepared->HasError()) {
		return Status::Invalid(state->prepared ? state->prepared->GetError() : "Unknown DuckDB prepared statement failure");
	}

	auto ordered_parameters = BuildOrderedParameters(*state->prepared);
	ClientProperties client_properties;
	if (transaction_state) {
		client_properties = transaction_state->connection->context->GetClientProperties();
	} else {
		client_properties = state->connection->context->GetClientProperties();
	}

	ARROW_ASSIGN_OR_RAISE(auto dataset_schema,
	                      DuckDBSchemaToArrow(state->prepared->GetTypes(), state->prepared->GetNames(), client_properties));

	vector<LogicalType> parameter_types;
	vector<std::string> parameter_names;
	parameter_types.reserve(ordered_parameters.size());
	parameter_names.reserve(ordered_parameters.size());
	for (auto &parameter : ordered_parameters) {
		parameter_types.push_back(parameter.type);
		parameter_names.push_back(parameter.name);
	}
	ARROW_ASSIGN_OR_RAISE(auto parameter_schema, DuckDBSchemaToArrow(parameter_types, parameter_names, client_properties));

	state->ordered_parameters = std::move(ordered_parameters);
	state->dataset_schema = dataset_schema;
	state->query_bound_parameters.reset();

	auto handle = GeneratePreparedHandle();
	ARROW_ASSIGN_OR_RAISE(auto handle_id, DecodePreparedHandle(handle));
	{
		std::lock_guard<std::mutex> guard(prepared_statements_mutex);
		prepared_statements[handle_id] = state;
	}
	if (transaction_state) {
		transaction_state->owned_prepared_handles.insert(handle_id);
		transaction_state->UpdateActivityTime();
	}

	return ActionCreatePreparedStatementResult {std::move(dataset_schema), std::move(parameter_schema), std::move(handle)};
}

Status DuckDBFlightSqlServer::ClosePreparedStatement(const ServerCallContext & /*context*/,
                                                     const ActionClosePreparedStatementRequest &request) {
	ARROW_ASSIGN_OR_RAISE(auto handle_id, DecodePreparedHandle(request.prepared_statement_handle));
	return RemovePreparedStatement(handle_id, true);
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
	ARROW_ASSIGN_OR_RAISE(auto handle_id, DecodePreparedHandle(handle));
	std::lock_guard<std::mutex> guard(prepared_statements_mutex);
	auto entry = prepared_statements.find(handle_id);
	if (entry == prepared_statements.end()) {
		return Status::Invalid("Prepared statement not found");
	}
	return entry->second;
}

Result<std::shared_ptr<DuckDBFlightSqlServer::TransactionState>>
DuckDBFlightSqlServer::LookupTransaction(const std::string &handle) {
	ARROW_ASSIGN_OR_RAISE(auto handle_id, DecodeTransactionHandle(handle));
	return LookupTransaction(handle_id);
}

Result<std::shared_ptr<DuckDBFlightSqlServer::TransactionState>>
DuckDBFlightSqlServer::LookupTransaction(uint64_t transaction_id) {
	std::lock_guard<std::mutex> guard(transactions_mutex);
	auto entry = transactions.find(transaction_id);
	if (entry == transactions.end()) {
		return Status::Invalid("Transaction not found");
	}
	return entry->second;
}

Result<uint64_t> DuckDBFlightSqlServer::DecodePreparedHandle(const std::string &encoded_handle) const {
	return DecodeHandleLE(encoded_handle, "Invalid prepared statement handle encoding");
}

Result<uint64_t> DuckDBFlightSqlServer::DecodeTransactionHandle(const std::string &encoded_handle) const {
	return DecodeHandleLE(encoded_handle, "Invalid transaction handle encoding");
}

std::string DuckDBFlightSqlServer::GeneratePreparedHandle() {
	auto next = prepared_statement_counter.fetch_add(1, std::memory_order_relaxed) + 1;
	return EncodeHandleLE(next);
}

std::string DuckDBFlightSqlServer::GenerateTransactionHandle() {
	auto next = transaction_counter.fetch_add(1, std::memory_order_relaxed) + 1;
	return EncodeHandleLE(next);
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

Result<std::unique_ptr<FlightDataStream>> DuckDBFlightSqlServer::StreamSQLInTransaction(
    std::shared_ptr<TransactionState> transaction_state, const std::string &sql, idx_t batch_size) {
	std::unique_lock<std::shared_mutex> transaction_lock(transaction_state->mutex);
	ARROW_ASSIGN_OR_RAISE(auto result, QueryInTransaction(transaction_state, sql, true));
	ARROW_ASSIGN_OR_RAISE(auto stream, ResultToFlightStream(std::move(result), batch_size));
	return std::make_unique<LockedFlightDataStream>(std::move(stream), std::move(transaction_lock),
	                                                std::shared_ptr<void>(transaction_state));
}

Result<unique_ptr<QueryResult>>
DuckDBFlightSqlServer::QueryInTransaction(const std::shared_ptr<TransactionState> &transaction_state, const std::string &sql,
                                          bool streaming) {
	if (!transaction_state || !transaction_state->connection) {
		return Status::Invalid("Transaction not found");
	}
	unique_ptr<QueryResult> result;
	if (streaming) {
		result = transaction_state->connection->SendQuery(sql);
	} else {
		result = transaction_state->connection->Query(sql);
	}
	if (!result || result->HasError()) {
		return Status::Invalid(result ? result->GetError() : "Unknown DuckDB query failure");
	}
	transaction_state->UpdateActivityTime();
	return result;
}

Result<std::unique_ptr<FlightInfo>> DuckDBFlightSqlServer::GetFlightInfoStatement(const ServerCallContext & /*context*/,
                                                                                   const StatementQuery &command,
                                                                                   const FlightDescriptor &descriptor) {
	std::optional<uint64_t> transaction_id;
	unique_ptr<QueryResult> result;
	if (command.transaction_id.empty()) {
		Connection conn(*db);
		result = conn.SendQuery(command.query);
	} else {
		ARROW_ASSIGN_OR_RAISE(transaction_id, DecodeTransactionHandle(command.transaction_id));
		ARROW_ASSIGN_OR_RAISE(auto transaction_state, LookupTransaction(transaction_id.value()));
		std::unique_lock<std::shared_mutex> transaction_lock(transaction_state->mutex);
		ARROW_ASSIGN_OR_RAISE(result, QueryInTransaction(transaction_state, command.query, true));
	}
	if (!result || result->HasError()) {
		return Status::Invalid(result ? result->GetError() : "Unknown DuckDB query failure");
	}

	ARROW_ASSIGN_OR_RAISE(auto schema_result,
	                      DuckDBSchemaToArrow(result->types, result->names, result->client_properties));

	const auto statement_handle = EncodeStatementHandle(command.query, transaction_id);
	ARROW_ASSIGN_OR_RAISE(auto ticket_str, CreateStatementQueryTicket(statement_handle));
	std::vector<FlightEndpoint> endpoints {FlightEndpoint {Ticket {std::move(ticket_str)}, {}, std::nullopt, ""}};
	ARROW_ASSIGN_OR_RAISE(auto info, FlightInfo::Make(*schema_result, descriptor, endpoints, -1, -1, false));
	return std::make_unique<FlightInfo>(std::move(info));
}

Result<std::unique_ptr<FlightDataStream>> DuckDBFlightSqlServer::DoGetStatement(const ServerCallContext & /*context*/,
                                                                                const StatementQueryTicket &command) {
	ARROW_ASSIGN_OR_RAISE(auto decoded, DecodeStatementHandle(command.statement_handle));
	if (decoded.transaction_id.has_value()) {
		ARROW_ASSIGN_OR_RAISE(auto transaction_state, LookupTransaction(decoded.transaction_id.value()));
		return StreamSQLInTransaction(transaction_state, decoded.query);
	}
	return StreamSQL(decoded.query);
}

Result<std::unique_ptr<FlightInfo>> DuckDBFlightSqlServer::GetFlightInfoPreparedStatement(
    const ServerCallContext & /*context*/, const PreparedStatementQuery &command, const FlightDescriptor &descriptor) {
	ARROW_ASSIGN_OR_RAISE(auto state, LookupPreparedStatement(command.prepared_statement_handle));
	std::shared_ptr<Schema> dataset_schema;
	{
		std::shared_lock<std::shared_mutex> guard(state->mutex);
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
		std::shared_lock<std::shared_mutex> guard(state->mutex);
		dataset_schema = state->dataset_schema;
	}
	return SchemaResult::Make(*dataset_schema);
}

Result<std::unique_ptr<FlightDataStream>> DuckDBFlightSqlServer::DoGetPreparedStatement(
    const ServerCallContext & /*context*/, const PreparedStatementQuery &command) {
	ARROW_ASSIGN_OR_RAISE(auto state, LookupPreparedStatement(command.prepared_statement_handle));

	std::optional<uint64_t> owner_transaction;
	{
		std::shared_lock<std::shared_mutex> guard(state->mutex);
		owner_transaction = state->transaction_owner;
	}

	if (owner_transaction.has_value()) {
		auto transaction_lookup = LookupTransaction(owner_transaction.value());
		if (!transaction_lookup.ok()) {
			return Status::Invalid("Prepared statement not found");
		}
		auto transaction_state = transaction_lookup.ValueOrDie();
		std::unique_lock<std::shared_mutex> transaction_lock(transaction_state->mutex);

		case_insensitive_map_t<BoundParameterData> named_values;
		{
			std::shared_lock<std::shared_mutex> guard(state->mutex);
			if (!state->prepared) {
				return Status::Invalid("Prepared statement state is not initialized");
			}
			if (!state->ordered_parameters.empty() && !state->query_bound_parameters.has_value()) {
				return Status::Invalid("No parameter binding found for prepared statement query");
			}
			if (state->query_bound_parameters.has_value()) {
				named_values = state->query_bound_parameters.value();
			}
		}

		auto result = state->prepared->Execute(named_values, true);
		if (!result || result->HasError()) {
			return Status::Invalid(result ? result->GetError() : "Unknown DuckDB query failure");
		}
		transaction_state->UpdateActivityTime();
		ARROW_ASSIGN_OR_RAISE(auto stream, ResultToFlightStream(std::move(result)));
		return std::make_unique<LockedFlightDataStream>(std::move(stream), std::move(transaction_lock),
		                                                std::shared_ptr<void>(transaction_state));
	}

	std::unique_lock<std::shared_mutex> state_lock(state->mutex);
	if (!state->prepared || !state->connection) {
		return Status::Invalid("Prepared statement state is not initialized");
	}
	if (!state->ordered_parameters.empty() && !state->query_bound_parameters.has_value()) {
		return Status::Invalid("No parameter binding found for prepared statement query");
	}

	case_insensitive_map_t<BoundParameterData> named_values;
	if (state->query_bound_parameters.has_value()) {
		named_values = state->query_bound_parameters.value();
	}
	auto result = state->prepared->Execute(named_values, true);
	if (!result || result->HasError()) {
		return Status::Invalid(result ? result->GetError() : "Unknown DuckDB query failure");
	}

	ARROW_ASSIGN_OR_RAISE(auto stream, ResultToFlightStream(std::move(result)));
	return std::make_unique<LockedFlightDataStream>(std::move(stream), std::move(state_lock), std::shared_ptr<void>(state));
}

Status DuckDBFlightSqlServer::DoPutPreparedStatementQuery(const ServerCallContext & /*context*/,
                                                          const PreparedStatementQuery &command,
                                                          FlightMessageReader *reader,
                                                          FlightMetadataWriter *writer) {
	ARROW_ASSIGN_OR_RAISE(auto state, LookupPreparedStatement(command.prepared_statement_handle));

	std::vector<PreparedParameterDefinition> ordered_parameters;
	{
		std::shared_lock<std::shared_mutex> guard(state->mutex);
		ordered_parameters = state->ordered_parameters;
	}

	ARROW_ASSIGN_OR_RAISE(auto bound_rows, ReadBoundParameterRows(reader, ordered_parameters));
	if (bound_rows.size() > 1) {
		return Status::Invalid("Prepared statement query expects at most one bound parameter row");
	}

	{
		std::unique_lock<std::shared_mutex> guard(state->mutex);
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

	std::vector<PreparedParameterDefinition> ordered_parameters;
	std::optional<uint64_t> owner_transaction;
	{
		std::shared_lock<std::shared_mutex> guard(state->mutex);
		ordered_parameters = state->ordered_parameters;
		owner_transaction = state->transaction_owner;
	}

	ARROW_ASSIGN_OR_RAISE(auto bound_rows, ReadBoundParameterRows(reader, ordered_parameters));

	std::shared_ptr<TransactionState> transaction_state;
	std::unique_lock<std::shared_mutex> transaction_lock;
	std::unique_lock<std::shared_mutex> state_lock;
	if (owner_transaction.has_value()) {
		auto transaction_lookup = LookupTransaction(owner_transaction.value());
		if (!transaction_lookup.ok()) {
			return Status::Invalid("Prepared statement not found");
		}
		transaction_state = transaction_lookup.ValueOrDie();
		transaction_lock = std::unique_lock<std::shared_mutex>(transaction_state->mutex);
	} else {
		state_lock = std::unique_lock<std::shared_mutex>(state->mutex);
	}

	if (!state->prepared || (!owner_transaction.has_value() && !state->connection)) {
		return Status::Invalid("Prepared statement state is not initialized");
	}

	int64_t rows_changed = 0;
	if (ordered_parameters.empty()) {
		case_insensitive_map_t<BoundParameterData> empty_parameters;
		auto result = state->prepared->Execute(empty_parameters, false);
		if (!result || result->HasError()) {
			return Status::Invalid(result ? result->GetError() : "Unknown DuckDB query failure");
		}
		ARROW_ASSIGN_OR_RAISE(rows_changed, ExtractChangedRows(*result));
	} else if (bound_rows.empty()) {
		return Status::Invalid("Prepared statement update expected at least one bound parameter row");
	} else {
		for (auto &named_values : bound_rows) {
			auto result = state->prepared->Execute(named_values, false);
			if (!result || result->HasError()) {
				return Status::Invalid(result ? result->GetError() : "Unknown DuckDB query failure");
			}
			ARROW_ASSIGN_OR_RAISE(auto changed_rows, ExtractChangedRows(*result));
			rows_changed += changed_rows;
		}
	}

	if (transaction_state) {
		transaction_state->UpdateActivityTime();
	}
	return rows_changed;
}

Result<int64_t> DuckDBFlightSqlServer::DoPutCommandStatementUpdate(const ServerCallContext & /*context*/,
                                                                  const StatementUpdate &command) {
	if (command.transaction_id.empty()) {
		Connection conn(*db);
		auto result = conn.Query(command.query);
		if (!result || result->HasError()) {
			return Status::Invalid(result ? result->GetError() : "Unknown DuckDB query failure");
		}
		ARROW_ASSIGN_OR_RAISE(auto rows_changed, ExtractChangedRows(*result));
		return rows_changed;
	}

	ARROW_ASSIGN_OR_RAISE(auto transaction_state, LookupTransaction(command.transaction_id));
	std::unique_lock<std::shared_mutex> transaction_lock(transaction_state->mutex);
	ARROW_ASSIGN_OR_RAISE(auto result, QueryInTransaction(transaction_state, command.query, false));
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
		sql += StringUtil::Format(" AND schema_name LIKE '%s'",
		                         StringUtil::Replace(command.db_schema_filter_pattern.value(), "'", "''"));
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

void DuckDBFlightSqlServer::UpdateTransactionSqlInfo() {
	RegisterSqlInfo(SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_TRANSACTION,
	                SqlInfoResult(int64_t(SqlInfoOptions::SqlSupportedTransaction::SQL_SUPPORTED_TRANSACTION_TRANSACTION)));

	auto timeout_seconds = transaction_timeout_seconds.load(std::memory_order_relaxed);
	int64_t timeout_millis = 0;
	if (timeout_seconds > 0) {
		const auto max_seconds = std::numeric_limits<int64_t>::max() / 1000;
		timeout_millis = timeout_seconds > max_seconds ? std::numeric_limits<int64_t>::max() : timeout_seconds * 1000;
	}
	RegisterSqlInfo(SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_TRANSACTION_TIMEOUT, SqlInfoResult(timeout_millis));
}

Status DuckDBFlightSqlServer::FinalizeTransaction(const std::shared_ptr<TransactionState> &transaction_state, bool commit) {
	std::unique_lock<std::shared_mutex> lock(transaction_state->mutex);
	if (!transaction_state->connection) {
		return Status::Invalid("Transaction not found");
	}
	try {
		if (commit) {
			transaction_state->connection->Commit();
		} else {
			transaction_state->connection->Rollback();
		}
	} catch (std::exception &ex) {
		RemovePreparedStatements(transaction_state->owned_prepared_handles);
		transaction_state->owned_prepared_handles.clear();
		return Status::Invalid(ex.what());
	}

	RemovePreparedStatements(transaction_state->owned_prepared_handles);
	transaction_state->owned_prepared_handles.clear();
	return Status::OK();
}

Status DuckDBFlightSqlServer::RemovePreparedStatement(uint64_t handle_id, bool error_if_missing) {
	std::lock_guard<std::mutex> guard(prepared_statements_mutex);
	auto erased_count = prepared_statements.erase(handle_id);
	if (erased_count == 0 && error_if_missing) {
		return Status::Invalid("Prepared statement not found");
	}
	return Status::OK();
}

void DuckDBFlightSqlServer::RemovePreparedStatements(const std::unordered_set<uint64_t> &handle_ids) {
	std::lock_guard<std::mutex> guard(prepared_statements_mutex);
	for (auto handle_id : handle_ids) {
		prepared_statements.erase(handle_id);
	}
}

void DuckDBFlightSqlServer::StartTransactionSweeper() {
	sweeper_stopping.store(false, std::memory_order_relaxed);
	sweeper_thread = std::thread([this]() { RunTransactionSweeper(); });
}

void DuckDBFlightSqlServer::StopTransactionSweeper() {
	sweeper_stopping.store(true, std::memory_order_relaxed);
	sweeper_cv.notify_all();
	if (sweeper_thread.joinable()) {
		sweeper_thread.join();
	}
}

void DuckDBFlightSqlServer::RunTransactionSweeper() {
	while (!sweeper_stopping.load(std::memory_order_relaxed)) {
		{
			std::unique_lock<std::mutex> cv_lock(sweeper_cv_mutex);
			sweeper_cv.wait_for(cv_lock, std::chrono::seconds(1), [this]() {
				return sweeper_stopping.load(std::memory_order_relaxed);
			});
		}
		if (sweeper_stopping.load(std::memory_order_relaxed)) {
			break;
		}

		auto timeout_seconds = transaction_timeout_seconds.load(std::memory_order_relaxed);
		if (timeout_seconds <= 0) {
			continue;
		}

		std::vector<std::pair<uint64_t, std::shared_ptr<TransactionState>>> snapshot;
		{
			std::lock_guard<std::mutex> guard(transactions_mutex);
			snapshot.reserve(transactions.size());
			for (auto &entry : transactions) {
				snapshot.push_back(entry);
			}
		}

		const auto now_ms = CurrentTimeMillis();
		const auto timeout_ms = NumericCast<uint64_t>(timeout_seconds) * 1000;
		for (auto &entry : snapshot) {
			auto transaction_id = entry.first;
			auto &transaction_state = entry.second;
			auto last_activity_ms = transaction_state->last_activity_ms.load(std::memory_order_relaxed);
			if (now_ms <= last_activity_ms || now_ms - last_activity_ms < timeout_ms) {
				continue;
			}

			std::unique_lock<std::shared_mutex> transaction_lock(transaction_state->mutex, std::try_to_lock);
			if (!transaction_lock.owns_lock()) {
				continue;
			}

			last_activity_ms = transaction_state->last_activity_ms.load(std::memory_order_relaxed);
			const auto recheck_now_ms = CurrentTimeMillis();
			if (recheck_now_ms <= last_activity_ms || recheck_now_ms - last_activity_ms < timeout_ms) {
				continue;
			}

			bool removed = false;
			{
				std::lock_guard<std::mutex> guard(transactions_mutex);
				auto current_entry = transactions.find(transaction_id);
				if (current_entry != transactions.end() && current_entry->second == transaction_state) {
					transactions.erase(current_entry);
					removed = true;
				}
			}
			if (!removed) {
				continue;
			}

			try {
				if (transaction_state->connection) {
					transaction_state->connection->Rollback();
				}
			} catch (...) {
			}
			RemovePreparedStatements(transaction_state->owned_prepared_handles);
			transaction_state->owned_prepared_handles.clear();
			transaction_lock.unlock();
		}
	}
}

void DuckDBFlightSqlServer::RollbackAllTransactions() {
	std::vector<std::shared_ptr<TransactionState>> states;
	{
		std::lock_guard<std::mutex> guard(transactions_mutex);
		states.reserve(transactions.size());
		for (auto &entry : transactions) {
			states.push_back(entry.second);
		}
		transactions.clear();
	}
	for (auto &state : states) {
		(void)FinalizeTransaction(state, false);
	}
}

} // namespace flight
} // namespace duckdb
