#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "duckdb.hpp"

#include "arrow/flight/sql/server.h"

namespace duckdb {
namespace flight {

class DuckDBFlightSqlServer : public arrow::flight::sql::FlightSqlServerBase {
public:
	explicit DuckDBFlightSqlServer(shared_ptr<DatabaseInstance> db_instance);
	~DuckDBFlightSqlServer() override;

	void SetTransactionTimeoutSeconds(int64_t timeout_seconds);
	int64_t GetTransactionTimeoutSeconds() const;

	arrow::Result<arrow::flight::sql::ActionBeginTransactionResult> BeginTransaction(
	    const arrow::flight::ServerCallContext &context,
	    const arrow::flight::sql::ActionBeginTransactionRequest &request) override;
	arrow::Status EndTransaction(const arrow::flight::ServerCallContext &context,
	                             const arrow::flight::sql::ActionEndTransactionRequest &request) override;
	arrow::Result<arrow::flight::sql::ActionBeginSavepointResult> BeginSavepoint(
	    const arrow::flight::ServerCallContext &context,
	    const arrow::flight::sql::ActionBeginSavepointRequest &request) override;
	arrow::Status EndSavepoint(const arrow::flight::ServerCallContext &context,
	                           const arrow::flight::sql::ActionEndSavepointRequest &request) override;

	arrow::Result<arrow::flight::sql::ActionCreatePreparedStatementResult> CreatePreparedStatement(
	    const arrow::flight::ServerCallContext &context,
	    const arrow::flight::sql::ActionCreatePreparedStatementRequest &request) override;
	arrow::Status ClosePreparedStatement(const arrow::flight::ServerCallContext &context,
	                                     const arrow::flight::sql::ActionClosePreparedStatementRequest &request) override;

	arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> GetFlightInfoStatement(
	    const arrow::flight::ServerCallContext &context, const arrow::flight::sql::StatementQuery &command,
	    const arrow::flight::FlightDescriptor &descriptor) override;
	arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> DoGetStatement(
	    const arrow::flight::ServerCallContext &context, const arrow::flight::sql::StatementQueryTicket &command) override;

	arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> GetFlightInfoPreparedStatement(
	    const arrow::flight::ServerCallContext &context, const arrow::flight::sql::PreparedStatementQuery &command,
	    const arrow::flight::FlightDescriptor &descriptor) override;
	arrow::Result<std::unique_ptr<arrow::flight::SchemaResult>> GetSchemaPreparedStatement(
	    const arrow::flight::ServerCallContext &context, const arrow::flight::sql::PreparedStatementQuery &command,
	    const arrow::flight::FlightDescriptor &descriptor) override;
	arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> DoGetPreparedStatement(
	    const arrow::flight::ServerCallContext &context, const arrow::flight::sql::PreparedStatementQuery &command) override;

	arrow::Status DoPutPreparedStatementQuery(const arrow::flight::ServerCallContext &context,
	                                          const arrow::flight::sql::PreparedStatementQuery &command,
	                                          arrow::flight::FlightMessageReader *reader,
	                                          arrow::flight::FlightMetadataWriter *writer) override;
	arrow::Result<int64_t> DoPutPreparedStatementUpdate(const arrow::flight::ServerCallContext &context,
	                                                    const arrow::flight::sql::PreparedStatementUpdate &command,
	                                                    arrow::flight::FlightMessageReader *reader) override;

	arrow::Result<int64_t> DoPutCommandStatementUpdate(const arrow::flight::ServerCallContext &context,
	                                                   const arrow::flight::sql::StatementUpdate &command) override;

	arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> GetFlightInfoCatalogs(
	    const arrow::flight::ServerCallContext &context, const arrow::flight::FlightDescriptor &descriptor) override;
	arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> DoGetCatalogs(
	    const arrow::flight::ServerCallContext &context) override;

	arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> GetFlightInfoSchemas(
	    const arrow::flight::ServerCallContext &context, const arrow::flight::sql::GetDbSchemas &command,
	    const arrow::flight::FlightDescriptor &descriptor) override;
	arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> DoGetDbSchemas(
	    const arrow::flight::ServerCallContext &context, const arrow::flight::sql::GetDbSchemas &command) override;

	arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> GetFlightInfoTables(
	    const arrow::flight::ServerCallContext &context, const arrow::flight::sql::GetTables &command,
	    const arrow::flight::FlightDescriptor &descriptor) override;
	arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> DoGetTables(
	    const arrow::flight::ServerCallContext &context, const arrow::flight::sql::GetTables &command) override;

	arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> GetFlightInfoTableTypes(
	    const arrow::flight::ServerCallContext &context, const arrow::flight::FlightDescriptor &descriptor) override;
	arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> DoGetTableTypes(
	    const arrow::flight::ServerCallContext &context) override;

private:
	struct PreparedStatementState;
	struct TransactionState;

	shared_ptr<DatabaseInstance> db;
	std::mutex prepared_statements_mutex;
	std::unordered_map<uint64_t, std::shared_ptr<PreparedStatementState>> prepared_statements;
	std::atomic<uint64_t> prepared_statement_counter {0};
	std::mutex transactions_mutex;
	std::unordered_map<uint64_t, std::shared_ptr<TransactionState>> transactions;
	std::atomic<uint64_t> transaction_counter {0};
	std::atomic<int64_t> transaction_timeout_seconds {1800};
	std::atomic<bool> sweeper_stopping {false};
	std::condition_variable sweeper_cv;
	mutable std::mutex sweeper_cv_mutex;
	std::thread sweeper_thread;

	arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> GetFlightInfoForSchema(
	    const arrow::flight::FlightDescriptor &descriptor, const std::shared_ptr<arrow::Schema> &schema,
	    bool ordered = false);
	arrow::Result<std::shared_ptr<PreparedStatementState>> LookupPreparedStatement(const std::string &handle);
	arrow::Result<std::shared_ptr<TransactionState>> LookupTransaction(const std::string &handle);
	arrow::Result<std::shared_ptr<TransactionState>> LookupTransaction(uint64_t transaction_id);
	arrow::Result<uint64_t> DecodePreparedHandle(const std::string &encoded_handle) const;
	arrow::Result<uint64_t> DecodeTransactionHandle(const std::string &encoded_handle) const;
	std::string GeneratePreparedHandle();
	std::string GenerateTransactionHandle();

	arrow::Result<std::shared_ptr<arrow::Schema>> DuckDBSchemaToArrow(const vector<LogicalType> &types,
	                                                                   const vector<std::string> &names,
	                                                                   ClientProperties client_properties);

	arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> ResultToFlightStream(
	    unique_ptr<QueryResult> result, idx_t batch_size = 2048);
	arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> ResultToLockedFlightStream(
	    std::shared_ptr<TransactionState> transaction_state, unique_ptr<QueryResult> result,
	    std::unique_lock<std::shared_mutex> transaction_lock, idx_t batch_size = 2048);
	arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> StreamSQL(const std::string &sql,
	                                                                           idx_t batch_size = 2048);
	arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> StreamSQLInTransaction(
	    std::shared_ptr<TransactionState> transaction_state, const std::string &sql, idx_t batch_size = 2048);

	void UpdateTransactionSqlInfo();
	void TouchTransaction(const std::shared_ptr<TransactionState> &transaction_state);
	arrow::Result<unique_ptr<QueryResult>> QueryInTransaction(const std::shared_ptr<TransactionState> &transaction_state,
	                                                          const std::string &sql, bool streaming);
	arrow::Status FinalizeTransaction(const std::shared_ptr<TransactionState> &transaction_state, bool commit,
	                                  std::vector<uint64_t> &owned_prepared_handles);
	arrow::Status RemovePreparedStatement(uint64_t handle_id, bool error_if_missing);
	void RemovePreparedStatements(const std::vector<uint64_t> &handle_ids);
	void StartTransactionSweeper();
	void StopTransactionSweeper();
	void RunTransactionSweeper();
	void RollbackAllTransactions();
};

} // namespace flight
} // namespace duckdb
