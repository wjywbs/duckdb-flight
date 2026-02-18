#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "duckdb.hpp"

#include "arrow/flight/sql/server.h"

namespace duckdb {
namespace flight {

class DuckDBFlightSqlServer : public arrow::flight::sql::FlightSqlServerBase {
public:
	explicit DuckDBFlightSqlServer(shared_ptr<DatabaseInstance> db_instance);

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

	shared_ptr<DatabaseInstance> db;
	std::mutex prepared_statements_mutex;
	std::unordered_map<std::string, std::shared_ptr<PreparedStatementState>> prepared_statements;
	std::atomic<uint64_t> prepared_statement_counter {0};

	arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> GetFlightInfoForSchema(
	    const arrow::flight::FlightDescriptor &descriptor, const std::shared_ptr<arrow::Schema> &schema,
	    bool ordered = false);
	arrow::Result<std::shared_ptr<PreparedStatementState>> LookupPreparedStatement(const std::string &handle);
	std::string GeneratePreparedHandle();

	arrow::Result<std::shared_ptr<arrow::Schema>> DuckDBSchemaToArrow(const vector<LogicalType> &types,
	                                                                   const vector<std::string> &names,
	                                                                   ClientProperties client_properties);

	arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> ResultToFlightStream(
	    unique_ptr<QueryResult> result, idx_t batch_size = 2048);
	arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> StreamSQL(const std::string &sql,
	                                                                         idx_t batch_size = 2048);
};

} // namespace flight
} // namespace duckdb
