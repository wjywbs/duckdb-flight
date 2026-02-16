#pragma once

#include <memory>
#include <string>

#include "duckdb.hpp"

#include "arrow/flight/sql/server.h"

namespace duckdb {
namespace flight {

class DuckDBFlightSqlServer : public arrow::flight::sql::FlightSqlServerBase {
public:
	explicit DuckDBFlightSqlServer(shared_ptr<DatabaseInstance> db_instance);

	arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> GetFlightInfoStatement(
	    const arrow::flight::ServerCallContext &context, const arrow::flight::sql::StatementQuery &command,
	    const arrow::flight::FlightDescriptor &descriptor) override;
	arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> DoGetStatement(
	    const arrow::flight::ServerCallContext &context, const arrow::flight::sql::StatementQueryTicket &command) override;
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
	shared_ptr<DatabaseInstance> db;

	arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> GetFlightInfoForSchema(
	    const arrow::flight::FlightDescriptor &descriptor, const std::shared_ptr<arrow::Schema> &schema,
	    bool ordered = false);
	arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> StreamSQL(const std::string &sql,
	                                                                         idx_t batch_size = 2048);
};

} // namespace flight
} // namespace duckdb
