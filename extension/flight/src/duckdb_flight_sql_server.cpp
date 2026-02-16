#include "duckdb_flight_sql_server.hpp"

#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/result_arrow_wrapper.hpp"
#include "duckdb/common/string_util.hpp"

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
using arrow::flight::RecordBatchStream;
using arrow::flight::ServerCallContext;
using arrow::flight::Ticket;
using arrow::flight::sql::CreateStatementQueryTicket;
using arrow::flight::sql::GetDbSchemas;
using arrow::flight::sql::GetTables;
using arrow::flight::sql::SqlInfoOptions;
using arrow::flight::sql::SqlInfoResult;
using arrow::flight::sql::SqlSchema;
using arrow::flight::sql::StatementQuery;
using arrow::flight::sql::StatementQueryTicket;
using arrow::flight::sql::StatementUpdate;

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

Result<std::unique_ptr<FlightInfo>> DuckDBFlightSqlServer::GetFlightInfoForSchema(const FlightDescriptor &descriptor,
                                                                                   const std::shared_ptr<Schema> &schema,
                                                                                   bool ordered) {
	std::vector<FlightEndpoint> endpoints {FlightEndpoint {Ticket {descriptor.cmd}, {}, std::nullopt, ""}};
	ARROW_ASSIGN_OR_RAISE(auto info, FlightInfo::Make(*schema, descriptor, endpoints, -1, -1, ordered));
	return std::make_unique<FlightInfo>(std::move(info));
}

Result<std::unique_ptr<FlightDataStream>> DuckDBFlightSqlServer::StreamSQL(const std::string &sql, idx_t batch_size) {
	Connection conn(*db);
	auto result = conn.Query(sql);
	if (!result || result->HasError()) {
		return Status::Invalid(result ? result->GetError() : "Unknown DuckDB query failure");
	}

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

Result<std::unique_ptr<FlightInfo>> DuckDBFlightSqlServer::GetFlightInfoStatement(const ServerCallContext & /*context*/,
                                                                                   const StatementQuery &command,
                                                                                   const FlightDescriptor &descriptor) {
	Connection conn(*db);
	auto result = conn.Query(command.query);
	if (!result || result->HasError()) {
		return Status::Invalid(result ? result->GetError() : "Unknown DuckDB query failure");
	}

	ArrowSchema schema;
	schema.Init();
	ArrowConverter::ToArrowSchema(&schema, result->types, result->names, result->client_properties);
	auto schema_result = arrow::ImportSchema(reinterpret_cast<struct ArrowSchema *>(&schema));
	if (schema.release) {
		schema.release(&schema);
	}
	if (!schema_result.ok()) {
		return schema_result.status();
	}

	ARROW_ASSIGN_OR_RAISE(auto ticket_str, CreateStatementQueryTicket(command.query));
	std::vector<FlightEndpoint> endpoints {FlightEndpoint {Ticket {std::move(ticket_str)}, {}, std::nullopt, ""}};
	ARROW_ASSIGN_OR_RAISE(auto info, FlightInfo::Make(*schema_result.ValueOrDie(), descriptor, endpoints, -1, -1, false));
	return std::make_unique<FlightInfo>(std::move(info));
}

Result<std::unique_ptr<FlightDataStream>> DuckDBFlightSqlServer::DoGetStatement(const ServerCallContext & /*context*/,
                                                                                const StatementQueryTicket &command) {
	return StreamSQL(command.statement_handle);
}

Result<int64_t> DuckDBFlightSqlServer::DoPutCommandStatementUpdate(const ServerCallContext & /*context*/,
                                                                  const StatementUpdate &command) {
	Connection conn(*db);
	auto result = conn.Query(command.query);
	if (!result || result->HasError()) {
		return Status::Invalid(result ? result->GetError() : "Unknown DuckDB query failure");
	}

	int64_t rows_changed = 0;
	if (result->properties.return_type == StatementReturnType::CHANGED_ROWS) {
		auto chunk = result->Fetch();
		if (chunk && chunk->size() == 1 && chunk->ColumnCount() == 1) {
			rows_changed = chunk->GetValue(0, 0).GetValue<int64_t>();
		}
	}
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
