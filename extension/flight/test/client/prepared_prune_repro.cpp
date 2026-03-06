#include "duckdb.hpp"

#include <iostream>
#include <stdexcept>
#include <string>

namespace {

using duckdb::Connection;
using duckdb::DuckDB;
using duckdb::idx_t;
using duckdb::PreparedStatement;
using duckdb::QueryResult;

constexpr idx_t ROWS_PER_WORKER = 250;
constexpr int64_t TARGET_WORKER = 14;
constexpr int64_t OTHER_WORKER = 15;

void ThrowIfError(QueryResult &result, const std::string &context) {
	if (result.HasError()) {
		throw std::runtime_error(context + ": " + result.GetError());
	}
}

void ThrowIfPrepareError(PreparedStatement &statement, const std::string &context) {
	if (statement.HasError()) {
		throw std::runtime_error(context + ": " + statement.GetError());
	}
}

int64_t FetchSingleBigInt(std::unique_ptr<QueryResult> result, const std::string &context) {
	if (!result) {
		throw std::runtime_error(context + ": null result");
	}
	ThrowIfError(*result, context);
	auto chunk = result->Fetch();
	if (!chunk) {
		throw std::runtime_error(context + ": no rows returned");
	}
	if (chunk->size() != 1 || chunk->ColumnCount() != 1) {
		throw std::runtime_error(context + ": expected exactly one row/one column");
	}
	return chunk->GetValue(0, 0).GetValue<int64_t>();
}

void ExecuteNoResult(Connection &conn, const std::string &sql, const std::string &context) {
	auto result = conn.Query(sql);
	if (!result) {
		throw std::runtime_error(context + ": null result");
	}
	ThrowIfError(*result, context);
}

std::string BuildInsertSQL(int64_t start_id, int64_t worker_id) {
	std::string sql = "INSERT INTO go_flight_tx_bench VALUES ";
	for (idx_t i = 0; i < ROWS_PER_WORKER; i++) {
		if (i > 0) {
			sql += ", ";
		}
		sql += "(" + std::to_string(start_id + static_cast<int64_t>(i)) + ", " + std::to_string(worker_id) + ", 1)";
	}
	return sql;
}

bool RunAttempt() {
	DuckDB db(nullptr);
	Connection setup(db);
	Connection prepare_conn(db);
	Connection writer_conn(db);
	Connection fresh_conn(db);

	ExecuteNoResult(setup, "CREATE TABLE go_flight_tx_bench(id BIGINT, worker_id BIGINT, v BIGINT)",
	                "create table");
	ExecuteNoResult(writer_conn, BuildInsertSQL(1, TARGET_WORKER), "insert target worker");

	const std::string target_sql =
	    "SELECT CAST(COUNT(*) AS BIGINT) FROM go_flight_tx_bench WHERE worker_id = " + std::to_string(TARGET_WORKER);

	auto prepared = prepare_conn.Prepare(target_sql);
	if (!prepared) {
		throw std::runtime_error("prepare returned null");
	}
	ThrowIfPrepareError(*prepared, "prepare target statement");

	ExecuteNoResult(writer_conn, BuildInsertSQL(1 + static_cast<int64_t>(ROWS_PER_WORKER), OTHER_WORKER),
	                "insert other worker");

	auto prepared_value = FetchSingleBigInt(prepared->Execute(), "execute prepared");
	auto fresh_value = FetchSingleBigInt(fresh_conn.Query(target_sql), "execute fresh");

	std::cout << "prepared=" << prepared_value << " fresh=" << fresh_value << " expected=" << ROWS_PER_WORKER
	          << std::endl;
	return prepared_value != fresh_value;
}

} // namespace

int main() {
	try {
		auto mismatch = RunAttempt();
		if (!mismatch) {
			std::cerr << "repro did not trigger" << std::endl;
			return 1;
		}
		return 0;
	} catch (const std::exception &ex) {
		std::cerr << ex.what() << std::endl;
		return 2;
	}
}
