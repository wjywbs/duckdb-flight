#include <cerrno>
#include <cstdlib>
#include <iostream>
#include <optional>
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

namespace {

struct Options {
	std::string host = "127.0.0.1";
	int32_t port = -1;
	std::string mode;
};

void PrintUsage(const char *program_name) {
	std::cerr << "Usage: " << program_name << " --host <host> --port <port> --mode <ping|crud>\n";
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
	if (options.mode != "ping" && options.mode != "crud") {
		error = "--mode must be ping or crud";
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

arrow::Result<std::shared_ptr<arrow::Table>> ExecuteQuery(FlightSqlClient &client, const std::string &query) {
	ARROW_ASSIGN_OR_RAISE(auto info, client.Execute({}, query));
	if (info->endpoints().empty()) {
		return Status::Invalid("no endpoints returned for query: ", query);
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

Status RunMain(const Options &options) {
	ARROW_ASSIGN_OR_RAISE(auto location, Location::ForGrpcTcp(options.host, options.port));
	ARROW_ASSIGN_OR_RAISE(auto client, FlightClient::Connect(location));
	FlightSqlClient sql_client(std::move(client));

	Status status;
	if (options.mode == "ping") {
		status = RunPing(sql_client);
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
