#define DUCKDB_EXTENSION_MAIN

#include "flight_extension.hpp"

#include "duckdb/main/extension/extension_loader.hpp"

#include "flight_service.hpp"

namespace duckdb {

namespace {

struct RunOnceState : public GlobalTableFunctionState {
	std::atomic<bool> run {false};
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &, TableFunctionInitInput &) {
		return make_uniq<RunOnceState>();
	}
};

static bool ShouldRun(TableFunctionInput &input) {
	auto &state = input.global_state->Cast<RunOnceState>();
	bool expected = false;
	return state.run.compare_exchange_strong(expected, true);
}

static unique_ptr<FunctionData> StringResultBind(ClientContext &, TableFunctionBindInput &, vector<LogicalType> &types,
                                                 vector<string> &names) {
	types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("status");
	return nullptr;
}

static unique_ptr<FunctionData> BoolResultBind(ClientContext &, TableFunctionBindInput &, vector<LogicalType> &types,
                                               vector<string> &names) {
	types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("started");
	return nullptr;
}

static unique_ptr<FunctionData> BigIntResultBind(ClientContext &, TableFunctionBindInput &, vector<LogicalType> &types,
                                                 vector<string> &names) {
	types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("timeout_seconds");
	return nullptr;
}

struct StartBindData : public FunctionData {
	explicit StartBindData(uint16_t port_p) : port(port_p) {
	}
	uint16_t port;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<StartBindData>(port);
	}
	bool Equals(const FunctionData &other_p) const override {
		return port == other_p.Cast<StartBindData>().port;
	}
};

struct TimeoutBindData : public FunctionData {
	explicit TimeoutBindData(int64_t timeout_seconds_p) : timeout_seconds(timeout_seconds_p) {
	}
	int64_t timeout_seconds;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<TimeoutBindData>(timeout_seconds);
	}
	bool Equals(const FunctionData &other_p) const override {
		return timeout_seconds == other_p.Cast<TimeoutBindData>().timeout_seconds;
	}
};

static unique_ptr<FunctionData> StartNoArgBind(ClientContext &, TableFunctionBindInput &, vector<LogicalType> &types,
                                               vector<string> &names) {
	types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("status");
	return make_uniq<StartBindData>(12345);
}

static unique_ptr<FunctionData> StartWithPortBind(ClientContext &, TableFunctionBindInput &input,
                                                  vector<LogicalType> &types, vector<string> &names) {
	types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("status");
	if (input.inputs.empty()) {
		throw InvalidInputException("start_flight_sql_server(port) requires a port argument");
	}
	if (input.inputs[0].IsNull()) {
		throw InvalidInputException("Flight SQL port cannot be NULL");
	}
	auto port = input.inputs[0].GetValue<uint16_t>();
	return make_uniq<StartBindData>(port);
}

static unique_ptr<FunctionData> SetTransactionTimeoutBind(ClientContext &, TableFunctionBindInput &input,
                                                          vector<LogicalType> &types, vector<string> &names) {
	types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("status");
	if (input.inputs.empty()) {
		throw InvalidInputException("set_flight_sql_transaction_timeout_seconds(seconds) requires an argument");
	}
	if (input.inputs[0].IsNull()) {
		throw InvalidInputException("Transaction timeout cannot be NULL");
	}
	auto timeout_seconds = input.inputs[0].GetValue<int64_t>();
	if (timeout_seconds < 0) {
		throw InvalidInputException("Transaction timeout must be >= 0 seconds");
	}
	return make_uniq<TimeoutBindData>(timeout_seconds);
}

static void StartFlightSQLServerFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	if (!ShouldRun(input)) {
		return;
	}
	auto port = input.bind_data ? input.bind_data->Cast<StartBindData>().port : static_cast<uint16_t>(12345);
	auto status = flight::FlightService::Get().Start(*context.db, port);
	output.SetCardinality(1);
	output.SetValue(0, 0, status);
}

static void StopFlightSQLServerFunction(ClientContext & /*context*/, TableFunctionInput &input, DataChunk &output) {
	if (!ShouldRun(input)) {
		return;
	}
	auto status = flight::FlightService::Get().Stop();
	output.SetCardinality(1);
	output.SetValue(0, 0, status);
}

static void FlightSQLIsStartedFunction(ClientContext & /*context*/, TableFunctionInput &input, DataChunk &output) {
	if (!ShouldRun(input)) {
		return;
	}
	output.SetCardinality(1);
	output.SetValue(0, 0, Value::BOOLEAN(flight::FlightService::Get().IsStarted()));
}

static void GetFlightSQLURLFunction(ClientContext & /*context*/, TableFunctionInput &input, DataChunk &output) {
	if (!ShouldRun(input)) {
		return;
	}
	auto url = flight::FlightService::Get().Location();
	output.SetCardinality(1);
	output.SetValue(0, 0, url);
}

static void SetTransactionTimeoutFunction(ClientContext & /*context*/, TableFunctionInput &input, DataChunk &output) {
	if (!ShouldRun(input)) {
		return;
	}
	auto timeout_seconds = input.bind_data->Cast<TimeoutBindData>().timeout_seconds;
	auto status = flight::FlightService::Get().SetTransactionTimeoutSeconds(timeout_seconds);
	output.SetCardinality(1);
	output.SetValue(0, 0, status);
}

static void GetTransactionTimeoutFunction(ClientContext & /*context*/, TableFunctionInput &input, DataChunk &output) {
	if (!ShouldRun(input)) {
		return;
	}
	auto timeout_seconds = flight::FlightService::Get().GetTransactionTimeoutSeconds();
	output.SetCardinality(1);
	output.SetValue(0, 0, Value::BIGINT(timeout_seconds));
}

static void RegisterFunctions(ExtensionLoader &loader) {
	auto start_no_arg =
	    TableFunction("start_flight_sql_server", {}, StartFlightSQLServerFunction, StartNoArgBind, RunOnceState::Init);
	loader.RegisterFunction(start_no_arg);

	auto start_with_port = TableFunction("start_flight_sql_server", {LogicalType::USMALLINT}, StartFlightSQLServerFunction,
	                                     StartWithPortBind, RunOnceState::Init);
	loader.RegisterFunction(start_with_port);

	auto stop =
	    TableFunction("stop_flight_sql_server", {}, StopFlightSQLServerFunction, StringResultBind, RunOnceState::Init);
	loader.RegisterFunction(stop);

	auto started =
	    TableFunction("flight_sql_is_started", {}, FlightSQLIsStartedFunction, BoolResultBind, RunOnceState::Init);
	loader.RegisterFunction(started);

	auto url = TableFunction("get_flight_sql_url", {}, GetFlightSQLURLFunction, StringResultBind, RunOnceState::Init);
	loader.RegisterFunction(url);

	auto set_timeout = TableFunction("set_flight_sql_transaction_timeout_seconds", {LogicalType::BIGINT},
	                                 SetTransactionTimeoutFunction, SetTransactionTimeoutBind, RunOnceState::Init);
	loader.RegisterFunction(set_timeout);

	auto get_timeout = TableFunction("get_flight_sql_transaction_timeout_seconds", {}, GetTransactionTimeoutFunction,
	                                 BigIntResultBind, RunOnceState::Init);
	loader.RegisterFunction(get_timeout);
}

} // namespace

void LoadInternal(ExtensionLoader &loader) {
	RegisterFunctions(loader);
}

void FlightExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string FlightExtension::Name() {
	return "flight";
}

std::string FlightExtension::Version() const {
#ifdef EXT_VERSION_FLIGHT
	return EXT_VERSION_FLIGHT;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(flight, loader) {
	duckdb::LoadInternal(loader);
}

}
