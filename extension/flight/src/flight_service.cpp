#include "flight_service.hpp"

#include <utility>

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

#include "arrow/flight/server.h"

#include "duckdb_flight_sql_server.hpp"

namespace duckdb {
namespace flight {

FlightService &FlightService::Get() {
	static FlightService instance;
	return instance;
}

FlightService::~FlightService() {
	try {
		Stop();
	} catch (...) {
	}
}

std::string FlightService::Start(DatabaseInstance &db_instance, uint16_t port) {
	std::lock_guard<std::mutex> guard(lock);
	if (started.load()) {
		return StringUtil::Format("Flight SQL server already started at %s", location);
	}

	auto db_ref = db_instance.shared_from_this();
	auto server_instance = std::make_shared<DuckDBFlightSqlServer>(std::move(db_ref));

	auto location_result = arrow::flight::Location::ForGrpcTcp("0.0.0.0", static_cast<int>(port));
	if (!location_result.ok()) {
		throw IOException("Failed to create Flight SQL location: %s", location_result.status().ToString());
	}
	auto flight_location = location_result.MoveValueUnsafe();

	arrow::flight::FlightServerOptions options(flight_location);
	auto status = server_instance->Init(options);
	if (!status.ok()) {
		throw IOException("Failed to initialize Flight SQL server: %s", status.ToString());
	}

	server = std::move(server_instance);
	location = server->location().ToString();
	started.store(true);

	server_thread = make_uniq<std::thread>([server = server] {
		auto serve_status = server->Serve();
		(void)serve_status;
	});

	return StringUtil::Format("Flight SQL server started at %s", location);
}

std::string FlightService::Stop() {
	std::shared_ptr<arrow::flight::sql::FlightSqlServerBase> to_stop;
	std::unique_ptr<std::thread> thread;
	std::string old_location;
	{
		std::lock_guard<std::mutex> guard(lock);
		if (!started.load()) {
			return "Flight SQL server already stopped";
		}
		started.store(false);
		to_stop = server;
		thread = std::move(server_thread);
		old_location = std::move(location);
		server.reset();
		location.clear();
	}

	if (to_stop) {
		auto shutdown_status = to_stop->Shutdown();
		if (!shutdown_status.ok()) {
			throw IOException("Failed to stop Flight SQL server: %s", shutdown_status.ToString());
		}
	}
	if (thread) {
		thread->join();
	}
	return StringUtil::Format("Flight SQL server stopped (%s)", old_location);
}

bool FlightService::IsStarted() const {
	return started.load();
}

std::string FlightService::Location() const {
	std::lock_guard<std::mutex> guard(lock);
	return location;
}

} // namespace flight
} // namespace duckdb
