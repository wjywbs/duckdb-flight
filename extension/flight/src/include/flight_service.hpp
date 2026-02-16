#pragma once

#include <atomic>
#include <mutex>
#include <thread>

#include "duckdb.hpp"

namespace arrow {
namespace flight {
namespace sql {
class FlightSqlServerBase;
} // namespace sql
} // namespace flight
} // namespace arrow

namespace duckdb {
namespace flight {

class FlightService {
public:
	static FlightService &Get();

	std::string Start(DatabaseInstance &db_instance, uint16_t port);
	std::string Stop();
	bool IsStarted() const;
	std::string Location() const;

private:
	FlightService() = default;
	~FlightService();

	FlightService(const FlightService &) = delete;
	FlightService &operator=(const FlightService &) = delete;

	mutable std::mutex lock;
	std::shared_ptr<arrow::flight::sql::FlightSqlServerBase> server;
	std::unique_ptr<std::thread> server_thread;
	std::string location;
	std::atomic<bool> started {false};
};

} // namespace flight
} // namespace duckdb
