#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <thread>

#include "duckdb.hpp"

namespace duckdb {
namespace flight {

class DuckDBFlightSqlServer;

class FlightService {
public:
	static FlightService &Get();

	std::string Start(DatabaseInstance &db_instance, uint16_t port);
	std::string Stop();
	std::string SetTransactionTimeoutSeconds(int64_t timeout_seconds);
	int64_t GetTransactionTimeoutSeconds() const;
	bool IsStarted() const;
	std::string Location() const;

private:
	FlightService() = default;
	~FlightService();

	FlightService(const FlightService &) = delete;
	FlightService &operator=(const FlightService &) = delete;

	mutable std::mutex lock;
	std::shared_ptr<DuckDBFlightSqlServer> server;
	std::unique_ptr<std::thread> server_thread;
	std::string location;
	std::atomic<bool> started {false};
	// Stores the desired timeout even when the server is stopped.
	// Start() applies this to new server instances, and GetTransactionTimeoutSeconds()
	// falls back to this value when no server is active.
	std::atomic<int64_t> transaction_timeout_seconds {1800};
};

} // namespace flight
} // namespace duckdb
