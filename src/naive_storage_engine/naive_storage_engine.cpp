#include "naive_storage_engine/naive_storage_engine.hpp"

#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <utility>

namespace naive_storage_engine {
namespace {

std::tm toLocalTime(std::time_t t) {
    std::tm tm{};
#if defined(_WIN32)
    localtime_s(&tm, &t);
#else
    localtime_r(&t, &tm);
#endif
    return tm;
}

}  // namespace

NaiveStorageEngine::NaiveStorageEngine(Limits limits) : limits_(limits) {}

bool NaiveStorageEngine::set(std::uint32_t key, std::int64_t value, const std::string& metadata) {
    auto it = rowsByKey_.find(key);
    if (it == rowsByKey_.end()) {
        if (rowsByKey_.size() >= limits_.maxUniqueKeys) {
            return false;
        }

        it = rowsByKey_.emplace(key, std::vector<Row>{}).first;
    }

    std::vector<Row>& rows = it->second;
    if (rows.size() >= limits_.maxValuesPerKey) {
        return false;
    }

    rows.push_back(Row{key, value, currentTimestamp(), metadata});
    return true;
}

std::vector<Row> NaiveStorageEngine::get(std::uint32_t key) const {
    const auto it = rowsByKey_.find(key);
    if (it == rowsByKey_.end()) {
        return {};
    }

    return it->second;
}

bool NaiveStorageEngine::clear() {
    rowsByKey_.clear();
    return true;
}

Limits NaiveStorageEngine::limits() const {
    return limits_;
}

std::string NaiveStorageEngine::currentTimestamp() {
    const auto now = std::chrono::system_clock::now();
    const auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;

    const std::time_t t = std::chrono::system_clock::to_time_t(now);
    const std::tm tm = toLocalTime(t);

    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S") << '.' << std::setfill('0') << std::setw(3)
        << millis.count();
    return oss.str();
}

}  // namespace naive_storage_engine
