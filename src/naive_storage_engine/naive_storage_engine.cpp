#include "naive_storage_engine/naive_storage_engine.hpp"

#include <mutex>
#include <shared_mutex>

namespace naive_storage_engine {

NaiveStorageEngine::NaiveStorageEngine(Limits limits) : limits_(limits), keys_(limits.maxUniqueKeys) {
    for (KeyEntry& entry : keys_) {
        // Keep each version vector stable by pre-reserving its max size.
        entry.versions.reserve(limits_.maxValuesPerKey);
    }
}

bool NaiveStorageEngine::set(std::uint32_t key, std::int64_t value, const std::string& metadata) {
    if (key >= limits_.maxUniqueKeys) {
        return false;
    }

    KeyEntry& entry = keys_[key];

    // Per-key writer lock: writes to different keys proceed independently.
    std::unique_lock<std::shared_mutex> lock(entry.lock);
    if (entry.versions.size() >= limits_.maxValuesPerKey) {
        return false;
    }

    const std::uint64_t ts = timestampCounter_.fetch_add(1) + 1;
    entry.versions.push_back(Row{key, value, ts, metadata});
    return true;
}

std::vector<Row> NaiveStorageEngine::get(std::uint32_t key) const {
    if (key >= limits_.maxUniqueKeys) {
        return {};
    }

    const KeyEntry& entry = keys_[key];

    // Per-key reader lock allows concurrent readers for the same key.
    std::shared_lock<std::shared_mutex> lock(entry.lock);
    return entry.versions;
}

bool NaiveStorageEngine::clear() {
    for (KeyEntry& entry : keys_) {
        std::unique_lock<std::shared_mutex> lock(entry.lock);
        entry.versions.clear();
    }

    return true;
}

Limits NaiveStorageEngine::limits() const {
    return limits_;
}

}  // namespace naive_storage_engine
