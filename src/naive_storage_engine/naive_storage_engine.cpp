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

std::optional<NaiveStorageEngine::SharedKeyLock> NaiveStorageEngine::acquireSharedLock(
    std::uint32_t key) const {
    if (key >= limits_.maxUniqueKeys) {
        return std::nullopt;
    }

    return SharedKeyLock(keys_[key].lock);
}

std::optional<NaiveStorageEngine::ExclusiveKeyLock> NaiveStorageEngine::acquireExclusiveLock(
    std::uint32_t key) {
    if (key >= limits_.maxUniqueKeys) {
        return std::nullopt;
    }

    return ExclusiveKeyLock(keys_[key].lock);
}

bool NaiveStorageEngine::set(std::uint32_t key, std::int64_t value, const std::string& metadata) {
    if (key >= limits_.maxUniqueKeys) {
        return false;
    }

    KeyEntry& entry = keys_[key];
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
