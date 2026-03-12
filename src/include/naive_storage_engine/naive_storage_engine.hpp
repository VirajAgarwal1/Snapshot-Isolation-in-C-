#ifndef NAIVE_STORAGE_ENGINE_NAIVE_STORAGE_ENGINE_HPP
#define NAIVE_STORAGE_ENGINE_NAIVE_STORAGE_ENGINE_HPP

#include <atomic>
#include <cstdint>
#include <cstddef>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <vector>

namespace naive_storage_engine {

struct Row {
    std::uint32_t key = 0;
    std::int64_t value = 0;
    std::uint64_t timestamp = 0;
    std::string metadata;
};

struct Limits {
    std::uint32_t maxUniqueKeys = 10;
    std::uint32_t maxValuesPerKey = 100;
};

struct KeyEntry {
    mutable std::shared_mutex lock;
    std::vector<Row> versions;
};

class NaiveStorageEngine {
public:
    using SharedKeyLock = std::shared_lock<std::shared_mutex>;
    using ExclusiveKeyLock = std::unique_lock<std::shared_mutex>;

    explicit NaiveStorageEngine(Limits limits = {});

    std::optional<SharedKeyLock> acquireSharedLock(std::uint32_t key) const;
    std::optional<ExclusiveKeyLock> acquireExclusiveLock(std::uint32_t key);

    bool set(std::uint32_t key, std::int64_t value, const std::string& metadata);
    std::vector<Row> get(std::uint32_t key) const;
    bool clear();
    Limits limits() const;

private:
    Limits limits_;
    std::vector<KeyEntry> keys_;
    std::atomic<std::uint64_t> timestampCounter_{0};
};

}  // namespace naive_storage_engine

#endif