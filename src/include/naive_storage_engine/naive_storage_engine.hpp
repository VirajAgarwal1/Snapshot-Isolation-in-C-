#ifndef NAIVE_STORAGE_ENGINE_NAIVE_STORAGE_ENGINE_HPP
#define NAIVE_STORAGE_ENGINE_NAIVE_STORAGE_ENGINE_HPP

#include <atomic>
#include <cstdint>
#include <cstddef>
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
    explicit NaiveStorageEngine(Limits limits = {});

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