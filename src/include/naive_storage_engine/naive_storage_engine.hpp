#ifndef NAIVE_STORAGE_ENGINE_NAIVE_STORAGE_ENGINE_HPP
#define NAIVE_STORAGE_ENGINE_NAIVE_STORAGE_ENGINE_HPP

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

namespace naive_storage_engine {

struct Row {
    std::uint32_t key = 0;
    std::int64_t value = 0;
    std::string timestamp;
    std::string metadata;
};

struct Limits {
    std::size_t maxUniqueKeys = 10;
    std::size_t maxValuesPerKey = 100;
};

class NaiveStorageEngine {
public:
    explicit NaiveStorageEngine(Limits limits = {});

    bool set(std::uint32_t key, std::int64_t value, const std::string& metadata);
    std::vector<Row> get(std::uint32_t key) const;
    bool clear();
    Limits limits() const;

private:
    static std::string currentTimestamp();

    Limits limits_;
    std::unordered_map<std::uint32_t, std::vector<Row>> rowsByKey_;
};

}  // namespace naive_storage_engine

#endif