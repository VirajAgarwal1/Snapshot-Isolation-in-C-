#include "naive_storage_engine/naive_storage_engine.hpp"

#include <iostream>
#include <string>

namespace {

bool runStorageEngineBehaviorTest() {
    naive_storage_engine::Limits limits;
    limits.maxUniqueKeys = 2;
    limits.maxValuesPerKey = 3;

    naive_storage_engine::NaiveStorageEngine engine(limits);

    const naive_storage_engine::Limits readLimits = engine.limits();
    if (readLimits.maxUniqueKeys != 2 || readLimits.maxValuesPerKey != 3) {
        std::cerr << "limits() did not return expected values\n";
        return false;
    }

    if (!engine.clear()) {
        std::cerr << "clear() should always succeed\n";
        return false;
    }

    if (!engine.set(42U, 1000, "phase=seed")) {
        std::cerr << "first set() failed\n";
        return false;
    }

    if (!engine.set(7U, -9, "other-key")) {
        std::cerr << "second set() failed\n";
        return false;
    }

    const std::string metadata = "status=active\\twith-tab\\nand-line";
    if (!engine.set(42U, 2000, metadata)) {
        std::cerr << "third set() failed\n";
        return false;
    }

    if (!engine.set(42U, 3000, "v3")) {
        std::cerr << "fourth set() on same key should still fit per-key limit\n";
        return false;
    }

    if (engine.set(42U, 4000, "v4-over-limit")) {
        std::cerr << "set() should fail when max values per key is exceeded\n";
        return false;
    }

    if (engine.set(9U, 1, "third-unique-key-over-limit")) {
        std::cerr << "set() should fail when max unique key count is exceeded\n";
        return false;
    }

    const auto rows = engine.get(42U);
    if (rows.size() != 3) {
        std::cerr << "get(42) should return all rows for that key\n";
        return false;
    }

    if (rows[0].value != 1000 || rows[0].metadata != "phase=seed") {
        std::cerr << "first row did not match expected data\n";
        return false;
    }

    if (rows[1].value != 2000 || rows[1].metadata != metadata) {
        std::cerr << "second row did not preserve value/metadata\n";
        return false;
    }

    if (rows[2].value != 3000 || rows[2].metadata != "v3") {
        std::cerr << "third row did not match expected data\n";
        return false;
    }

    if (rows[0].timestamp.empty() || rows[1].timestamp.empty() || rows[2].timestamp.empty()) {
        std::cerr << "timestamps should be populated\n";
        return false;
    }

    if (!engine.clear()) {
        std::cerr << "clear() failed\n";
        return false;
    }

    if (!engine.get(42U).empty()) {
        std::cerr << "data should be empty after clear()\n";
        return false;
    }

    if (!engine.set(9U, 500, "post-clear")) {
        std::cerr << "set() should work after clear()\n";
        return false;
    }

    return true;
}

}  // namespace

int main() {
    const bool ok = runStorageEngineBehaviorTest();
    return ok ? 0 : 1;
}
