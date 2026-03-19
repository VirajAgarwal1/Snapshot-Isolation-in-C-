#include "naive_storage_engine/naive_storage_engine.hpp"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <functional>
#include <iostream>
#include <limits>
#include <string>
#include <thread>
#include <vector>

namespace {

bool setWithExclusiveLock(naive_storage_engine::NaiveStorageEngine& engine, std::uint32_t key,
                          std::int64_t value, const std::string& metadata) {
    auto lock = engine.acquireExclusiveLock(key);
    if (!lock.has_value()) {
        return false;
    }

    return engine.set(key, value, metadata);
}

bool vacuumWithExclusiveLock(naive_storage_engine::NaiveStorageEngine& engine, std::uint32_t key,
                             std::uint64_t horizonTimestamp) {
    auto lock = engine.acquireExclusiveLock(key);
    if (!lock.has_value()) {
        return false;
    }

    return engine.vacuumByHorizon(key, horizonTimestamp);
}

bool updateWithExclusiveLock(
    naive_storage_engine::NaiveStorageEngine& engine, std::uint32_t key,
    const std::function<bool(const naive_storage_engine::Row&)>& shouldUpdate,
    const std::function<void(naive_storage_engine::Row&)>& applyUpdate) {
    auto lock = engine.acquireExclusiveLock(key);
    if (!lock.has_value()) {
        return false;
    }

    return engine.updateVersions(key, shouldUpdate, applyUpdate);
}

std::vector<naive_storage_engine::Row> getWithSharedLock(
    const naive_storage_engine::NaiveStorageEngine& engine, std::uint32_t key) {
    auto lock = engine.acquireSharedLock(key);
    if (!lock.has_value()) {
        return {};
    }

    return engine.get(key);
}

bool runBasicBehaviorTest() {
    naive_storage_engine::Limits limits;
    limits.maxUniqueKeys = 2;
    limits.maxValuesPerKey = 3;

    naive_storage_engine::NaiveStorageEngine engine(limits);

    const naive_storage_engine::Limits readLimits = engine.limits();
    if (readLimits.maxUniqueKeys != 2 || readLimits.maxValuesPerKey != 3) {
        std::cerr << "limits() did not return expected values\n";
        return false;
    }

    if (!setWithExclusiveLock(engine, 0U, 1000, "phase=seed")) {
        std::cerr << "first set() failed\n";
        return false;
    }

    if (!setWithExclusiveLock(engine, 1U, -9, "other-key")) {
        std::cerr << "second set() failed\n";
        return false;
    }

    const std::string metadata = "status=active with-spaces";
    if (!setWithExclusiveLock(engine, 0U, 2000, metadata)) {
        std::cerr << "third set() failed\n";
        return false;
    }

    if (!setWithExclusiveLock(engine, 0U, 3000, "v3")) {
        std::cerr << "fourth set() on same key should still fit per-key limit\n";
        return false;
    }

    if (setWithExclusiveLock(engine, 0U, 4000, "v4-over-limit")) {
        std::cerr << "set() should fail when max values per key is exceeded\n";
        return false;
    }

    if (engine.set(2U, 1, "invalid-key")) {
        std::cerr << "set() should fail for key >= maxUniqueKeys\n";
        return false;
    }

    if (engine.acquireSharedLock(2U).has_value()) {
        std::cerr << "acquireSharedLock() should fail for key >= maxUniqueKeys\n";
        return false;
    }

    if (engine.acquireExclusiveLock(2U).has_value()) {
        std::cerr << "acquireExclusiveLock() should fail for key >= maxUniqueKeys\n";
        return false;
    }

    const auto rows = getWithSharedLock(engine, 0U);
    if (rows.size() != 3) {
        std::cerr << "get(0) should return all rows for that key\n";
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

    if (rows[0].timestamp == 0 || rows[1].timestamp == 0 || rows[2].timestamp == 0) {
        std::cerr << "timestamps should be non-zero\n";
        return false;
    }

    if (!(rows[0].timestamp < rows[1].timestamp && rows[1].timestamp < rows[2].timestamp)) {
        std::cerr << "timestamps should preserve insertion order\n";
        return false;
    }

    if (!engine.clear()) {
        std::cerr << "clear() failed\n";
        return false;
    }

    if (!getWithSharedLock(engine, 0U).empty()) {
        std::cerr << "data should be empty after clear()\n";
        return false;
    }

    if (!setWithExclusiveLock(engine, 1U, 500, "post-clear")) {
        std::cerr << "set() should work after clear()\n";
        return false;
    }

    return true;
}

bool runUpdateVersionsBehaviorTest() {
    naive_storage_engine::Limits limits;
    limits.maxUniqueKeys = 2;
    limits.maxValuesPerKey = 10;

    naive_storage_engine::NaiveStorageEngine engine(limits);

    if (!setWithExclusiveLock(engine, 0U, 10, "v1")) {
        std::cerr << "seed row 1 failed in updateVersions test\n";
        return false;
    }

    if (!setWithExclusiveLock(engine, 0U, 20, "target")) {
        std::cerr << "seed row 2 failed in updateVersions test\n";
        return false;
    }

    if (!setWithExclusiveLock(engine, 0U, 30, "v3")) {
        std::cerr << "seed row 3 failed in updateVersions test\n";
        return false;
    }

    const bool updated = updateWithExclusiveLock(
        engine, 0U,
        [](const naive_storage_engine::Row& row) { return row.metadata == "target"; },
        [](naive_storage_engine::Row& row) {
            row.value = 2000;
            row.metadata = "target-updated";
        });

    if (!updated) {
        std::cerr << "updateVersions() should report true when at least one row is changed\n";
        return false;
    }

    const auto rowsAfterUpdate = getWithSharedLock(engine, 0U);
    if (rowsAfterUpdate.size() != 3U) {
        std::cerr << "updateVersions() should not change number of versions\n";
        return false;
    }

    if (rowsAfterUpdate[1].value != 2000 || rowsAfterUpdate[1].metadata != "target-updated") {
        std::cerr << "updateVersions() did not apply mutator to matching row\n";
        return false;
    }

    const bool updatedNothing = updateWithExclusiveLock(
        engine, 0U,
        [](const naive_storage_engine::Row& row) { return row.metadata == "missing"; },
        [](naive_storage_engine::Row& row) { row.value = -1; });

    if (updatedNothing) {
        std::cerr << "updateVersions() should return false when no rows match\n";
        return false;
    }

    {
        auto lock = engine.acquireExclusiveLock(0U);
        if (!lock.has_value()) {
            std::cerr << "exclusive lock acquisition failed in updateVersions test\n";
            return false;
        }

        if (engine.updateVersions(2U, [](const naive_storage_engine::Row&) { return true; },
                                  [](naive_storage_engine::Row& row) { row.value = 7; })) {
            std::cerr << "updateVersions() should fail for key >= maxUniqueKeys\n";
            return false;
        }

        std::function<bool(const naive_storage_engine::Row&)> emptyPredicate;
        if (engine.updateVersions(0U, emptyPredicate,
                                  [](naive_storage_engine::Row& row) { row.value = 9; })) {
            std::cerr << "updateVersions() should fail when predicate callback is empty\n";
            return false;
        }
    }

    return true;
}

bool runVacuumByHorizonTest() {
    naive_storage_engine::Limits limits;
    limits.maxUniqueKeys = 2;
    limits.maxValuesPerKey = 10;

    naive_storage_engine::NaiveStorageEngine engine(limits);

    if (!vacuumWithExclusiveLock(engine, 0U, 5U)) {
        std::cerr << "vacuumByHorizon() should succeed for empty key history\n";
        return false;
    }

    for (int i = 0; i < 6; ++i) {
        if (!setWithExclusiveLock(engine, 0U, i, "vacuum-seed")) {
            std::cerr << "seed set() failed in vacuum test\n";
            return false;
        }
    }

    const auto rowsBeforeVacuum = getWithSharedLock(engine, 0U);
    if (rowsBeforeVacuum.size() != 6U) {
        std::cerr << "unexpected seed row count in vacuum test\n";
        return false;
    }

    const std::uint64_t horizonTimestamp = rowsBeforeVacuum[3].timestamp;
    if (!vacuumWithExclusiveLock(engine, 0U, horizonTimestamp)) {
        std::cerr << "vacuumByHorizon() failed for valid key\n";
        return false;
    }

    const auto rowsAfterVacuum = getWithSharedLock(engine, 0U);
    if (rowsAfterVacuum.size() != 4U) {
        std::cerr << "vacuumByHorizon() should keep boundary predecessor and newer rows\n";
        return false;
    }

    if (rowsAfterVacuum[0].value != 2 || rowsAfterVacuum[0].timestamp != 0) {
        std::cerr << "vacuumByHorizon() should mark retained boundary predecessor with timestamp 0\n";
        return false;
    }

    if (rowsAfterVacuum[1].value != 3 || rowsAfterVacuum[1].timestamp != rowsBeforeVacuum[3].timestamp) {
        std::cerr << "vacuumByHorizon() should preserve first row at/after horizon\n";
        return false;
    }

    if (rowsAfterVacuum[2].value != 4 || rowsAfterVacuum[3].value != 5) {
        std::cerr << "vacuumByHorizon() should preserve newest rows\n";
        return false;
    }

    const auto rowsBeforeNoOpVacuum = rowsAfterVacuum;
    if (!vacuumWithExclusiveLock(engine, 0U, 0U)) {
        std::cerr << "vacuumByHorizon() with horizon 0 should succeed\n";
        return false;
    }

    const auto rowsAfterNoOpVacuum = getWithSharedLock(engine, 0U);
    if (rowsAfterNoOpVacuum.size() != rowsBeforeNoOpVacuum.size()) {
        std::cerr << "vacuumByHorizon() with horizon 0 should be no-op\n";
        return false;
    }

    for (std::size_t i = 0; i < rowsAfterNoOpVacuum.size(); ++i) {
        if (rowsAfterNoOpVacuum[i].value != rowsBeforeNoOpVacuum[i].value ||
            rowsAfterNoOpVacuum[i].timestamp != rowsBeforeNoOpVacuum[i].timestamp ||
            rowsAfterNoOpVacuum[i].metadata != rowsBeforeNoOpVacuum[i].metadata) {
            std::cerr << "vacuumByHorizon() with horizon 0 changed row content\n";
            return false;
        }
    }

    if (!vacuumWithExclusiveLock(engine, 0U, std::numeric_limits<std::uint64_t>::max())) {
        std::cerr << "vacuumByHorizon() with large horizon should succeed\n";
        return false;
    }

    const auto rowsAfterFullVacuum = getWithSharedLock(engine, 0U);
    if (rowsAfterFullVacuum.size() != 1U) {
        std::cerr << "vacuumByHorizon() with large horizon should keep only newest row\n";
        return false;
    }

    if (rowsAfterFullVacuum[0].value != 5 || rowsAfterFullVacuum[0].timestamp != 0) {
        std::cerr << "vacuumByHorizon() with large horizon should normalize newest row timestamp\n";
        return false;
    }

    if (engine.vacuumByHorizon(2U, 1U)) {
        std::cerr << "vacuumByHorizon() should fail for key >= maxUniqueKeys\n";
        return false;
    }

    return true;
}

bool runConcurrentReadTest() {
    naive_storage_engine::Limits limits;
    limits.maxUniqueKeys = 4;
    limits.maxValuesPerKey = 256;

    naive_storage_engine::NaiveStorageEngine engine(limits);

    constexpr int kSeedRows = 120;
    for (int i = 0; i < kSeedRows; ++i) {
        if (!setWithExclusiveLock(engine, 1U, i, "seed")) {
            std::cerr << "seed set() failed in concurrent read test\n";
            return false;
        }
    }

    std::atomic<bool> failed{false};
    constexpr int kReaderThreads = 8;
    constexpr int kReadLoops = 300;
    std::vector<std::thread> threads;
    threads.reserve(kReaderThreads);

    for (int t = 0; t < kReaderThreads; ++t) {
        threads.emplace_back([&engine, &failed]() {
            for (int i = 0; i < kReadLoops; ++i) {
                const auto rows = getWithSharedLock(engine, 1U);
                if (rows.size() != kSeedRows) {
                    failed.store(true);
                    return;
                }

                if (rows.front().value != 0 || rows.back().value != kSeedRows - 1) {
                    failed.store(true);
                    return;
                }
            }
        });
    }

    for (std::thread& t : threads) {
        t.join();
    }

    if (failed.load()) {
        std::cerr << "concurrent readers observed inconsistent results\n";
        return false;
    }

    return true;
}

bool runConcurrentWriteDifferentKeysTest() {
    constexpr std::uint32_t kThreadCount = 4;
    constexpr std::uint32_t kWritesPerThread = 150;

    naive_storage_engine::Limits limits;
    limits.maxUniqueKeys = kThreadCount;
    limits.maxValuesPerKey = kWritesPerThread;
    naive_storage_engine::NaiveStorageEngine engine(limits);

    std::atomic<bool> failed{false};
    std::vector<std::thread> threads;
    threads.reserve(kThreadCount);

    for (std::uint32_t key = 0; key < kThreadCount; ++key) {
        threads.emplace_back([&engine, &failed, key]() {
            for (std::uint32_t i = 0; i < kWritesPerThread; ++i) {
                if (!setWithExclusiveLock(engine, key, static_cast<std::int64_t>(key * 100000 + i),
                                          "multi-key")) {
                    failed.store(true);
                    return;
                }
            }
        });
    }

    for (std::thread& t : threads) {
        t.join();
    }

    if (failed.load()) {
        std::cerr << "concurrent set() failed on independent keys\n";
        return false;
    }

    for (std::uint32_t key = 0; key < kThreadCount; ++key) {
        const auto rows = getWithSharedLock(engine, key);
        if (rows.size() != kWritesPerThread) {
            std::cerr << "wrong version count for key " << key << " after multi-key writes\n";
            return false;
        }
    }

    return true;
}

bool runConcurrentWriteSameKeyTest() {
    constexpr std::uint32_t kThreadCount = 8;
    constexpr std::uint32_t kWritesPerThread = 100;
    constexpr std::uint32_t kTotalWrites = kThreadCount * kWritesPerThread;

    naive_storage_engine::Limits limits;
    limits.maxUniqueKeys = 1;
    limits.maxValuesPerKey = kTotalWrites;
    naive_storage_engine::NaiveStorageEngine engine(limits);

    std::atomic<bool> failed{false};
    std::vector<std::thread> threads;
    threads.reserve(kThreadCount);

    for (std::uint32_t t = 0; t < kThreadCount; ++t) {
        threads.emplace_back([&engine, &failed, t]() {
            for (std::uint32_t i = 0; i < kWritesPerThread; ++i) {
                if (!setWithExclusiveLock(engine, 0U, static_cast<std::int64_t>(t * 100000 + i),
                                          "same-key")) {
                    failed.store(true);
                    return;
                }
            }
        });
    }

    for (std::thread& t : threads) {
        t.join();
    }

    if (failed.load()) {
        std::cerr << "concurrent set() failed for same key\n";
        return false;
    }

    const auto rows = getWithSharedLock(engine, 0U);
    if (rows.size() != kTotalWrites) {
        std::cerr << "wrong row count after same-key writes\n";
        return false;
    }

    for (std::size_t i = 1; i < rows.size(); ++i) {
        if (rows[i - 1].timestamp >= rows[i].timestamp) {
            std::cerr << "timestamps should be strictly increasing in append order\n";
            return false;
        }
    }

    std::vector<std::uint64_t> timestamps;
    timestamps.reserve(rows.size());
    for (const auto& row : rows) {
        timestamps.push_back(row.timestamp);
    }

    std::sort(timestamps.begin(), timestamps.end());
    if (std::adjacent_find(timestamps.begin(), timestamps.end()) != timestamps.end()) {
        std::cerr << "timestamps must be unique\n";
        return false;
    }

    return true;
}

bool runConcurrentClearSmokeTest() {
    naive_storage_engine::Limits limits;
    limits.maxUniqueKeys = 3;
    limits.maxValuesPerKey = 64;

    naive_storage_engine::NaiveStorageEngine engine(limits);

    std::atomic<bool> failed{false};

    std::thread writer([&engine]() {
        for (int i = 0; i < 5000; ++i) {
            const std::uint32_t key = static_cast<std::uint32_t>(i % 3);
            (void)setWithExclusiveLock(engine, key, i, "writer");
        }
    });

    std::thread reader([&engine, &failed]() {
        for (int i = 0; i < 3000; ++i) {
            for (std::uint32_t key = 0; key < 3; ++key) {
                const auto rows = getWithSharedLock(engine, key);
                if (rows.size() > 64) {
                    failed.store(true);
                    return;
                }
            }
        }
    });

    std::thread clearer([&engine]() {
        for (int i = 0; i < 400; ++i) {
            (void)engine.clear();
        }
    });

    writer.join();
    reader.join();
    clearer.join();

    if (failed.load()) {
        std::cerr << "clear() concurrency smoke test observed invalid row counts\n";
        return false;
    }

    if (!engine.clear()) {
        std::cerr << "final clear() failed in clear smoke test\n";
        return false;
    }

    for (std::uint32_t key = 0; key < 3; ++key) {
        if (!getWithSharedLock(engine, key).empty()) {
            std::cerr << "key should be empty after final clear() in clear smoke test\n";
            return false;
        }
    }

    return true;
}

}  // namespace

int main() {
    const bool ok = runBasicBehaviorTest() && runUpdateVersionsBehaviorTest() &&
                    runVacuumByHorizonTest() && runConcurrentReadTest() &&
                    runConcurrentWriteDifferentKeysTest() && runConcurrentWriteSameKeyTest() &&
                    runConcurrentClearSmokeTest();
    return ok ? 0 : 1;
}
