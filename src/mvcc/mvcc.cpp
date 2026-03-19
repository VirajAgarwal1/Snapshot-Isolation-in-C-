#include "mvcc/mvcc.hpp"

#include <algorithm>
#include <limits>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "util/mvcc_metadata_codec.hpp"

namespace {

bool isVisibleForSnapshot(const util::MVCCMetadata& metadata, mvcc::TransactionId transactionId,
                          const mvcc::Snapshot& snapshot,
                          const std::unordered_set<mvcc::TransactionId>& committedTransactions) {
    if (metadata.transactionId == transactionId) {
        return true;
    }

    if (metadata.transactionId == 0) {
        return true;
    }

    if (metadata.transactionId >= snapshot.minTransactionId) {
        return false;
    }

    if (snapshot.activeTransactions.contains(metadata.transactionId)) {
        return false;
    }

    return committedTransactions.contains(metadata.transactionId);
}

}  // namespace

namespace mvcc {

MVCC::MVCC(naive_storage_engine::Limits limits) : storageEngine_(limits) {
}

TransactionId MVCC::startTransaction() {
    const TransactionId transactionId = nextTransactionId_.fetch_add(1);

    Snapshot snapshot;
    {
        std::unique_lock<std::shared_mutex> activeLock(activeTransactionsMutex_);
        snapshot.activeTransactions = activeTransactions_;
        activeTransactions_.insert(transactionId);
        snapshot.minTransactionId =
            *std::min_element(activeTransactions_.begin(), activeTransactions_.end());
    }

    try {
        std::unique_lock<std::shared_mutex> snapshotsLock(transactionSnapshotsMutex_);
        transactionSnapshots_[transactionId] = std::move(snapshot);
        return transactionId;
    } catch (...) {
        (void)abort(transactionId);
        return 0;
    }
}

GetResult MVCC::get(TransactionId transactionId, std::uint32_t key) {
    try {
        const auto snapshot = getSnapshotIfActive(transactionId);
        if (!snapshot.has_value()) {
            return GetResult{};
        }

        std::vector<naive_storage_engine::Row> rows;
        {
            auto keyLock = storageEngine_.acquireSharedLock(key);
            if (!keyLock.has_value()) {
                return GetResult{};
            }

            rows = storageEngine_.get(key);
        }

        std::unordered_set<TransactionId> committedTransactions;
        {
            std::shared_lock<std::shared_mutex> committedLock(committedTransactionsMutex_);
            committedTransactions = committedTransactions_;
        }

        bool foundVisible = false;
        std::size_t latestVisibleIndex = 0;
        TransactionId latestVisibleTransactionId = 0;
        bool latestVisibleIsDead = false;
        std::int64_t latestVisibleValue = 0;

        for (std::size_t index = 0; index < rows.size(); ++index) {
            const auto metadata = util::decodeMVCCMetadata(rows[index].metadata);
            if (!metadata.has_value()) {
                continue;
            }

            if (!isVisibleForSnapshot(metadata.value(), transactionId, snapshot.value(),
                                      committedTransactions)) {
                continue;
            }

            if (!foundVisible || metadata->transactionId > latestVisibleTransactionId ||
                (metadata->transactionId == latestVisibleTransactionId && index > latestVisibleIndex)) {
                foundVisible = true;
                latestVisibleIndex = index;
                latestVisibleTransactionId = metadata->transactionId;
                latestVisibleIsDead = metadata->isDead;
                latestVisibleValue = rows[index].value;
            }
        }

        if (!foundVisible) {
            return GetResult{};
        }

        if (latestVisibleIsDead) {
            return GetResult{GetStatus::Deleted, std::nullopt};
        }

        return GetResult{GetStatus::Found, latestVisibleValue};
    } catch (...) {
        (void)abort(transactionId);
        return GetResult{};
    }
}

bool MVCC::set(TransactionId transactionId, std::uint32_t key, std::int64_t value) {
    return setInternal(transactionId, key, value, false);
}

bool MVCC::deleteKey(TransactionId transactionId, std::uint32_t key) {
    return setInternal(transactionId, key, 0, true);
}

bool MVCC::setInternal(TransactionId transactionId, std::uint32_t key, std::int64_t value, bool isDead) {
    try {
        const auto snapshot = getSnapshotIfActive(transactionId);
        if (!snapshot.has_value()) {
            return false;
        }

        bool shouldAbort = false;
        bool writeSucceeded = false;

        {
            auto keyLock = storageEngine_.acquireExclusiveLock(key);
            if (!keyLock.has_value()) {
                shouldAbort = true;
            } else {
                const std::vector<naive_storage_engine::Row> rows = storageEngine_.get(key);
                if (!rows.empty()) {
                    const auto latestMetadata = util::decodeMVCCMetadata(rows.back().metadata);
                    if (!latestMetadata.has_value()) {
                        shouldAbort = true;
                    } else if (latestMetadata->transactionId != transactionId &&
                               latestMetadata->transactionId >= snapshot->minTransactionId) {
                        shouldAbort = true;
                    }
                }

                if (!shouldAbort) {
                    const std::string encodedMetadata =
                        util::encodeMVCCMetadata(util::MVCCMetadata{transactionId, isDead});
                    writeSucceeded = storageEngine_.set(key, value, encodedMetadata);
                    if (!writeSucceeded) {
                        shouldAbort = true;
                    }
                }
            }
        }

        if (shouldAbort) {
            (void)abort(transactionId);
            return false;
        }

        return writeSucceeded;
    } catch (...) {
        (void)abort(transactionId);
        return false;
    }
}

bool MVCC::commit(TransactionId transactionId) {
    try {
        {
            std::unique_lock<std::shared_mutex> activeLock(activeTransactionsMutex_);
            if (!activeTransactions_.erase(transactionId)) {
                return false;
            }
        }

        {
            std::unique_lock<std::shared_mutex> committedLock(committedTransactionsMutex_);
            committedTransactions_.insert(transactionId);
        }

        {
            std::unique_lock<std::shared_mutex> snapshotsLock(transactionSnapshotsMutex_);
            transactionSnapshots_.erase(transactionId);
        }

        return true;
    } catch (...) {
        (void)abort(transactionId);
        return false;
    }
}

bool MVCC::abort(TransactionId transactionId) {
    bool removed = false;
    {
        std::unique_lock<std::shared_mutex> activeLock(activeTransactionsMutex_);
        removed = activeTransactions_.erase(transactionId) > 0;
    }

    {
        std::unique_lock<std::shared_mutex> snapshotsLock(transactionSnapshotsMutex_);
        transactionSnapshots_.erase(transactionId);
    }

    return removed;
}

bool MVCC::vacuum() {
    std::unordered_set<TransactionId> activeTransactions;
    {
        std::shared_lock<std::shared_mutex> activeLock(activeTransactionsMutex_);
        activeTransactions = activeTransactions_;
    }

    {
        std::unique_lock<std::shared_mutex> snapshotsLock(transactionSnapshotsMutex_);
        for (auto iterator = transactionSnapshots_.begin(); iterator != transactionSnapshots_.end();) {
            if (activeTransactions.contains(iterator->first)) {
                ++iterator;
                continue;
            }

            iterator = transactionSnapshots_.erase(iterator);
        }
    }

    TransactionId dbHorizon = nextTransactionId_.load();
    {
        std::shared_lock<std::shared_mutex> snapshotsLock(transactionSnapshotsMutex_);

        bool hasHorizon = false;
        TransactionId minHorizon = std::numeric_limits<TransactionId>::max();

        for (TransactionId transactionId : activeTransactions) {
            const auto snapshotIterator = transactionSnapshots_.find(transactionId);
            if (snapshotIterator == transactionSnapshots_.end()) {
                continue;
            }

            minHorizon = std::min(minHorizon, snapshotIterator->second.minTransactionId);
            hasHorizon = true;
        }

        if (hasHorizon) {
            dbHorizon = minHorizon;
        }
    }

    const naive_storage_engine::Limits limits = storageEngine_.limits();
    for (std::uint32_t key = 0; key < limits.maxUniqueKeys; ++key) {
        auto keyLock = storageEngine_.acquireExclusiveLock(key);
        if (!keyLock.has_value()) {
            continue;
        }

        const std::vector<naive_storage_engine::Row> rowsBeforeVacuum = storageEngine_.get(key);
        if (rowsBeforeVacuum.empty()) {
            continue;
        }

        std::optional<std::size_t> firstAtOrAboveHorizonIndex;
        for (std::size_t index = 0; index < rowsBeforeVacuum.size(); ++index) {
            const auto metadata = util::decodeMVCCMetadata(rowsBeforeVacuum[index].metadata);
            if (!metadata.has_value()) {
                continue;
            }

            if (metadata->transactionId >= dbHorizon) {
                firstAtOrAboveHorizonIndex = index;
                break;
            }
        }

        if (firstAtOrAboveHorizonIndex.has_value() && *firstAtOrAboveHorizonIndex == 0) {
            continue;
        }

        std::uint64_t vacuumBoundaryTimestamp = std::numeric_limits<std::uint64_t>::max();
        if (firstAtOrAboveHorizonIndex.has_value()) {
            vacuumBoundaryTimestamp = rowsBeforeVacuum[*firstAtOrAboveHorizonIndex].timestamp;
        }

        if (!storageEngine_.vacuumByHorizon(key, vacuumBoundaryTimestamp)) {
            return false;
        }

        const std::vector<naive_storage_engine::Row> rowsAfterVacuum = storageEngine_.get(key);
        if (rowsAfterVacuum.empty()) {
            continue;
        }

        const auto retainedMetadata = util::decodeMVCCMetadata(rowsAfterVacuum.front().metadata);
        if (!retainedMetadata.has_value() || retainedMetadata->transactionId == 0) {
            continue;
        }

        const std::string previousMetadata = rowsAfterVacuum.front().metadata;
        const std::string zeroTransactionMetadata =
            util::encodeMVCCMetadata(util::MVCCMetadata{0, retainedMetadata->isDead});

        bool matched = false;
        const bool updated = storageEngine_.updateVersions(
            key,
            [&matched, &previousMetadata](const naive_storage_engine::Row& row) {
                if (matched) {
                    return false;
                }

                if (row.timestamp != 0 || row.metadata != previousMetadata) {
                    return false;
                }

                matched = true;
                return true;
            },
            [&zeroTransactionMetadata](naive_storage_engine::Row& row) {
                row.metadata = zeroTransactionMetadata;
            });

        if (!updated) {
            return false;
        }
    }

    {
        std::unique_lock<std::shared_mutex> committedLock(committedTransactionsMutex_);
        for (auto iterator = committedTransactions_.begin(); iterator != committedTransactions_.end();) {
            if (*iterator < dbHorizon) {
                iterator = committedTransactions_.erase(iterator);
                continue;
            }

            ++iterator;
        }
    }

    return true;
}

std::optional<Snapshot> MVCC::getSnapshotIfActive(TransactionId transactionId) const {
    if (!isTransactionActive(transactionId)) {
        return std::nullopt;
    }

    std::shared_lock<std::shared_mutex> snapshotsLock(transactionSnapshotsMutex_);
    const auto iterator = transactionSnapshots_.find(transactionId);
    if (iterator == transactionSnapshots_.end()) {
        return std::nullopt;
    }

    return iterator->second;
}

bool MVCC::isTransactionActive(TransactionId transactionId) const {
    std::shared_lock<std::shared_mutex> activeLock(activeTransactionsMutex_);
    return activeTransactions_.contains(transactionId);
}

}  // namespace mvcc