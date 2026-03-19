#ifndef MVCC_MVCC_HPP
#define MVCC_MVCC_HPP

#include <atomic>
#include <cstdint>
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "naive_storage_engine/naive_storage_engine.hpp"

namespace mvcc {

using TransactionId = std::uint64_t;

enum class GetStatus {
    Found,
    Deleted,
    NotFound,
};

struct GetResult {
    GetStatus status = GetStatus::NotFound;
    std::optional<std::int64_t> value;
};

struct Snapshot {
    std::unordered_set<TransactionId> activeTransactions;
    TransactionId minTransactionId = 0;
};

class MVCC {
public:
    explicit MVCC(naive_storage_engine::Limits limits = {});

    TransactionId startTransaction();
    GetResult get(TransactionId transactionId, std::uint32_t key);
    bool set(TransactionId transactionId, std::uint32_t key, std::int64_t value);
    bool deleteKey(TransactionId transactionId, std::uint32_t key);
    bool commit(TransactionId transactionId);
    bool abort(TransactionId transactionId);
    bool vacuum();

private:
    bool setInternal(TransactionId transactionId, std::uint32_t key, std::int64_t value, bool isDead);
    std::optional<Snapshot> getSnapshotIfActive(TransactionId transactionId) const;
    bool isTransactionActive(TransactionId transactionId) const;

    naive_storage_engine::NaiveStorageEngine storageEngine_;
    std::atomic<TransactionId> nextTransactionId_{1};

    mutable std::shared_mutex activeTransactionsMutex_;
    std::unordered_set<TransactionId> activeTransactions_;

    mutable std::shared_mutex committedTransactionsMutex_;
    std::unordered_set<TransactionId> committedTransactions_;

    mutable std::shared_mutex transactionSnapshotsMutex_;
    std::unordered_map<TransactionId, Snapshot> transactionSnapshots_;
};

}  // namespace mvcc

#endif