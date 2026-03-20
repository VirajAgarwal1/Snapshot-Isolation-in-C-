#include "mvcc/mvcc.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <optional>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace {

using SteadyClock = std::chrono::steady_clock;
using SystemClock = std::chrono::system_clock;
using TransactionId = mvcc::TransactionId;

constexpr std::size_t kThreadCount = 10;
constexpr std::uint32_t kTotalKeys = 1000;
constexpr double kKeyRangeRatio = 0.40;
constexpr std::uint32_t kMinTransactionsPerThread = 200;
constexpr std::uint32_t kMaxTransactionsPerThread = 1000;
constexpr std::uint32_t kMinOperationsPerTransaction = 5;
constexpr std::uint32_t kMaxOperationsPerTransaction = 50;
constexpr std::uint32_t kReadPercent = 60;
constexpr std::uint32_t kUpdatePercent = 35;
constexpr std::uint32_t kDeletePercent = 5;
constexpr std::uint32_t kCommitPercent = 95;
constexpr std::int64_t kBaseValue = 1000;
constexpr std::uint32_t kMaxValuesPerKey = 20000;
constexpr std::int64_t kRandomValueMin = 1;
constexpr std::int64_t kRandomValueMax = 1000000;

static_assert(kReadPercent + kUpdatePercent + kDeletePercent == 100,
              "operation percentages must add to 100");

enum class OperationType {
    StartTransaction,
    Get,
    Set,
    DeleteKey,
    Commit,
    Abort,
};

enum class TransactionFinalState {
    Open,
    Committed,
    Aborted,
};

struct LogEntry {
    OperationType operationType = OperationType::Get;
    std::size_t threadId = 0;
    TransactionId transactionId = 0;
    std::uint64_t requestTimestampNs = 0;
    std::uint64_t responseTimestampNs = 0;
    std::optional<std::uint32_t> key;
    std::optional<std::int64_t> requestedValue;
    std::optional<std::int64_t> observedValue;
    mvcc::GetStatus getStatus = mvcc::GetStatus::NotFound;
    bool success = false;
    std::uint32_t rangeStart = 0;
    std::uint32_t rangeEndExclusive = 0;
    std::string note;
    std::vector<TransactionId> derivedActiveTransactions;
    TransactionId derivedMinTransactionId = 0;
};

struct TransactionInfo {
    TransactionId transactionId = 0;
    std::size_t threadId = 0;
    std::uint64_t startRequestTimestampNs = 0;
    std::uint64_t startResponseTimestampNs = 0;
    std::optional<std::uint64_t> closeRequestTimestampNs;
    std::optional<std::uint64_t> closeResponseTimestampNs;
    TransactionFinalState finalState = TransactionFinalState::Open;
    std::vector<TransactionId> snapshotActiveTransactions;
    TransactionId snapshotMinTransactionId = 0;
};

struct ExpectedRead {
    mvcc::GetStatus status = mvcc::GetStatus::NotFound;
    std::optional<std::int64_t> value;
    TransactionId sourceTransactionId = 0;
    std::string reason;
};

struct KeyMismatch {
    std::size_t timelineIndex = 0;
    std::uint64_t requestTimestampNs = 0;
    TransactionId transactionId = 0;
    mvcc::GetStatus observedStatus = mvcc::GetStatus::NotFound;
    std::optional<std::int64_t> observedValue;
    mvcc::GetStatus expectedStatus = mvcc::GetStatus::NotFound;
    std::optional<std::int64_t> expectedValue;
    std::string reason;
};

struct VerificationStats {
    std::size_t totalTransactions = 0;
    std::size_t totalOperations = 0;
    std::size_t totalGets = 0;
    std::size_t checkedGets = 0;
    std::size_t mismatches = 0;
};

std::uint64_t nowNs(const SteadyClock::time_point& experimentStart) {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(SteadyClock::now() - experimentStart)
            .count());
}

std::string makeExperimentNameIst() {
    const auto adjustedNow = SystemClock::now() + std::chrono::hours(5) + std::chrono::minutes(30);
    const std::time_t adjustedTime = SystemClock::to_time_t(adjustedNow);
    const std::tm timeInfo = *std::gmtime(&adjustedTime);

    std::ostringstream builder;
    builder << std::put_time(&timeInfo, "%Y%m%d_%H%M%S") << "_IST";
    return builder.str();
}

std::string operationTypeToString(OperationType operationType) {
    switch (operationType) {
        case OperationType::StartTransaction:
            return "START_TXN";
        case OperationType::Get:
            return "GET";
        case OperationType::Set:
            return "SET";
        case OperationType::DeleteKey:
            return "DELETE";
        case OperationType::Commit:
            return "COMMIT";
        case OperationType::Abort:
            return "ABORT";
    }

    return "UNKNOWN";
}

std::string getStatusToString(mvcc::GetStatus getStatus) {
    switch (getStatus) {
        case mvcc::GetStatus::Found:
            return "FOUND";
        case mvcc::GetStatus::Deleted:
            return "DELETED";
        case mvcc::GetStatus::NotFound:
            return "NOT_FOUND";
    }

    return "UNKNOWN";
}

std::string transactionIdsToString(const std::vector<TransactionId>& transactionIds) {
    if (transactionIds.empty()) {
        return "-";
    }

    std::ostringstream builder;
    for (std::size_t index = 0; index < transactionIds.size(); ++index) {
        if (index > 0) {
            builder << ",";
        }
        builder << transactionIds[index];
    }

    return builder.str();
}

bool isWriteOperation(const LogEntry& entry) {
    return entry.operationType == OperationType::Set || entry.operationType == OperationType::DeleteKey;
}

bool isVisibleCommittedWrite(const TransactionInfo& writerTransaction,
                             const TransactionInfo& readerTransaction) {
    if (writerTransaction.finalState != TransactionFinalState::Committed) {
        return false;
    }

    if (writerTransaction.transactionId >= readerTransaction.snapshotMinTransactionId) {
        return false;
    }

    return !std::binary_search(readerTransaction.snapshotActiveTransactions.begin(),
                               readerTransaction.snapshotActiveTransactions.end(),
                               writerTransaction.transactionId);
}

ExpectedRead computeExpectedRead(
    const LogEntry& getEntry, const TransactionInfo& readerTransaction,
    const std::vector<LogEntry>& keyTimeline,
    const std::unordered_map<TransactionId, TransactionInfo>& transactionInfoById) {
    const LogEntry* latestOwnWrite = nullptr;
    for (const LogEntry& entry : keyTimeline) {
        if (!isWriteOperation(entry) || !entry.success || entry.transactionId != getEntry.transactionId) {
            continue;
        }

        if (entry.responseTimestampNs > getEntry.requestTimestampNs) {
            continue;
        }

        if (latestOwnWrite == nullptr || entry.responseTimestampNs > latestOwnWrite->responseTimestampNs) {
            latestOwnWrite = &entry;
        }
    }

    if (latestOwnWrite != nullptr) {
        if (latestOwnWrite->operationType == OperationType::DeleteKey) {
            return ExpectedRead{mvcc::GetStatus::Deleted, std::nullopt,
                                latestOwnWrite->transactionId, "latest own write is delete"};
        }

        return ExpectedRead{mvcc::GetStatus::Found, latestOwnWrite->requestedValue,
                            latestOwnWrite->transactionId, "latest own write is set"};
    }

    const LogEntry* latestVisibleCommittedWrite = nullptr;
    for (const LogEntry& entry : keyTimeline) {
        if (!isWriteOperation(entry) || !entry.success || entry.transactionId == getEntry.transactionId) {
            continue;
        }

        const auto transactionIt = transactionInfoById.find(entry.transactionId);
        if (transactionIt == transactionInfoById.end()) {
            continue;
        }

        if (!isVisibleCommittedWrite(transactionIt->second, readerTransaction)) {
            continue;
        }

        if (latestVisibleCommittedWrite == nullptr ||
            entry.transactionId > latestVisibleCommittedWrite->transactionId ||
            (entry.transactionId == latestVisibleCommittedWrite->transactionId &&
             entry.responseTimestampNs > latestVisibleCommittedWrite->responseTimestampNs)) {
            latestVisibleCommittedWrite = &entry;
        }
    }

    if (latestVisibleCommittedWrite == nullptr) {
        return ExpectedRead{mvcc::GetStatus::NotFound, std::nullopt, 0,
                            "no visible committed write found"};
    }

    if (latestVisibleCommittedWrite->operationType == OperationType::DeleteKey) {
        return ExpectedRead{mvcc::GetStatus::Deleted, std::nullopt,
                            latestVisibleCommittedWrite->transactionId,
                            "latest visible committed write is delete"};
    }

    return ExpectedRead{mvcc::GetStatus::Found, latestVisibleCommittedWrite->requestedValue,
                        latestVisibleCommittedWrite->transactionId,
                        "latest visible committed write is set"};
}

void dumpLogFile(const std::filesystem::path& filePath, const std::vector<LogEntry>& entries) {
    std::ofstream output(filePath);
    for (std::size_t index = 0; index < entries.size(); ++index) {
        const LogEntry& entry = entries[index];
        output << "event_index:\t" << index << '\n';
        output << "request_timestamp_ns:\t" << entry.requestTimestampNs << '\n';
        output << "response_timestamp_ns:\t" << entry.responseTimestampNs << '\n';
        output << "thread_id:\t" << entry.threadId << '\n';
        output << "txn_id:\t" << entry.transactionId << '\n';
        output << "operation:\t" << operationTypeToString(entry.operationType) << '\n';
        output << "success:\t" << (entry.success ? "true" : "false") << '\n';
        output << "key:\t" << (entry.key.has_value() ? std::to_string(entry.key.value()) : "-")
               << '\n';
        output << "requested_value:\t"
               << (entry.requestedValue.has_value() ? std::to_string(entry.requestedValue.value())
                                                   : "-")
               << '\n';
        output << "observed_value:\t"
               << (entry.observedValue.has_value() ? std::to_string(entry.observedValue.value())
                                                  : "-")
               << '\n';
        output << "get_status:\t" << getStatusToString(entry.getStatus) << '\n';
        output << "key_range:\t[" << entry.rangeStart << ", " << entry.rangeEndExclusive << ")\n";
        output << "snapshot_min_txn_id:\t"
               << (entry.derivedMinTransactionId == 0 ? std::string("-")
                                                     : std::to_string(entry.derivedMinTransactionId))
               << '\n';
        output << "snapshot_active_txns:\t"
               << transactionIdsToString(entry.derivedActiveTransactions) << '\n';
        output << "note:\t" << (entry.note.empty() ? "-" : entry.note) << "\n\n";
    }
}

void dumpTimelineFile(const std::filesystem::path& filePath, std::uint32_t key,
                      const std::vector<LogEntry>& keyTimeline,
                      const std::vector<KeyMismatch>& mismatches,
                      const std::unordered_map<TransactionId, TransactionInfo>& transactionInfoById) {
    std::ofstream output(filePath);
    output << "key:\t" << key << '\n';
    output << "mismatch_count:\t" << mismatches.size() << "\n\n";

    for (const KeyMismatch& mismatch : mismatches) {
        output << "mismatch_timeline_index:\t" << mismatch.timelineIndex << '\n';
        output << "txn_id:\t" << mismatch.transactionId << '\n';
        output << "get_request_timestamp_ns:\t" << mismatch.requestTimestampNs << '\n';
        output << "observed_status:\t" << getStatusToString(mismatch.observedStatus) << '\n';
        output << "observed_value:\t"
               << (mismatch.observedValue.has_value() ? std::to_string(mismatch.observedValue.value())
                                                     : "-")
               << '\n';
        output << "expected_status:\t" << getStatusToString(mismatch.expectedStatus) << '\n';
        output << "expected_value:\t"
               << (mismatch.expectedValue.has_value() ? std::to_string(mismatch.expectedValue.value())
                                                     : "-")
               << '\n';
        output << "reason:\t" << mismatch.reason << "\n\n";
    }

    output << "timeline:\n\n";
    for (std::size_t index = 0; index < keyTimeline.size(); ++index) {
        const LogEntry& entry = keyTimeline[index];
        const bool isMismatch = std::any_of(mismatches.begin(), mismatches.end(),
                                            [index](const KeyMismatch& mismatch) {
                                                return mismatch.timelineIndex == index;
                                            });
        output << "timeline_index:\t" << index;
        if (isMismatch) {
            output << "\t<< MISMATCH";
        }
        output << '\n';
        output << "request_timestamp_ns:\t" << entry.requestTimestampNs << '\n';
        output << "response_timestamp_ns:\t" << entry.responseTimestampNs << '\n';
        output << "txn_id:\t" << entry.transactionId << '\n';
        output << "thread_id:\t" << entry.threadId << '\n';
        output << "operation:\t" << operationTypeToString(entry.operationType) << '\n';
        output << "success:\t" << (entry.success ? "true" : "false") << '\n';
        output << "requested_value:\t"
               << (entry.requestedValue.has_value() ? std::to_string(entry.requestedValue.value())
                                                   : "-")
               << '\n';
        output << "observed_value:\t"
               << (entry.observedValue.has_value() ? std::to_string(entry.observedValue.value())
                                                  : "-")
               << '\n';
        output << "get_status:\t" << getStatusToString(entry.getStatus) << '\n';
        const auto transactionIt = transactionInfoById.find(entry.transactionId);
        if (transactionIt != transactionInfoById.end()) {
            std::string finalState = "OPEN";
            if (transactionIt->second.finalState == TransactionFinalState::Committed) {
                finalState = "COMMITTED";
            } else if (transactionIt->second.finalState == TransactionFinalState::Aborted) {
                finalState = "ABORTED";
            }
            output << "txn_final_state:\t" << finalState << '\n';
        }
        output << '\n';
    }
}

std::uint32_t bandSize() {
    const auto computed = static_cast<std::uint32_t>(static_cast<double>(kTotalKeys) * kKeyRangeRatio);
    return std::max<std::uint32_t>(1, computed);
}

std::unordered_map<TransactionId, TransactionInfo> buildTransactionInfo(
    const std::vector<LogEntry>& allEntries) {
    std::unordered_map<TransactionId, TransactionInfo> transactionInfoById;

    for (const LogEntry& entry : allEntries) {
        if (entry.transactionId == 0) {
            continue;
        }

        if (entry.operationType == OperationType::StartTransaction) {
            TransactionInfo& info = transactionInfoById[entry.transactionId];
            info.transactionId = entry.transactionId;
            info.threadId = entry.threadId;
            info.startRequestTimestampNs = entry.requestTimestampNs;
            info.startResponseTimestampNs = entry.responseTimestampNs;
            info.snapshotActiveTransactions = entry.derivedActiveTransactions;
            info.snapshotMinTransactionId = entry.derivedMinTransactionId;
            continue;
        }

        if (entry.operationType == OperationType::Commit && entry.success) {
            TransactionInfo& info = transactionInfoById[entry.transactionId];
            info.closeRequestTimestampNs = entry.requestTimestampNs;
            info.closeResponseTimestampNs = entry.responseTimestampNs;
            info.finalState = TransactionFinalState::Committed;
            continue;
        }

        if (entry.operationType == OperationType::Abort) {
            TransactionInfo& info = transactionInfoById[entry.transactionId];
            info.closeRequestTimestampNs = entry.requestTimestampNs;
            info.closeResponseTimestampNs = entry.responseTimestampNs;
            info.finalState = TransactionFinalState::Aborted;
        }
    }

    return transactionInfoById;
}

void pushImplicitAbort(std::vector<LogEntry>& threadLog, std::size_t threadId, TransactionId transactionId,
                       std::uint64_t timestampNs, std::uint32_t rangeStart,
                       std::uint32_t rangeEndExclusive, const std::string& note) {
    LogEntry abortEntry;
    abortEntry.operationType = OperationType::Abort;
    abortEntry.threadId = threadId;
    abortEntry.transactionId = transactionId;
    abortEntry.requestTimestampNs = timestampNs;
    abortEntry.responseTimestampNs = timestampNs;
    abortEntry.success = true;
    abortEntry.rangeStart = rangeStart;
    abortEntry.rangeEndExclusive = rangeEndExclusive;
    abortEntry.note = note;
    threadLog.push_back(abortEntry);
}

void runWorker(std::size_t threadId, mvcc::MVCC& store, const SteadyClock::time_point& experimentStart,
               std::vector<LogEntry>& threadLog) {
    std::mt19937_64 randomGenerator(static_cast<std::uint64_t>(threadId + 1) * 982451653ULL);
    std::uniform_int_distribution<std::uint32_t> transactionCountDist(kMinTransactionsPerThread,
                                                                      kMaxTransactionsPerThread);
    std::uniform_int_distribution<std::uint32_t> operationCountDist(kMinOperationsPerTransaction,
                                                                    kMaxOperationsPerTransaction);
    std::uniform_int_distribution<std::int64_t> randomValueDist(kRandomValueMin, kRandomValueMax);
    std::uniform_int_distribution<std::uint32_t> opRollDist(1, 100);
    std::uniform_int_distribution<std::uint32_t> finalRollDist(1, 100);

    const std::uint32_t txnCount = transactionCountDist(randomGenerator);
    const std::uint32_t keyBandSize = bandSize();
    std::uniform_int_distribution<std::uint32_t> rangeStartDist(0, kTotalKeys - keyBandSize);

    for (std::uint32_t txnIndex = 0; txnIndex < txnCount; ++txnIndex) {
        const std::uint32_t rangeStart = rangeStartDist(randomGenerator);
        const std::uint32_t rangeEndExclusive = rangeStart + keyBandSize;
        std::uniform_int_distribution<std::uint32_t> keyDist(rangeStart, rangeEndExclusive - 1);

        const std::uint64_t startRequestTimestampNs = nowNs(experimentStart);
        const mvcc::StartResult startResult = store.startTransaction();
        const std::uint64_t startResponseTimestampNs = nowNs(experimentStart);
        const TransactionId transactionId = startResult.transactionId;

        std::vector<TransactionId> snapshotActive(startResult.snapshot.activeTransactions.begin(),
                                                  startResult.snapshot.activeTransactions.end());
        std::sort(snapshotActive.begin(), snapshotActive.end());

        LogEntry startEntry;
        startEntry.operationType = OperationType::StartTransaction;
        startEntry.threadId = threadId;
        startEntry.transactionId = transactionId;
        startEntry.requestTimestampNs = startRequestTimestampNs;
        startEntry.responseTimestampNs = startResponseTimestampNs;
        startEntry.success = transactionId != 0;
        startEntry.rangeStart = rangeStart;
        startEntry.rangeEndExclusive = rangeEndExclusive;
        startEntry.derivedActiveTransactions = std::move(snapshotActive);
        startEntry.derivedMinTransactionId = startResult.snapshot.minTransactionId;
        threadLog.push_back(startEntry);

        if (transactionId == 0) {
            continue;
        }

        const std::uint32_t operationCount = operationCountDist(randomGenerator);
        bool transactionClosedByFailure = false;

        for (std::uint32_t operationIndex = 0; operationIndex < operationCount; ++operationIndex) {
            const std::uint32_t key = keyDist(randomGenerator);
            const std::uint32_t opRoll = opRollDist(randomGenerator);

            if (opRoll <= kReadPercent) {
                const std::uint64_t requestTimestampNs = nowNs(experimentStart);
                const mvcc::GetResult result = store.get(transactionId, key);
                const std::uint64_t responseTimestampNs = nowNs(experimentStart);

                LogEntry getEntry;
                getEntry.operationType = OperationType::Get;
                getEntry.threadId = threadId;
                getEntry.transactionId = transactionId;
                getEntry.requestTimestampNs = requestTimestampNs;
                getEntry.responseTimestampNs = responseTimestampNs;
                getEntry.key = key;
                getEntry.observedValue = result.value;
                getEntry.getStatus = result.status;
                getEntry.success = result.status != mvcc::GetStatus::NotFound;
                getEntry.rangeStart = rangeStart;
                getEntry.rangeEndExclusive = rangeEndExclusive;
                threadLog.push_back(getEntry);

                if (!getEntry.success) {
                    pushImplicitAbort(threadLog, threadId, transactionId, responseTimestampNs,
                                      rangeStart, rangeEndExclusive,
                                      "implicit abort after failed GET");
                    transactionClosedByFailure = true;
                    break;
                }

                continue;
            }

            if (opRoll <= kReadPercent + kUpdatePercent) {
                const std::int64_t value = randomValueDist(randomGenerator);
                const std::uint64_t requestTimestampNs = nowNs(experimentStart);
                const bool success = store.set(transactionId, key, value);
                const std::uint64_t responseTimestampNs = nowNs(experimentStart);

                LogEntry setEntry;
                setEntry.operationType = OperationType::Set;
                setEntry.threadId = threadId;
                setEntry.transactionId = transactionId;
                setEntry.requestTimestampNs = requestTimestampNs;
                setEntry.responseTimestampNs = responseTimestampNs;
                setEntry.key = key;
                setEntry.requestedValue = value;
                setEntry.success = success;
                setEntry.rangeStart = rangeStart;
                setEntry.rangeEndExclusive = rangeEndExclusive;
                threadLog.push_back(setEntry);

                if (!success) {
                    pushImplicitAbort(threadLog, threadId, transactionId, responseTimestampNs,
                                      rangeStart, rangeEndExclusive,
                                      "implicit abort after failed SET");
                    transactionClosedByFailure = true;
                    break;
                }

                continue;
            }

            const std::uint64_t requestTimestampNs = nowNs(experimentStart);
            const bool success = store.deleteKey(transactionId, key);
            const std::uint64_t responseTimestampNs = nowNs(experimentStart);

            LogEntry deleteEntry;
            deleteEntry.operationType = OperationType::DeleteKey;
            deleteEntry.threadId = threadId;
            deleteEntry.transactionId = transactionId;
            deleteEntry.requestTimestampNs = requestTimestampNs;
            deleteEntry.responseTimestampNs = responseTimestampNs;
            deleteEntry.key = key;
            deleteEntry.success = success;
            deleteEntry.rangeStart = rangeStart;
            deleteEntry.rangeEndExclusive = rangeEndExclusive;
            threadLog.push_back(deleteEntry);

            if (!success) {
                pushImplicitAbort(threadLog, threadId, transactionId, responseTimestampNs,
                                  rangeStart, rangeEndExclusive,
                                  "implicit abort after failed DELETE");
                transactionClosedByFailure = true;
                break;
            }
        }

        if (transactionClosedByFailure) {
            continue;
        }

        if (finalRollDist(randomGenerator) <= kCommitPercent) {
            const std::uint64_t requestTimestampNs = nowNs(experimentStart);
            const bool success = store.commit(transactionId);
            const std::uint64_t responseTimestampNs = nowNs(experimentStart);

            LogEntry commitEntry;
            commitEntry.operationType = OperationType::Commit;
            commitEntry.threadId = threadId;
            commitEntry.transactionId = transactionId;
            commitEntry.requestTimestampNs = requestTimestampNs;
            commitEntry.responseTimestampNs = responseTimestampNs;
            commitEntry.success = success;
            commitEntry.rangeStart = rangeStart;
            commitEntry.rangeEndExclusive = rangeEndExclusive;
            threadLog.push_back(commitEntry);

            if (!success) {
                pushImplicitAbort(threadLog, threadId, transactionId, responseTimestampNs,
                                  rangeStart, rangeEndExclusive,
                                  "implicit abort after failed COMMIT");
            }

            continue;
        }

        const std::uint64_t requestTimestampNs = nowNs(experimentStart);
        const bool success = store.abort(transactionId);
        const std::uint64_t responseTimestampNs = nowNs(experimentStart);

        LogEntry abortEntry;
        abortEntry.operationType = OperationType::Abort;
        abortEntry.threadId = threadId;
        abortEntry.transactionId = transactionId;
        abortEntry.requestTimestampNs = requestTimestampNs;
        abortEntry.responseTimestampNs = responseTimestampNs;
        abortEntry.success = success;
        abortEntry.rangeStart = rangeStart;
        abortEntry.rangeEndExclusive = rangeEndExclusive;
        threadLog.push_back(abortEntry);
    }
}

std::vector<LogEntry> combineLogs(const std::vector<LogEntry>& seedLog,
                                  const std::vector<std::vector<LogEntry>>& threadLogs) {
    std::vector<LogEntry> allEntries = seedLog;
    for (const std::vector<LogEntry>& threadLog : threadLogs) {
        allEntries.insert(allEntries.end(), threadLog.begin(), threadLog.end());
    }

    std::sort(allEntries.begin(), allEntries.end(), [](const LogEntry& left, const LogEntry& right) {
        if (left.requestTimestampNs != right.requestTimestampNs) {
            return left.requestTimestampNs < right.requestTimestampNs;
        }

        if (left.responseTimestampNs != right.responseTimestampNs) {
            return left.responseTimestampNs < right.responseTimestampNs;
        }

        return left.transactionId < right.transactionId;
    });

    return allEntries;
}

VerificationStats verifyAndSaveTimelines(
    const std::vector<LogEntry>& allEntries,
    const std::unordered_map<TransactionId, TransactionInfo>& transactionInfoById,
    const std::filesystem::path& savedTimelineDir) {
    VerificationStats stats;
    std::unordered_map<std::uint32_t, std::vector<LogEntry>> keyTimelines;

    for (const LogEntry& entry : allEntries) {
        if (entry.operationType == OperationType::StartTransaction) {
            ++stats.totalTransactions;
        }

        if (entry.operationType == OperationType::Get || entry.operationType == OperationType::Set ||
            entry.operationType == OperationType::DeleteKey || entry.operationType == OperationType::Commit ||
            entry.operationType == OperationType::Abort) {
            ++stats.totalOperations;
        }

        if (entry.key.has_value() &&
            (entry.operationType == OperationType::Get || isWriteOperation(entry))) {
            keyTimelines[entry.key.value()].push_back(entry);
        }
    }

    for (auto& [key, timeline] : keyTimelines) {
        std::sort(timeline.begin(), timeline.end(), [](const LogEntry& left, const LogEntry& right) {
            if (left.requestTimestampNs != right.requestTimestampNs) {
                return left.requestTimestampNs < right.requestTimestampNs;
            }

            if (left.responseTimestampNs != right.responseTimestampNs) {
                return left.responseTimestampNs < right.responseTimestampNs;
            }

            return left.transactionId < right.transactionId;
        });

        std::vector<KeyMismatch> mismatches;
        for (std::size_t index = 0; index < timeline.size(); ++index) {
            const LogEntry& entry = timeline[index];
            if (entry.operationType != OperationType::Get) {
                continue;
            }

            ++stats.totalGets;
            if (!entry.success) {
                continue;
            }

            const auto txnIt = transactionInfoById.find(entry.transactionId);
            if (txnIt == transactionInfoById.end()) {
                continue;
            }

            const ExpectedRead expected = computeExpectedRead(entry, txnIt->second, timeline,
                                                              transactionInfoById);
            ++stats.checkedGets;

            const bool valueMatches = expected.value == entry.observedValue;
            if (expected.status == entry.getStatus && valueMatches) {
                continue;
            }

            ++stats.mismatches;
            mismatches.push_back(KeyMismatch{index,
                                             entry.requestTimestampNs,
                                             entry.transactionId,
                                             entry.getStatus,
                                             entry.observedValue,
                                             expected.status,
                                             expected.value,
                                             expected.reason});
        }

        if (mismatches.empty()) {
            continue;
        }

        std::filesystem::create_directories(savedTimelineDir);
        dumpTimelineFile(savedTimelineDir / ("key_" + std::to_string(key) + ".txt"), key, timeline,
                         mismatches, transactionInfoById);
    }

    return stats;
}

}  // namespace

int main() {
    const std::string experimentName = makeExperimentNameIst();
    const std::filesystem::path systemTestRoot = SYSTEM_TEST_SOURCE_DIR;
    const std::filesystem::path logsDir = systemTestRoot / "logs" / experimentName;
    const std::filesystem::path savedTimelinesDir = systemTestRoot / "saved_timelines" / experimentName;

    const SteadyClock::time_point experimentStart = SteadyClock::now();

    naive_storage_engine::Limits limits;
    limits.maxUniqueKeys = kTotalKeys;
    limits.maxValuesPerKey = kMaxValuesPerKey;
    mvcc::MVCC store(limits);

    std::vector<LogEntry> seedLog;
    const std::uint64_t seedStartRequestNs = nowNs(experimentStart);
    const mvcc::StartResult seedStartResult = store.startTransaction();
    const std::uint64_t seedStartResponseNs = nowNs(experimentStart);
    const TransactionId seedTransactionId = seedStartResult.transactionId;

    if (seedTransactionId == 0) {
        std::cerr << "failed to start seed transaction\n";
        return 1;
    }

    std::vector<TransactionId> seedSnapshotActive(
        seedStartResult.snapshot.activeTransactions.begin(),
        seedStartResult.snapshot.activeTransactions.end());
    std::sort(seedSnapshotActive.begin(), seedSnapshotActive.end());

    LogEntry seedStartEntry;
    seedStartEntry.operationType = OperationType::StartTransaction;
    seedStartEntry.threadId = std::numeric_limits<std::size_t>::max();
    seedStartEntry.transactionId = seedTransactionId;
    seedStartEntry.requestTimestampNs = seedStartRequestNs;
    seedStartEntry.responseTimestampNs = seedStartResponseNs;
    seedStartEntry.success = true;
    seedStartEntry.derivedActiveTransactions = std::move(seedSnapshotActive);
    seedStartEntry.derivedMinTransactionId = seedStartResult.snapshot.minTransactionId;
    seedStartEntry.note = "seed transaction start";
    seedLog.push_back(seedStartEntry);

    for (std::uint32_t key = 0; key < kTotalKeys; ++key) {
        const std::uint64_t requestTimestampNs = nowNs(experimentStart);
        const bool success = store.set(seedTransactionId, key, kBaseValue);
        const std::uint64_t responseTimestampNs = nowNs(experimentStart);

        LogEntry seedSetEntry;
        seedSetEntry.operationType = OperationType::Set;
        seedSetEntry.threadId = std::numeric_limits<std::size_t>::max();
        seedSetEntry.transactionId = seedTransactionId;
        seedSetEntry.requestTimestampNs = requestTimestampNs;
        seedSetEntry.responseTimestampNs = responseTimestampNs;
        seedSetEntry.key = key;
        seedSetEntry.requestedValue = kBaseValue;
        seedSetEntry.success = success;
        seedSetEntry.note = "seed base value";
        seedLog.push_back(seedSetEntry);

        if (!success) {
            std::cerr << "failed to seed key " << key << '\n';
            return 1;
        }
    }

    const std::uint64_t seedCommitRequestNs = nowNs(experimentStart);
    const bool seedCommitSuccess = store.commit(seedTransactionId);
    const std::uint64_t seedCommitResponseNs = nowNs(experimentStart);

    LogEntry seedCommitEntry;
    seedCommitEntry.operationType = OperationType::Commit;
    seedCommitEntry.threadId = std::numeric_limits<std::size_t>::max();
    seedCommitEntry.transactionId = seedTransactionId;
    seedCommitEntry.requestTimestampNs = seedCommitRequestNs;
    seedCommitEntry.responseTimestampNs = seedCommitResponseNs;
    seedCommitEntry.success = seedCommitSuccess;
    seedCommitEntry.note = "seed transaction commit";
    seedLog.push_back(seedCommitEntry);

    if (!seedCommitSuccess) {
        std::cerr << "failed to commit seed transaction\n";
        return 1;
    }

    std::vector<std::vector<LogEntry>> threadLogs(kThreadCount);
    std::vector<std::thread> threads;
    threads.reserve(kThreadCount);

    for (std::size_t threadId = 0; threadId < kThreadCount; ++threadId) {
        threads.emplace_back([threadId, &store, &experimentStart, &threadLogs]() {
            runWorker(threadId, store, experimentStart, threadLogs[threadId]);
        });
    }

    for (std::thread& thread : threads) {
        thread.join();
    }

    const std::vector<LogEntry> allEntries = combineLogs(seedLog, threadLogs);
    const auto transactionInfoById = buildTransactionInfo(allEntries);

    std::filesystem::create_directories(logsDir);
    dumpLogFile(logsDir / "seed.log", seedLog);
    for (std::size_t threadId = 0; threadId < threadLogs.size(); ++threadId) {
        dumpLogFile(logsDir / ("thread_" + std::to_string(threadId) + ".log"), threadLogs[threadId]);
    }

    const VerificationStats stats =
        verifyAndSaveTimelines(allEntries, transactionInfoById, savedTimelinesDir);

    std::cout << "experiment_name:\t" << experimentName << '\n';
    std::cout << "total_transactions:\t" << stats.totalTransactions << '\n';
    std::cout << "total_operations:\t" << stats.totalOperations << '\n';
    std::cout << "total_gets:\t" << stats.totalGets << '\n';
    std::cout << "checked_gets:\t" << stats.checkedGets << '\n';
    std::cout << "mismatches:\t" << stats.mismatches << '\n';
    std::cout << "logs_dir:\t" << logsDir << '\n';
    if (stats.mismatches > 0) {
        std::cout << "saved_timelines_dir:\t" << savedTimelinesDir << '\n';
    }

    return stats.mismatches == 0 ? 0 : 1;
}
