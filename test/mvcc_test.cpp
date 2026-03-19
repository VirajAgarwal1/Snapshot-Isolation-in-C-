#include "mvcc/mvcc.hpp"

#include <cstdint>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

#include "util/mvcc_metadata_codec.hpp"

namespace {

bool expectFound(const mvcc::GetResult& result, std::int64_t expectedValue,
                 const std::string& context) {
    if (result.status != mvcc::GetStatus::Found) {
        std::cerr << context << ": expected Found status\n";
        return false;
    }

    if (!result.value.has_value() || result.value.value() != expectedValue) {
        std::cerr << context << ": expected value did not match\n";
        return false;
    }

    return true;
}

bool expectDeleted(const mvcc::GetResult& result, const std::string& context) {
    if (result.status != mvcc::GetStatus::Deleted) {
        std::cerr << context << ": expected Deleted status\n";
        return false;
    }

    if (result.value.has_value()) {
        std::cerr << context << ": deleted result should not carry a value\n";
        return false;
    }

    return true;
}

bool expectNotFound(const mvcc::GetResult& result, const std::string& context) {
    if (result.status != mvcc::GetStatus::NotFound) {
        std::cerr << context << ": expected NotFound status\n";
        return false;
    }

    if (result.value.has_value()) {
        std::cerr << context << ": not-found result should not carry a value\n";
        return false;
    }

    return true;
}

bool runMetadataCodecTest() {
    const util::MVCCMetadata metadata{42, true};
    const std::string encoded = util::encodeMVCCMetadata(metadata);
    if (encoded != "42|1") {
        std::cerr << "encodeMVCCMetadata() produced unexpected output\n";
        return false;
    }

    const auto decoded = util::decodeMVCCMetadata(encoded);
    if (!decoded.has_value()) {
        std::cerr << "decodeMVCCMetadata() failed for valid metadata\n";
        return false;
    }

    if (decoded->transactionId != 42 || !decoded->isDead) {
        std::cerr << "decodeMVCCMetadata() returned wrong field values\n";
        return false;
    }

    const util::MVCCMetadata maxMetadata{std::numeric_limits<std::uint64_t>::max(), false};
    const auto maxRoundTrip = util::decodeMVCCMetadata(util::encodeMVCCMetadata(maxMetadata));
    if (!maxRoundTrip.has_value() || maxRoundTrip->transactionId != maxMetadata.transactionId ||
        maxRoundTrip->isDead != maxMetadata.isDead) {
        std::cerr << "metadata codec max boundary round-trip failed\n";
        return false;
    }

    const std::vector<std::string> invalidInputs = {
        "", "42", "42|", "|1", "abc|1", "42|2", "42|10", "42||1", " 42|1", "42|1 "};

    for (const std::string& invalid : invalidInputs) {
        if (util::decodeMVCCMetadata(invalid).has_value()) {
            std::cerr << "decodeMVCCMetadata() accepted invalid input: " << invalid << "\n";
            return false;
        }
    }

    return true;
}

bool runSnapshotIsolationTest() {
    naive_storage_engine::Limits limits;
    limits.maxUniqueKeys = 4;
    limits.maxValuesPerKey = 64;

    mvcc::MVCC store(limits);

    const mvcc::TransactionId seedTxn = store.startTransaction();
    if (!store.set(seedTxn, 0U, 100) || !store.commit(seedTxn)) {
        std::cerr << "seed transaction failed in snapshot isolation test\n";
        return false;
    }

    const mvcc::TransactionId readerTxn = store.startTransaction();
    const mvcc::TransactionId writerTxn = store.startTransaction();

    if (!store.set(writerTxn, 0U, 200) || !store.commit(writerTxn)) {
        std::cerr << "writer transaction failed in snapshot isolation test\n";
        return false;
    }

    if (!expectFound(store.get(readerTxn, 0U), 100,
                     "reader should keep snapshot of older committed value")) {
        return false;
    }

    if (!store.commit(readerTxn)) {
        std::cerr << "reader commit failed in snapshot isolation test\n";
        return false;
    }

    const mvcc::TransactionId freshTxn = store.startTransaction();
    if (!expectFound(store.get(freshTxn, 0U), 200,
                     "fresh transaction should see latest committed value")) {
        return false;
    }

    if (!store.commit(freshTxn)) {
        std::cerr << "fresh transaction commit failed in snapshot isolation test\n";
        return false;
    }

    return true;
}

bool runDeleteAndNotFoundTest() {
    naive_storage_engine::Limits limits;
    limits.maxUniqueKeys = 4;
    limits.maxValuesPerKey = 64;

    mvcc::MVCC store(limits);

    const mvcc::TransactionId createTxn = store.startTransaction();
    if (!store.set(createTxn, 1U, 55) || !store.commit(createTxn)) {
        std::cerr << "create transaction failed in delete test\n";
        return false;
    }

    const mvcc::TransactionId deleteTxn = store.startTransaction();
    if (!store.deleteKey(deleteTxn, 1U) || !store.commit(deleteTxn)) {
        std::cerr << "delete transaction failed in delete test\n";
        return false;
    }

    const mvcc::TransactionId readTxn = store.startTransaction();
    if (!expectDeleted(store.get(readTxn, 1U), "deleted key should report Deleted status")) {
        return false;
    }

    if (!expectNotFound(store.get(readTxn, 2U), "never-written key should report NotFound")) {
        return false;
    }

    if (!store.commit(readTxn)) {
        std::cerr << "read transaction commit failed in delete test\n";
        return false;
    }

    return true;
}

bool runConflictAutoAbortTest() {
    naive_storage_engine::Limits limits;
    limits.maxUniqueKeys = 4;
    limits.maxValuesPerKey = 64;

    mvcc::MVCC store(limits);

    const mvcc::TransactionId firstTxn = store.startTransaction();
    const mvcc::TransactionId conflictingTxn = store.startTransaction();

    if (!store.set(firstTxn, 0U, 10) || !store.commit(firstTxn)) {
        std::cerr << "first transaction failed in conflict test\n";
        return false;
    }

    if (store.set(conflictingTxn, 0U, 20)) {
        std::cerr << "set() should fail on write-write conflict\n";
        return false;
    }

    if (store.commit(conflictingTxn)) {
        std::cerr << "commit() should fail after auto-abort on conflict\n";
        return false;
    }

    const mvcc::TransactionId verifyTxn = store.startTransaction();
    if (!expectFound(store.get(verifyTxn, 0U), 10, "conflict should not overwrite existing value")) {
        return false;
    }

    if (!store.commit(verifyTxn)) {
        std::cerr << "verify transaction commit failed in conflict test\n";
        return false;
    }

    return true;
}

bool runAbortVisibilityTest() {
    naive_storage_engine::Limits limits;
    limits.maxUniqueKeys = 4;
    limits.maxValuesPerKey = 64;

    mvcc::MVCC store(limits);

    const mvcc::TransactionId writeTxn = store.startTransaction();
    if (!store.set(writeTxn, 0U, 900)) {
        std::cerr << "set() failed in abort visibility test\n";
        return false;
    }

    if (!store.abort(writeTxn)) {
        std::cerr << "abort() failed in abort visibility test\n";
        return false;
    }

    const mvcc::TransactionId readerTxn = store.startTransaction();
    if (!expectNotFound(store.get(readerTxn, 0U), "aborted write should not become visible")) {
        return false;
    }

    if (!store.commit(readerTxn)) {
        std::cerr << "reader commit failed in abort visibility test\n";
        return false;
    }

    return true;
}

bool runVacuumPreservesVisibilityTest() {
    naive_storage_engine::Limits limits;
    limits.maxUniqueKeys = 2;
    limits.maxValuesPerKey = 128;

    mvcc::MVCC store(limits);

    const mvcc::TransactionId txn1 = store.startTransaction();
    if (!store.set(txn1, 0U, 7) || !store.commit(txn1)) {
        std::cerr << "txn1 failed in vacuum test\n";
        return false;
    }

    const mvcc::TransactionId txn2 = store.startTransaction();
    if (!store.set(txn2, 0U, 9) || !store.commit(txn2)) {
        std::cerr << "txn2 failed in vacuum test\n";
        return false;
    }

    if (!store.vacuum()) {
        std::cerr << "vacuum() failed after two committed writes\n";
        return false;
    }

    const mvcc::TransactionId postVacuumReadTxn = store.startTransaction();
    if (!expectFound(store.get(postVacuumReadTxn, 0U), 9,
                     "post-vacuum read should still see latest value")) {
        return false;
    }

    if (!store.commit(postVacuumReadTxn)) {
        std::cerr << "post-vacuum read commit failed\n";
        return false;
    }

    const mvcc::TransactionId tombstoneTxn = store.startTransaction();
    if (!store.deleteKey(tombstoneTxn, 0U) || !store.commit(tombstoneTxn)) {
        std::cerr << "tombstone transaction failed in vacuum test\n";
        return false;
    }

    if (!store.vacuum()) {
        std::cerr << "vacuum() failed after tombstone commit\n";
        return false;
    }

    const mvcc::TransactionId deletedReadTxn = store.startTransaction();
    if (!expectDeleted(store.get(deletedReadTxn, 0U),
                       "post-vacuum read should preserve tombstone visibility")) {
        return false;
    }

    if (!store.commit(deletedReadTxn)) {
        std::cerr << "deleted read transaction commit failed\n";
        return false;
    }

    return true;
}

}  // namespace

int main() {
    const bool ok = runMetadataCodecTest() && runSnapshotIsolationTest() && runDeleteAndNotFoundTest() &&
                    runConflictAutoAbortTest() && runAbortVisibilityTest() &&
                    runVacuumPreservesVisibilityTest();
    return ok ? 0 : 1;
}
