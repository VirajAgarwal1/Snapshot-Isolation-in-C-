# MVCC With Snapshot Isolation In C++

This repository contains:

1. A TCP server + thread-pool demo (`src/server`).
2. An in-memory key/version storage engine (`src/naive_storage_engine`).
3. An MVCC layer implementing Snapshot Isolation (`src/mvcc`).
4. A concurrent system stress test that validates MVCC GET visibility (`system_test`).

## Project Layout

- `src/include/naive_storage_engine`: storage engine public headers
- `src/include/mvcc`: MVCC public headers
- `src/include/util`: shared helpers (including MVCC metadata codec)
- `src/naive_storage_engine`: storage engine implementation
- `src/mvcc`: MVCC implementation
- `test`: unit tests (`thread_pool_test`, `naive_storage_engine_test`, `mvcc_test`)
- `system_test`: multi-threaded MVCC stress + verifier

## Build

```bash
mkdir -p build
cd build
cmake ..
make -j
```

## Run Binaries

TCP server:

```bash
./src/tcp_server
```

MVCC system stress test:

```bash
./system_test/mvcc_system_test
```

## Run Unit Tests

```bash
ctest --output-on-failure
```

Current unit-test executables:

- `thread_pool_test`
- `naive_storage_engine_test`
- `mvcc_test`

## MVCC Snapshot Isolation (Current Behavior)

- `startTransaction()` returns `mvcc::StartResult`:
	- `transactionId`
	- `snapshot` (`activeTransactions`, `minTransactionId`)
- `get(txn, key)` reads the latest version visible in that transaction snapshot.
- `set(txn, key, value)` and `deleteKey(txn, key)` enforce first-committer style write/write conflict checks using snapshot horizon.
- `commit(txn)` moves txn from active to committed and clears its snapshot entry.
- `abort(txn)` removes txn from active and clears its snapshot entry.
- `vacuum()` computes DB horizon from active snapshots and compacts old row versions safely.

MVCC metadata is encoded as `txnId|isDead`.

## System Stress Test Notes

- Worker threads run randomized transactions over overlapping key bands.
- Logs are captured in-memory during execution and written after completion.
- Verifier reconstructs transaction state and checks each successful GET against expected visible writes.
- Snapshot validation uses the exact snapshot returned by `startTransaction()` (not inferred timing windows), so there is no ambiguous-window skip path.

Outputs:

- Human-readable logs: `system_test/logs/<experiment_name>/`
- Mismatch timelines (only when mismatches exist): `system_test/saved_timelines/<experiment_name>/`

## Quick `nc` Example (TCP Server)

Terminal 1:

```bash
cd build
./src/tcp_server
```

Terminal 2:

```bash
nc 127.0.0.1 8080
hello from netcat
```

Expected response:

```text
OK
```