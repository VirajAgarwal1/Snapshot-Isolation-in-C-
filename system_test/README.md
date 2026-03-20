## What this test is trying to do

This test is meant to seriously stress the MVCC with many threads at once.

I start with all keys already inserted through normal MVCC flow. After that, many threads start doing random transactions. Each transaction works only on one continuous band of keys, and inside that band it will randomly do reads, updates, and deletes.

I keep all raw logs in memory while the run is going on. This avoids run-time file I/O noise in the hot path. Once the run finishes, logs are dumped to readable files.

## Build and run

From repo root:

```bash
mkdir -p build
cd build
cmake ..
make -j
./system_test/mvcc_system_test
```

## What each thread does

Each thread:
- starts a random number of transactions
- each transaction does a random number of operations
- each operation picks a random key from that transaction's chosen key band
- the transaction usually commits, but sometimes aborts
- if an operation fails, I treat that transaction as aborted and move on to the next one

The percentages and all other numbers are not hidden deep in the code. They are all kept at the top of the test file so they are easy to change.

Current defaults in `mvcc_system_test.cpp`:

- `kThreadCount = 10`
- `kTotalKeys = 500`
- key-band width = `40%` of key-space per transaction
- operations mix: read `60%`, set `35%`, delete `5%`
- commit probability: `95%` (else explicit abort)

## Logging

Each thread gets its own log file.

The logs are written in a human-readable way first. I am not trying to optimize them for machine parsing. So I keep them easy to read with tabs, labels, and spacing.

I also keep a separate seed log because the initial base values are written before the worker threads start.

## About the verification logic

The verifier uses the real snapshot captured by MVCC at `startTransaction()` time.

Each `START_TXN` log entry includes:

- `snapshot_min_txn_id`
- `snapshot_active_txns`

Using this exact snapshot data, the checker computes visibility deterministically and validates each successful GET against the expected latest visible write on that key timeline.

This removes timing-window approximation from snapshot reconstruction and removes the need for an ambiguity/skip path.

## Saved timelines

If I find something suspicious, I save a detailed timeline for that key inside `saved_timelines/<experiment_name>/`.

That file is supposed to be readable by a person. It shows the key timeline and clearly marks which GETs looked wrong.

## Output summary

At the end, the test prints:

- `total_transactions`
- `total_operations`
- `total_gets`
- `checked_gets`
- `mismatches`
- `logs_dir`

If mismatches are present, `saved_timelines_dir` is also printed.

## Time naming

The experiment name uses IST so the log folder names are in my local expected time and not in UTC/GMT.
