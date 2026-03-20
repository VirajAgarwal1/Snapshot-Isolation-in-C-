## What this test is trying to do

This test is meant to seriously stress the MVCC with many threads at once.

I start with all keys already inserted through normal MVCC flow. After that, many threads start doing random transactions. Each transaction works only on one continuous band of keys, and inside that band it will randomly do reads, updates, and deletes.

I am keeping all the raw logs in memory while the run is going on. This is better because writing to files during the run would slow things down and would also mess with the timing too much. Once the run finishes, I dump the logs to readable files.

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

## Logging

Each thread gets its own log file.

The logs are written in a human-readable way first. I am not trying to optimize them for machine parsing. So I keep them easy to read with tabs, labels, and spacing.

I also keep a separate seed log because the initial base values are written before the worker threads start.

## About the verification logic

I derive the transaction windows from the logs:
- when a txn started
- when it finished
- whether it committed or aborted

From that, I build a per-key timeline and check the GET results against the writes that should be visible.

This means the checker is based on what I can observe from outside MVCC. So it is strong, but it is still based on logged timing windows and not private internal state.

If a transaction start window overlaps other transaction start or close events too tightly, then I treat that snapshot as ambiguous from outside and skip checking those GETs instead of claiming a false mismatch.

So if you see many `skipped_gets`, that means the run had many overlapping/ambiguous start windows. It does **not** automatically mean MVCC is wrong.

## Saved timelines

If I find something suspicious, I save a detailed timeline for that key inside `saved_timelines/<experiment_name>/`.

That file is supposed to be readable by a person. It shows the key timeline and clearly marks which GETs looked wrong.

## Time naming

The experiment name uses IST so the log folder names are in my local expected time and not in UTC/GMT.
