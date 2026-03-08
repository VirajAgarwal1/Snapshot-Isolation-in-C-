This naive storage engine is intentionally simple and in-memory only.

1. Data is kept in a hash map keyed by `uint32_t`.
2. Each key maps to an array-like list (`std::vector`) of rows.
3. Each row stores:
	- `key` (`uint32_t`)
	- `value` (`int64_t`)
	- `timestamp` (engine-generated full timestamp)
	- `metadata` (caller-provided string)
4. `set(key, value, metadata)` appends a new row for that key.
5. `get(key)` returns all rows for that key.
6. `clear()` removes everything from memory.
7. Delete by key/version is not supported.
8. No mutex is used internally; this implementation is single-threaded and not thread-safe.

Limits (easy to change):
- `maxUniqueKeys` (default: `10`)
- `maxValuesPerKey` (default: `100`)

These are configured via the `Limits` struct passed to `NaiveStorageEngine`:

```cpp
naive_storage_engine::Limits limits;
limits.maxUniqueKeys = 10;
limits.maxValuesPerKey = 100;

naive_storage_engine::NaiveStorageEngine engine(limits);
```