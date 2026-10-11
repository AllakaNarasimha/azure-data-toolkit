# Shared storage contract

All applications writing the same destination must use the same canonical container, prefix, path and `_adl_locks` naming convention. Readers need read-only credentials where possible. Direct SDK uploads, other engines and manual edits can bypass this cooperative lock. Azure access controls and service ownership must enforce the agreed writer policy.

## Opt-in safeguards

```python
import pyarrow as pa
from azure_data_toolkit import DataLake

lake = DataLake(account_name=account, container=container, account_key=secret,
                require_append_keys=True, app_id="quotes-service")
contract = pa.schema([("id", pa.int64()), ("revision", pa.int64()), ("price", pa.float64())])
lake.upsert("quotes", incoming, format="delta", keys=["id"],
            revision_col="revision", schema=contract)
```

The highest revision wins per business key, including duplicate incoming keys. Revisions must be non-null, mutually comparable and globally meaningful across writers. Equal revisions retain last-writer-wins semantics: use identical payloads for equal revisions, or establish a stronger upstream ordering contract. Existing tables need a revision column before enabling this option. Overwrite is a full replacement and does not provide stale-update protection.

Repeating the same keyed upsert avoids duplicate business keys. This is not a durable exactly-once ingestion protocol. Plain append can duplicate rows after retries; `require_append_keys=True` rejects it. Logged operation IDs correlate individual calls; they are not persisted deduplication receipts.

An optional Arrow schema contract checks incoming names, order and types and existing stored names/order/types. Incoming non-nullable fields reject nulls. All writers must supply the same contract; it is not a centralized schema registry. Nullable metadata from earlier versions does not itself fail a compatible contract. Schema evolution requires a coordinated migration.

`LockLost` before a commit means this call did not begin its commit. `WriteOutcomeUnknown` means a transport failure or observed lease loss during a commit leaves the result uncertain. Verify destination business keys/revisions before retrying. Delta-engine exceptions propagate with their original details; an engine/network error can also leave an uncertain outcome. Lease checks are not complete fencing against an in-flight writer whose lease expires. There are no cross-table transactions.

Standalone Parquet reads retry ETag conflicts and interrupted transport a bounded number of times, refreshing properties and download together. Readers can observe an earlier or later complete blob. Sustained write contention may still exhaust retries. Delta reads use transaction snapshots; `missing_ok` suppresses only an absent table after checking that no log exists. Existing log corruption, authorization and connectivity errors must remain visible.

Delta reads accept `columns` and equality/membership `filters` for Arrow scan projection/filtering. Time range filtering is performed afterward. Standalone Parquet still downloads and decodes the full blob; keyed upsert rewrites it. Numeric timestamps in range filtering require `timestamp_unit="s"`, `"ms"`, `"us"` or `"ns"` to avoid silently assuming nanoseconds. Paths/prefixes must be canonical and use forward slashes.

Logging emits app ID, destination, mode, row count, operation ID, duration and exception type; it does not log credentials or row payloads. Configure application log handlers and alerts. Pin each application's tested dependencies and coordinate Delta protocol/feature upgrades across engines. This release still requires account-key authentication for Delta.

## Verification

Offline tests use temporary local Delta tables and Azure transport doubles:

```bash
PYTHONPATH=src python -m pytest tests -q
```

Live tests write/delete only a unique run prefix inside the explicitly selected test container. They also remove that run's lock blobs. Use disposable test credentials; never production credentials/container:

```bash
# Set DATA_LAKE_ACCOUNT and AZURE_STORAGE_ACCOUNT_KEY securely.
export TEST_MODE=true
export TEST_MODE_CACHE_BLOB=test-mode-data
export DATA_LAKE_CONTAINER=test-mode-data
PYTHONPATH=src python -m pytest tests --run-azure -q
```

The container must already exist and differ from `MARKET_DATA_CACHE_BLOB` (default `market-data-cache`). Opted-in runs fail when required configuration is missing or unsafe. Live cases include independent spawned processes, concurrent readers/writers for both formats, stale updates/repeated upserts, and recovery after process death with a 15-second lease. These tests demonstrate specific scenarios, not safety against arbitrary external writes or all Azure outages. A killed test runner may leave its own run prefix behind; clean only confirmed abandoned test prefixes.
