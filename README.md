# azure-delta-lake

Shared, in-process Python library for existing Azure Blob Storage Parquet files and Delta tables.

## Install

```bash
pip install -e '.[test]'
```

## Configuration

```text
DATA_LAKE_ACCOUNT=<storage account>
DATA_LAKE_CONTAINER=<container>
DATA_LAKE_PREFIX=<optional common folder>
AZURE_STORAGE_ACCOUNT_KEY=<secret; required for Delta in this version>
```

Standalone Parquet supports DefaultAzureCredential if the account key is omitted. The principal must have blob read/write/list/lease permissions. For Delta tables, this release uses an account key for delta-rs; keep it in a managed secret store. Do not embed secrets in code.

## Usage

```python
from azure_delta_lake import DataLake
with DataLake.from_env() as lake:
    lake.upsert("quotes/symbol=NSE%3ASBIN-EQ/year=2026/month=10/day=08/part-0.parquet", df, keys=["symbol", "source", "fetched_at"])
    rows = lake.read("quotes/symbol=NSE%3ASBIN-EQ/year=2026/month=10/day=08/part-0.parquet", missing_ok=True)
```

## Concurrency contract

- All writers MUST use this library, the same account/container/prefix, and the same logical path. The lease is on a *separate* lock blob, so direct Blob SDK writers can bypass it.
- Standalone Parquet: renewable cooperative lock + ETag conditional replacement. A lock holder reads the current blob and merges records. Readers perform ETag-conditional downloads without locking.
- Delta: cooperative per-table lock + Delta transaction log. Verify atomic commit support of your installed delta-rs version/backend in integration tests. This library does not make direct, uncoordinated Delta writers safe.
- If the lease is lost during an in-progress remote commit, the result can be ambiguous. This version does not claim exactly-once guarantees. Use deterministic business keys, verify outcomes, and do not blindly retry appends.
- Multi-blob operations are not atomic. A single Parquet blob append rewrites the whole blob. Delta table creation, merge and append require backend integration tests before production.
- Delta `upsert` uses `MERGE`; table schema must already be compatible. New Delta tables can be created with `partition_by`. Standalone Parquet partition paths are constructed by the consuming app.
- Generic OHLCV resampling is calendar-agnostic. It retains partial bars; trading session and holiday filtering belong to the caller.

## Testing

```bash
pytest -q
python -m build
```

`tests/test_frames.py` tests pure transformations. Run Azure integration tests with two separate processes against a disposable container before production; these are not included.

## Automated tests
See [TESTING.md](TESTING.md) for Windows/Linux instructions and opt-in real Azure concurrency tests.
