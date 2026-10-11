"""Azure-backed Parquet files and Delta tables.

All writers to a given path MUST use this library and its lock naming convention.
Delta transactions require a compatible atomic commit implementation in the engine.
"""
from __future__ import annotations
import io
import os
import posixpath
import logging
import time
from uuid import uuid4
from enum import Enum
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from azure.core import MatchConditions
from azure.core.exceptions import ResourceNotFoundError, ResourceModifiedError, ServiceRequestError, ServiceResponseError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError
from .frames import merge_frames, filter_frame
from .locking import BlobLock, LockLost

logger = logging.getLogger(__name__)

class WriteOutcomeUnknown(RuntimeError):
    """A commit may have succeeded; verify business keys before retrying."""

class SchemaMismatch(ValueError):
    """A write differs from its explicitly supplied Arrow schema contract."""


class StorageFormat(str, Enum):
    PARQUET = "parquet"
    DELTA = "delta"

class WriteMode(str, Enum):
    OVERWRITE = "overwrite"
    APPEND = "append"

class DataLake:
    def __init__(self, account_name: str, container: str, *, prefix: str = "", account_key: str | None = None, lease_seconds: int = 60, lock_wait_seconds: float = 30, read_retries: int = 3, read_retry_delay: float = 0.05, require_append_keys: bool = False, app_id: str = ""):
        self.account_name = account_name
        self.container_name = container
        self.prefix = prefix.strip("/")
        if self.prefix and any(p in ("", ".", "..") for p in self.prefix.split("/")):
            raise ValueError("prefix must be a canonical relative path")
        if "\\" in self.prefix:
            raise ValueError("prefix must use forward slashes")
        if read_retries < 0 or read_retry_delay < 0:
            raise ValueError("read retry settings cannot be negative")
        self.read_retries = read_retries
        self.read_retry_delay = read_retry_delay
        self.require_append_keys = require_append_keys
        self.app_id = app_id
        self.account_key = account_key
        self._credential = None if account_key else DefaultAzureCredential()
        self._service = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net", credential=account_key or self._credential)
        self._container = self._service.get_container_client(container)
        self.lease_seconds = lease_seconds
        self.lock_wait_seconds = lock_wait_seconds

    @classmethod
    def from_env(cls):
        return cls(account_name=os.environ["DATA_LAKE_ACCOUNT"], container=os.environ["DATA_LAKE_CONTAINER"], prefix=os.getenv("DATA_LAKE_PREFIX", ""), account_key=os.getenv("AZURE_STORAGE_ACCOUNT_KEY"))

    def _path(self, path: str) -> str:
        parts = path.split("/")
        if "\\" in path or not path or path.startswith("/") or any(p in ("", ".", "..") for p in parts):
            raise ValueError(f"invalid relative storage path: {path!r}")
        return posixpath.join(self.prefix, path) if self.prefix else path

    def _lock(self, full_path: str) -> BlobLock:
        # Identical path -> identical lock across all cooperating apps.
        return BlobLock(self._container, f"_adl_locks/{full_path}.lock", lease_seconds=self.lease_seconds, wait_seconds=self.lock_wait_seconds)

    def _delta_options(self):
        # Explicitly use key auth for Delta Rust Azure backend in this release.
        # DefaultAzureCredential supports standalone Parquet, not Delta here.
        if not self.account_key:
            raise RuntimeError("Delta access requires AZURE_STORAGE_ACCOUNT_KEY in this version; use a secret store, never commit the key")
        return {"AZURE_STORAGE_ACCOUNT_NAME": self.account_name, "AZURE_STORAGE_ACCOUNT_KEY": self.account_key}

    def _delta_uri(self, full_path: str) -> str:
        return f"az://{self.container_name}/{full_path}"

    def _read_parquet_blob(self, full_path: str, *, missing_ok: bool) -> pd.DataFrame:
        blob = self._container.get_blob_client(full_path)
        for attempt in range(getattr(self, "read_retries", 3) + 1):
            try:
                # Restart BOTH properties and download after an ETag conflict.
                props = blob.get_blob_properties()
                content = blob.download_blob(etag=props.etag, match_condition=MatchConditions.IfNotModified).readall()
                return pq.read_table(io.BytesIO(content)).to_pandas()
            except ResourceNotFoundError:
                if missing_ok:
                    return pd.DataFrame()
                raise
            except ResourceModifiedError:
                if attempt >= getattr(self, "read_retries", 3):
                    raise
                logger.info("data_lake read_conflict container=%s path=%s attempt=%s", self.container_name, full_path, attempt + 1)
                time.sleep(getattr(self, "read_retry_delay", 0.05) * (attempt + 1))

    def read(self, path: str, *, format: StorageFormat | str = StorageFormat.PARQUET, missing_ok: bool = False, filters: dict | None = None, timestamp_col: str | None = None, start=None, end=None, columns: list[str] | None = None, timestamp_unit: str | None = None) -> pd.DataFrame:
        full = self._path(path)
        fmt = StorageFormat(format)
        if fmt is StorageFormat.PARQUET:
            df = self._read_parquet_blob(full, missing_ok=missing_ok)
        else:
            try:
                table = DeltaTable(self._delta_uri(full), storage_options=self._delta_options())
            except TableNotFoundError:
                # Listing errors propagate; an existing log means corruption,
                # protocol incompatibility, or another failure, not absence.
                if not missing_ok or self._delta_log_exists(full):
                    raise
                return pd.DataFrame()
            needed = None if columns is None else list(dict.fromkeys(columns + list((filters or {}).keys()) + ([timestamp_col] if timestamp_col else [])))
            arrow_filters = [(key, "in", list(value)) if isinstance(value, (list, tuple, set)) else (key, "=", value) for key, value in (filters or {}).items()]
            df = table.to_pyarrow_table(columns=needed, filters=arrow_filters or None).to_pandas()
        result = filter_frame(df, filters, timestamp_col=timestamp_col, start=start, end=end, timestamp_unit=timestamp_unit)
        return result if columns is None else result.loc[:, columns]

    @staticmethod
    def _validate_schema(data: pd.DataFrame, schema: pa.Schema | None) -> None:
        if schema is None:
            return
        actual = pa.Table.from_pandas(data, preserve_index=False).schema
        if actual.names != schema.names or any(a.type != b.type for a, b in zip(actual, schema)):
            raise SchemaMismatch(f"incoming schema differs from contract: {actual} != {schema}")
        for field in schema:
            if not field.nullable and data[field.name].isna().any():
                raise SchemaMismatch(f"non-nullable field contains nulls: {field.name}")

    @staticmethod
    def _validate_existing_schema(actual: pa.Schema, schema: pa.Schema | None) -> None:
        if schema is not None and (actual.names != schema.names or any(a.type != b.type for a, b in zip(actual, schema))):
            raise SchemaMismatch("stored schema differs from supplied contract")

    @staticmethod
    def _commit(action, lost, full: str):
        if lost.is_set():
            raise LockLost("Lost write lease before commit")
        try:
            result = action()
        except (ServiceRequestError, ServiceResponseError, TimeoutError, ConnectionError) as exc:
            raise WriteOutcomeUnknown(f"Commit outcome unknown for {full}; verify before retrying") from exc
        if lost.is_set():
            raise WriteOutcomeUnknown(f"Lease lost during commit for {full}; verify before retrying")
        return result

    def write(self, path: str, data: pd.DataFrame, *, format: StorageFormat | str = StorageFormat.PARQUET, mode: WriteMode | str = WriteMode.OVERWRITE, keys: list[str] | None = None, sort_by: str | None = None, partition_by: list[str] | None = None, revision_col: str | None = None, schema: pa.Schema | None = None) -> None:
        if not isinstance(data, pd.DataFrame):
            raise TypeError("data must be a pandas DataFrame")
        fmt, write_mode = StorageFormat(format), WriteMode(mode)
        full = self._path(path)
        self._validate_schema(data, schema)
        if revision_col and (not keys or write_mode is not WriteMode.APPEND):
            raise ValueError("revision_col requires keyed upsert")
        if revision_col and (revision_col not in data or data[revision_col].isna().any()):
            raise ValueError("revision column must exist and contain no nulls")
        if getattr(self, "require_append_keys", False) and write_mode is WriteMode.APPEND and not keys:
            raise ValueError("append requires keys under require_append_keys; plain append cannot be retried safely")
        operation_id = uuid4().hex
        started = time.monotonic()
        logger.info("data_lake write_start operation_id=%s app=%s container=%s path=%s format=%s mode=%s rows=%s", operation_id, getattr(self, "app_id", ""), self.container_name, full, fmt.value, write_mode.value, len(data))
        try:
            self._write_selected(full, data, fmt, write_mode, keys, sort_by, partition_by, revision_col, schema)
        except Exception as exc:
            logger.warning("data_lake write_failed operation_id=%s error_type=%s duration_ms=%.1f", operation_id, type(exc).__name__, (time.monotonic() - started) * 1000)
            raise
        logger.info("data_lake write_complete operation_id=%s duration_ms=%.1f", operation_id, (time.monotonic() - started) * 1000)

    def _write_selected(self, full, data, fmt, write_mode, keys, sort_by, partition_by, revision_col, schema):
        if fmt is StorageFormat.DELTA:
            self._write_delta(full, data, write_mode, keys, partition_by, revision_col, schema)
        else:
            if partition_by:
                raise ValueError("partition_by is for Delta tables; use explicit paths for standalone Parquet")
            self._write_parquet(full, data, write_mode, keys, sort_by, revision_col, schema)

    def upsert(self, path: str, data: pd.DataFrame, *, keys: list[str], format: StorageFormat | str = StorageFormat.PARQUET, sort_by: str | None = None, partition_by: list[str] | None = None, revision_col: str | None = None, schema: pa.Schema | None = None) -> None:
        if not keys:
            raise ValueError("keys required")
        self.write(path, data, format=format, mode=WriteMode.APPEND, keys=keys, sort_by=sort_by, partition_by=partition_by, revision_col=revision_col, schema=schema)

    def _write_parquet(self, full: str, data: pd.DataFrame, mode: WriteMode, keys: list[str] | None, sort_by: str | None, revision_col: str | None = None, schema: pa.Schema | None = None):
        if data.empty:
            return
        with self._lock(full).acquire() as lost:
            blob = self._container.get_blob_client(full)
            try:
                props = blob.get_blob_properties()
                etag = props.etag
                current = self._read_parquet_blob(full, missing_ok=False) if mode is WriteMode.APPEND or schema is not None else pd.DataFrame()
                if schema is not None:
                    self._validate_existing_schema(pa.Table.from_pandas(current, preserve_index=False).schema, schema)
            except ResourceNotFoundError:
                etag = None
                current = pd.DataFrame()
            if mode is WriteMode.APPEND:
                result = merge_frames(current, data, keys, sort_by, revision_col) if keys else pd.concat([current, data], ignore_index=True)
            else:
                result = data.copy()
            out = io.BytesIO()
            pq.write_table(pa.Table.from_pandas(result, schema=schema, preserve_index=False), out, compression="zstd")
            if lost.is_set():
                raise LockLost("Lost write lease before commit")
            if etag is None:
                self._commit(lambda: blob.upload_blob(out.getvalue(), overwrite=False), lost, full)
            else:
                self._commit(lambda: blob.upload_blob(out.getvalue(), overwrite=True, etag=etag, match_condition=MatchConditions.IfNotModified), lost, full)

    def _write_delta(self, full: str, data: pd.DataFrame, mode: WriteMode, keys: list[str] | None, partition_by: list[str] | None, revision_col: str | None = None, schema: pa.Schema | None = None):
        if data.empty:
            return
        uri = self._delta_uri(full)
        opts = self._delta_options()
        with self._lock(full).acquire() as lost:
            if keys and mode is WriteMode.APPEND:
                if any(k not in data.columns for k in keys):
                    raise ValueError("upsert key absent from incoming data")
                if data[keys].isna().any().any():
                    raise ValueError("upsert keys must not be null")
                incoming = merge_frames(pd.DataFrame(), data, keys, revision_col=revision_col)
                try:
                    table = DeltaTable(uri, storage_options=opts)
                except TableNotFoundError:
                    # Only treat an absent _delta_log as a missing table.
                    if not self._delta_log_exists(full):
                        if lost.is_set():
                            raise LockLost("Lost lease before table creation")
                        self._commit(lambda: write_deltalake(uri, pa.Table.from_pandas(incoming, schema=schema, preserve_index=False), mode="error", partition_by=partition_by, storage_options=opts), lost, full)
                        return
                    raise
                self._validate_existing_schema(pa.schema(table.schema().to_arrow()), schema)
                if revision_col and revision_col not in pa.schema(table.schema().to_arrow()).names:
                    raise ValueError("stored table lacks revision column")
                source = pa.Table.from_pandas(incoming, schema=schema, preserve_index=False)
                predicate = " AND ".join(f'target."{k.replace(chr(34), chr(34) * 2)}" = source."{k.replace(chr(34), chr(34) * 2)}"' for k in keys)
                if lost.is_set():
                    raise LockLost("Lost lease before Delta merge")
                revision = revision_col.replace('"', '""') if revision_col else None
                condition = f'source."{revision}" >= target."{revision}"' if revision else None
                merger = table.merge(source=source, predicate=predicate, source_alias="source", target_alias="target")
                merger = merger.when_matched_update_all(predicate=condition).when_not_matched_insert_all()
                self._commit(merger.execute, lost, full)
            else:
                if lost.is_set():
                    raise LockLost("Lost lease before Delta commit")
                if schema is not None and self._delta_log_exists(full):
                    self._validate_existing_schema(pa.schema(DeltaTable(uri, storage_options=opts).schema().to_arrow()), schema)
                self._commit(lambda: write_deltalake(uri, pa.Table.from_pandas(data, schema=schema, preserve_index=False), mode=mode.value, partition_by=partition_by if mode is WriteMode.OVERWRITE or not self._delta_log_exists(full) else None, storage_options=opts), lost, full)

    def _delta_log_exists(self, full: str) -> bool:
        return any(True for _ in self._container.list_blobs(name_starts_with=f"{full}/_delta_log/", results_per_page=1))

    def close(self):
        self._service.close()
        if self._credential:
            self._credential.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

