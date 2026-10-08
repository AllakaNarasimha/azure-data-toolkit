"""Azure-backed Parquet files and Delta tables.

All writers to a given path MUST use this library and its lock naming convention.
Delta transactions require a compatible atomic commit implementation in the engine.
"""
from __future__ import annotations
import io
import os
import posixpath
from enum import Enum
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from azure.core import MatchConditions
from azure.core.exceptions import ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from deltalake import DeltaTable, write_deltalake
from .frames import merge_frames, filter_frame
from .locking import BlobLock, LockLost

class StorageFormat(str, Enum):
    PARQUET = "parquet"
    DELTA = "delta"

class WriteMode(str, Enum):
    OVERWRITE = "overwrite"
    APPEND = "append"

class DataLake:
    def __init__(self, account_name: str, container: str, *, prefix: str = "", account_key: str | None = None, lease_seconds: int = 60, lock_wait_seconds: float = 30):
        self.account_name = account_name
        self.container_name = container
        self.prefix = prefix.strip("/")
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
        parts = path.replace("\\", "/").split("/")
        if not path or path.startswith("/") or any(p in ("", ".", "..") for p in parts):
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
        try:
            # Conditional read pins the blob to a single ETag.
            props = blob.get_blob_properties()
            content = blob.download_blob(etag=props.etag, match_condition=MatchConditions.IfNotModified).readall()
        except ResourceNotFoundError:
            if missing_ok:
                return pd.DataFrame()
            raise
        return pq.read_table(io.BytesIO(content)).to_pandas()

    def read(self, path: str, *, format: StorageFormat | str = StorageFormat.PARQUET, missing_ok: bool = False, filters: dict | None = None, timestamp_col: str | None = None, start=None, end=None) -> pd.DataFrame:
        full = self._path(path)
        fmt = StorageFormat(format)
        if fmt is StorageFormat.PARQUET:
            df = self._read_parquet_blob(full, missing_ok=missing_ok)
        else:
            try:
                df = DeltaTable(self._delta_uri(full), storage_options=self._delta_options()).to_pandas()
            except Exception:
                # Never mask permissions/network/corruption as missing data.
                raise
        return filter_frame(df, filters, timestamp_col=timestamp_col, start=start, end=end)

    def write(self, path: str, data: pd.DataFrame, *, format: StorageFormat | str = StorageFormat.PARQUET, mode: WriteMode | str = WriteMode.OVERWRITE, keys: list[str] | None = None, sort_by: str | None = None, partition_by: list[str] | None = None) -> None:
        if not isinstance(data, pd.DataFrame):
            raise TypeError("data must be a pandas DataFrame")
        fmt, write_mode = StorageFormat(format), WriteMode(mode)
        full = self._path(path)
        if fmt is StorageFormat.DELTA:
            self._write_delta(full, data, write_mode, keys, partition_by)
        else:
            if partition_by:
                raise ValueError("partition_by is for Delta tables; use explicit paths for standalone Parquet")
            self._write_parquet(full, data, write_mode, keys, sort_by)

    def upsert(self, path: str, data: pd.DataFrame, *, keys: list[str], format: StorageFormat | str = StorageFormat.PARQUET, sort_by: str | None = None, partition_by: list[str] | None = None) -> None:
        if not keys:
            raise ValueError("keys required")
        self.write(path, data, format=format, mode=WriteMode.APPEND, keys=keys, sort_by=sort_by, partition_by=partition_by)

    def _write_parquet(self, full: str, data: pd.DataFrame, mode: WriteMode, keys: list[str] | None, sort_by: str | None):
        if data.empty:
            return
        with self._lock(full).acquire() as lost:
            blob = self._container.get_blob_client(full)
            try:
                props = blob.get_blob_properties()
                etag = props.etag
                current = self._read_parquet_blob(full, missing_ok=False) if mode is WriteMode.APPEND else pd.DataFrame()
            except ResourceNotFoundError:
                etag = None
                current = pd.DataFrame()
            if mode is WriteMode.APPEND:
                result = merge_frames(current, data, keys, sort_by) if keys else pd.concat([current, data], ignore_index=True)
            else:
                result = data.copy()
            out = io.BytesIO()
            pq.write_table(pa.Table.from_pandas(result, preserve_index=False), out, compression="zstd")
            if lost.is_set():
                raise LockLost("Lost write lease before commit")
            if etag is None:
                blob.upload_blob(out.getvalue(), overwrite=False)
            else:
                blob.upload_blob(out.getvalue(), overwrite=True, etag=etag, match_condition=MatchConditions.IfNotModified)

    def _write_delta(self, full: str, data: pd.DataFrame, mode: WriteMode, keys: list[str] | None, partition_by: list[str] | None):
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
                incoming = data.drop_duplicates(subset=keys, keep="last")
                try:
                    table = DeltaTable(uri, storage_options=opts)
                except Exception as exc:
                    # Only treat an absent _delta_log as a missing table.
                    if not self._delta_log_exists(full):
                        if lost.is_set():
                            raise LockLost("Lost lease before table creation")
                        write_deltalake(uri, incoming, mode="error", partition_by=partition_by, storage_options=opts)
                        return
                    raise exc
                source = pa.Table.from_pandas(incoming, preserve_index=False)
                predicate = " AND ".join(f'target."{k}" = source."{k}"' for k in keys)
                if lost.is_set():
                    raise LockLost("Lost lease before Delta merge")
                (table.merge(source=source, predicate=predicate, source_alias="source", target_alias="target")
                 .when_matched_update_all().when_not_matched_insert_all().execute())
            else:
                if lost.is_set():
                    raise LockLost("Lost lease before Delta commit")
                write_deltalake(uri, data, mode=mode.value, partition_by=partition_by if mode is WriteMode.OVERWRITE or not self._delta_log_exists(full) else None, storage_options=opts)

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
