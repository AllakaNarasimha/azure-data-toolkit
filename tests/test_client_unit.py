"""Offline tests using fake Azure Blob clients; no Azure account needed."""
from __future__ import annotations
import io
from types import SimpleNamespace
from unittest.mock import MagicMock
import pandas as pd
import pytest
from azure.core.exceptions import ResourceNotFoundError, HttpResponseError
from azure_data_toolkit.client import DataLake, StorageFormat
from azure_data_toolkit.locking import LockLost


@pytest.fixture
def lake():
    obj = object.__new__(DataLake)
    obj.prefix = 'test'
    obj._container = MagicMock()
    obj._service = MagicMock()
    obj._credential = None
    obj.account_name = 'fake'
    obj.container_name = 'fake'
    obj.account_key = 'fake'
    obj.lease_seconds = 60
    obj.lock_wait_seconds = 1
    return obj


def test_path_validation(lake):
    assert lake._path('symbol=A/year=2026/part-0.parquet') == 'test/symbol=A/year=2026/part-0.parquet'
    for invalid in ('', '../escape', '/absolute', 'a//b', 'a/./b', 'a/../b'):
        with pytest.raises(ValueError):
            lake._path(invalid)


def test_read_missing_ok(lake):
    blob = lake._container.get_blob_client.return_value
    blob.get_blob_properties.side_effect = ResourceNotFoundError('not found')
    assert lake.read('missing.parquet', missing_ok=True).empty
    with pytest.raises(ResourceNotFoundError):
        lake.read('missing.parquet')


def test_read_uses_etag(lake, monkeypatch):
    blob = lake._container.get_blob_client.return_value
    blob.get_blob_properties.return_value = SimpleNamespace(etag='etag-v1')
    blob.download_blob.return_value.readall.return_value = b'parquet-bytes'
    monkeypatch.setattr('azure_data_toolkit.client.pq.read_table', lambda _: SimpleNamespace(to_pandas=lambda: pd.DataFrame({'id': [1]})))
    assert lake.read('x.parquet')['id'].tolist() == [1]
    assert blob.download_blob.call_args.kwargs['etag'] == 'etag-v1'


class FakeLock:
    def __init__(self, lost=False):
        self.lost = lost
    def acquire(self):
        from contextlib import contextmanager
        import threading
        @contextmanager
        def ctx():
            e = threading.Event()
            if self.lost:
                e.set()
            yield e
        return ctx()


def test_upsert_merge_happens_under_lock(lake, monkeypatch):
    blob = lake._container.get_blob_client.return_value
    blob.get_blob_properties.return_value = SimpleNamespace(etag='etag-v1')
    monkeypatch.setattr(lake, '_lock', lambda _: FakeLock())
    monkeypatch.setattr(lake, '_read_parquet_blob', lambda *_args, **_kw: pd.DataFrame({'id': [1], 'value': ['old']}))
    captured = []
    monkeypatch.setattr('azure_data_toolkit.client.pq.write_table', lambda table, buffer, **kwargs: captured.append(table.to_pandas()))
    lake.upsert('x.parquet', pd.DataFrame({'id': [1, 2], 'value': ['new', 'other']}), keys=['id'])
    assert captured[0].sort_values('id')['value'].tolist() == ['new', 'other']
    assert blob.upload_blob.call_args.kwargs['etag'] == 'etag-v1'


def test_lost_lease_aborts_before_upload(lake, monkeypatch):
    blob = lake._container.get_blob_client.return_value
    blob.get_blob_properties.side_effect = ResourceNotFoundError('missing')
    monkeypatch.setattr(lake, '_lock', lambda _: FakeLock(lost=True))
    with pytest.raises(LockLost):
        lake.write('x.parquet', pd.DataFrame({'id': [1]}))
    blob.upload_blob.assert_not_called()


def test_delta_requires_key_auth(lake):
    lake.account_key = None
    with pytest.raises(RuntimeError, match='AZURE_STORAGE_ACCOUNT_KEY'):
        lake._delta_options()


def test_delta_upsert_rejects_missing_keys(lake, monkeypatch):
    monkeypatch.setattr(lake, '_lock', lambda _: FakeLock())
    with pytest.raises(ValueError, match='upsert key absent'):
        lake.upsert('table', pd.DataFrame({'other': [1]}), keys=['id'], format=StorageFormat.DELTA)


def test_parquet_partition_by_not_accepted(lake):
    with pytest.raises(ValueError, match='partition_by'):
        lake.write('file.parquet', pd.DataFrame({'id': [1]}), partition_by=['id'])
