"""Offline regressions, including the real Delta engine on temporary storage."""
from contextlib import contextmanager
from threading import Event
from types import SimpleNamespace
import pandas as pd
import pyarrow as pa
import pytest
from azure.core.exceptions import ResourceModifiedError, ServiceRequestError
from azure_data_toolkit import DataLake, SchemaMismatch, WriteOutcomeUnknown
from azure_data_toolkit.locking import LockLost
from azure_data_toolkit.frames import merge_frames, filter_frame


@contextmanager
def acquired():
    yield Event()


@pytest.fixture
def local_delta(tmp_path):
    lake = object.__new__(DataLake)
    lake.prefix = ''
    lake.container_name = 'offline'
    lake._delta_uri = lambda path: str(tmp_path / path)
    lake._delta_options = lambda: {}
    lake._delta_log_exists = lambda path: (tmp_path / path / '_delta_log').exists()
    lake._lock = lambda path: SimpleNamespace(acquire=acquired)
    return lake


def test_revision_merge_rejects_stale_update():
    current = pd.DataFrame({'id': [1], 'rev': [3], 'value': [30]})
    incoming = pd.DataFrame({'id': [1, 2, 2], 'rev': [2, 4, 1], 'value': [20, 40, 10]})
    result = merge_frames(current, incoming, ['id'], revision_col='rev').sort_values('id')
    assert result.value.tolist() == [30, 40]


@pytest.mark.parametrize('revision', [None, float('nan')])
def test_null_revision_rejected(revision):
    with pytest.raises(ValueError):
        merge_frames(pd.DataFrame(), pd.DataFrame({'id': [1], 'rev': [revision]}), ['id'], revision_col='rev')


def test_commit_outcomes():
    lost = Event()
    lost.set()
    with pytest.raises(LockLost):
        DataLake._commit(lambda: pytest.fail('must not commit'), lost, 'table')
    lost.clear()
    def ambiguous():
        raise ServiceRequestError('connection interrupted')
    with pytest.raises(WriteOutcomeUnknown):
        DataLake._commit(ambiguous, lost, 'table')
    with pytest.raises(WriteOutcomeUnknown):
        DataLake._commit(lost.set, lost, 'table')


def test_schema_contract():
    schema = pa.schema([pa.field('id', pa.int64(), nullable=False)])
    DataLake._validate_schema(pd.DataFrame({'id': [1]}), schema)
    with pytest.raises(SchemaMismatch):
        DataLake._validate_schema(pd.DataFrame({'id': ['1']}), schema)
    with pytest.raises(SchemaMismatch):
        DataLake._validate_schema(pd.DataFrame({'id': pd.Series([None], dtype='Int64')}), schema)


def test_epoch_units_explicit():
    data = pd.DataFrame({'ts': [1700000000, 1700000060]})
    with pytest.raises(ValueError, match='timestamp_unit'):
        filter_frame(data, timestamp_col='ts', start='2023-11-14')
    assert len(filter_frame(data, timestamp_col='ts', timestamp_unit='s', start='2023-11-14T22:14:00Z')) == 1


def test_read_conflict_bounded():
    lake = object.__new__(DataLake)
    lake.read_retries = 2
    lake.read_retry_delay = 0
    lake.container_name = 'offline'
    calls = []
    def properties():
        calls.append(1)
        return SimpleNamespace(etag=str(len(calls)))
    def download(**kwargs):
        raise ResourceModifiedError('changed')
    blob = SimpleNamespace(get_blob_properties=properties, download_blob=download)
    lake._container = SimpleNamespace(get_blob_client=lambda path: blob)
    with pytest.raises(ResourceModifiedError):
        lake._read_parquet_blob('file', missing_ok=False)
    assert len(calls) == 3


def test_delta_revision_retry_and_projection(local_delta):
    lake = local_delta
    schema = pa.schema([('id', pa.int64()), ('rev', pa.int64()), ('value', pa.int64())])
    newer = pd.DataFrame({'id': [1], 'rev': [3], 'value': [30]})
    for data in (newer, newer, pd.DataFrame({'id': [1, 2], 'rev': [2, 1], 'value': [20, 10]})):
        lake.upsert('table', data, keys=['id'], revision_col='rev', schema=schema, format='delta')
    assert lake.read('table', format='delta').sort_values('id').value.tolist() == [30, 10]
    projected = lake.read('table', format='delta', filters={'id': [1]}, columns=['value'])
    assert projected.to_dict('list') == {'value': [30]}
    with pytest.raises(SchemaMismatch):
        lake.upsert('table', pd.DataFrame({'id': [3], 'rev': [1], 'value': ['bad']}), keys=['id'], schema=schema, format='delta')


def test_missing_delta_and_broken_log(local_delta, tmp_path):
    assert local_delta.read('missing', format='delta', missing_ok=True).empty
    (tmp_path / 'broken' / '_delta_log').mkdir(parents=True)
    with pytest.raises(Exception):
        local_delta.read('broken', format='delta', missing_ok=True)


def test_require_append_keys(local_delta):
    local_delta.require_append_keys = True
    with pytest.raises(ValueError, match='append requires keys'):
        local_delta.write('table', pd.DataFrame({'id': [1]}), format='delta', mode='append')


@pytest.mark.parametrize('path', ['../table', 'a//b', 'a\\b', '/table'])
def test_noncanonical_paths(local_delta, path):
    with pytest.raises(ValueError):
        local_delta._path(path)
