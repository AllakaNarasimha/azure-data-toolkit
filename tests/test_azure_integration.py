"""Real Azure tests: run only with --run-azure. Unique disposable prefixes."""
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import pytest
from azure_data_toolkit import DataLake, StorageFormat

pytestmark = pytest.mark.azure


def test_parquet_roundtrip_and_upsert(azure_lake):
    path = 'quotes/symbol=A/year=2026/month=10/day=08/part-0.parquet'
    azure_lake.upsert(path, pd.DataFrame({'id': [1], 'price': [10]}), keys=['id'])
    azure_lake.upsert(path, pd.DataFrame({'id': [1, 2], 'price': [11, 20]}), keys=['id'])
    result = azure_lake.read(path).sort_values('id')
    assert result['price'].tolist() == [11, 20]


def test_parallel_writers_same_parquet_blob(azure_lake):
    path = 'concurrency/shared.parquet'
    def write_one(n):
        azure_lake.upsert(path, pd.DataFrame({'id': [n], 'value': [n * 10]}), keys=['id'])
    with ThreadPoolExecutor(max_workers=6) as executor:
        list(executor.map(write_one, range(12)))
    result = azure_lake.read(path)
    assert len(result) == 12
    assert set(result['id']) == set(range(12))


def test_parallel_writers_different_blobs(azure_lake):
    def write_one(n):
        path = f'concurrency/partition={n}/part-0.parquet'
        azure_lake.write(path, pd.DataFrame({'id': [n]}))
        assert azure_lake.read(path)['id'].tolist() == [n]
    with ThreadPoolExecutor(max_workers=6) as executor:
        list(executor.map(write_one, range(12)))


@pytest.mark.delta
def test_delta_roundtrip_and_upsert(azure_lake):
    path = 'delta/test_table'
    azure_lake.write(path, pd.DataFrame({'id': [1, 2], 'price': [10, 20]}), format=StorageFormat.DELTA)
    azure_lake.upsert(path, pd.DataFrame({'id': [2, 3], 'price': [22, 30]}), keys=['id'], format=StorageFormat.DELTA)
    result = azure_lake.read(path, format=StorageFormat.DELTA).sort_values('id')
    assert result['price'].tolist() == [10, 22, 30]


@pytest.mark.delta
def test_parallel_delta_writers(azure_lake):
    path = 'delta/concurrency'
    azure_lake.write(path, pd.DataFrame({'id': [-1], 'price': [0]}), format=StorageFormat.DELTA)
    def write_one(n):
        azure_lake.upsert(path, pd.DataFrame({'id': [n], 'price': [n]}), keys=['id'], format=StorageFormat.DELTA)
    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(write_one, range(8)))
    result = azure_lake.read(path, format=StorageFormat.DELTA)
    assert set(result['id']) == set(range(-1, 8))



def _process_writer(config, path, fmt, number, results):
    """Top-level entry point supports Windows spawn, with independent clients."""
    try:
        with DataLake(**config) as lake:
            lake.upsert(path, pd.DataFrame({'id': [number], 'value': [number]}), keys=['id'], format=fmt)
        results.put((number, None))
    except Exception as exc:
        results.put((number, type(exc).__name__))


@pytest.mark.parametrize('fmt,path', [('parquet', 'process/shared.parquet'), ('delta', 'process/table')])
def test_independent_process_writers(azure_lake, fmt, path):
    import multiprocessing
    context = multiprocessing.get_context('spawn')
    results = context.Queue()
    config = dict(account_name=azure_lake.account_name, container=azure_lake.container_name,
                  account_key=azure_lake.account_key, prefix=azure_lake.prefix)
    workers = [context.Process(target=_process_writer, args=(config, path, fmt, n, results)) for n in range(3)]
    try:
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(timeout=45)
            assert not worker.is_alive(), 'Writer exceeded timeout'
            assert worker.exitcode == 0
        outcomes = [results.get(timeout=5) for _ in workers]
        assert all(error is None for _, error in outcomes), outcomes
        assert set(azure_lake.read(path, format=fmt).id) == {0, 1, 2}
    finally:
        for worker in workers:
            if worker.is_alive():
                worker.terminate()
                worker.join(timeout=5)
        results.close()


@pytest.mark.parametrize('fmt,path', [('parquet', 'overlap/shared.parquet'), ('delta', 'overlap/table')])
def test_reader_during_writer(azure_lake, fmt, path):
    from threading import Event
    azure_lake.write(path, pd.DataFrame({'id': [1], 'rev': [0], 'value': [0]}), format=fmt)
    ready, finished = Event(), Event()
    def writer():
        assert ready.wait(timeout=10)
        try:
            for revision in range(1, 9):
                azure_lake.upsert(path, pd.DataFrame({'id': [1], 'rev': [revision], 'value': [revision]}), keys=['id'], revision_col='rev', format=fmt)
        finally:
            finished.set()
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(writer)
        ready.set()
        observations = 0
        while not finished.is_set() and observations < 100:
            snapshot = azure_lake.read(path, format=fmt)
            assert len(snapshot) == 1
            assert snapshot.rev.iloc[0] == snapshot.value.iloc[0]
            observations += 1
        future.result(timeout=45)
    assert observations > 0
    assert azure_lake.read(path, format=fmt).rev.tolist() == [8]


@pytest.mark.parametrize('fmt,path', [('parquet', 'revision/shared.parquet'), ('delta', 'revision/table')])
def test_stale_update_and_repeat(azure_lake, fmt, path):
    newer = pd.DataFrame({'id': [1], 'rev': [2], 'value': [20]})
    for frame in (newer, newer, pd.DataFrame({'id': [1], 'rev': [1], 'value': [10]})):
        azure_lake.upsert(path, frame, keys=['id'], revision_col='rev', format=fmt)
    assert azure_lake.read(path, format=fmt).to_dict('list') == newer.to_dict('list')


def _crash_holding_lease(config, path, ready):
    import os
    with DataLake(**config) as lake:
        with lake._lock(lake._path(path)).acquire():
            ready.set()
            os._exit(0)  # Simulate process death without releasing the lease.


def test_recovery_after_writer_crash(azure_lake):
    import multiprocessing
    context = multiprocessing.get_context('spawn')
    ready = context.Event()
    config = dict(account_name=azure_lake.account_name, container=azure_lake.container_name,
                  account_key=azure_lake.account_key, prefix=azure_lake.prefix,
                  lease_seconds=15, lock_wait_seconds=30)
    path = 'recovery/shared.parquet'
    worker = context.Process(target=_crash_holding_lease, args=(config, path, ready))
    try:
        worker.start()
        assert ready.wait(timeout=15)
        worker.join(timeout=5)
        assert worker.exitcode == 0
        with DataLake(**config) as lake:
            lake.upsert(path, pd.DataFrame({'id': [1]}), keys=['id'])
            assert lake.read(path).id.tolist() == [1]
    finally:
        if worker.is_alive():
            worker.terminate()
            worker.join(timeout=5)
