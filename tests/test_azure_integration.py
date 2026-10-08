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
