"""Test configuration. Azure tests are opt-in and use a disposable test prefix."""
import os
import uuid
import pytest


def pytest_addoption(parser):
    parser.addoption('--run-azure', action='store_true', default=False, help='Run real Azure Blob integration tests (writes and deletes test blobs)')


def pytest_configure(config):
    config.addinivalue_line('markers', 'azure: live Azure Storage integration test, opt-in')
    config.addinivalue_line('markers', 'delta: Delta Lake test')


@pytest.fixture
def azure_lake(request):
    if not request.config.getoption('--run-azure'):
        pytest.skip('Pass --run-azure to enable real Azure Storage tests')
    expected = os.getenv('TEST_MODE_CACHE_BLOB', 'test-mode-data')
    production = os.getenv('MARKET_DATA_CACHE_BLOB', 'market-data-cache')
    if os.getenv('TEST_MODE', '').lower() != 'true' or os.getenv('DATA_LAKE_CONTAINER') != expected or expected == production:
        pytest.fail('Live tests require TEST_MODE=true and a distinct test DATA_LAKE_CONTAINER matching TEST_MODE_CACHE_BLOB')
    for key in ('DATA_LAKE_ACCOUNT', 'DATA_LAKE_CONTAINER', 'AZURE_STORAGE_ACCOUNT_KEY'):
        if not os.getenv(key):
            pytest.fail(f'{key} required for live integration tests')
    from azure_data_toolkit import DataLake
    prefix = f'_adl_test_runs/{uuid.uuid4().hex}'
    lake = DataLake(account_name=os.environ['DATA_LAKE_ACCOUNT'], container=os.environ['DATA_LAKE_CONTAINER'], prefix=prefix, account_key=os.environ['AZURE_STORAGE_ACCOUNT_KEY'])
    try:
        yield lake
    finally:
        # Delete only blobs created within this unique test run prefix.
        for owned_prefix in (prefix + '/', '_adl_locks/' + prefix + '/'):
            for item in lake._container.list_blobs(name_starts_with=owned_prefix):
                lake._container.delete_blob(item.name)
        lake.close()

