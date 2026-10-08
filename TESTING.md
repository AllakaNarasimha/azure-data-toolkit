# Testing the shared azure-delta-lake library

## Prerequisites
- Python 3.11 or 3.12 recommended.
- For real Azure tests: an Azure Storage account, an existing container, and an account key.
- The tests **write and delete** blobs under unique `_adl_test_runs/<uuid>/` prefixes. Use a dedicated test container whenever possible.

## 1. Extract and install (Windows PowerShell)
```powershell
Expand-Archive .\azure-delta-lake-with-tests.zip -DestinationPath . -Force
cd .\azure-delta-lake
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[test]"
```

## 2. Run automated offline tests
```powershell
python -m pytest -q -m "not azure"
```

## 3. Run tests against real Azure Storage
Create a **dedicated test container**. Never run the integration suite against production market data.

```powershell
$env:DATA_LAKE_ACCOUNT="yourstorageaccount"
$env:DATA_LAKE_CONTAINER="your-test-container"
$env:AZURE_STORAGE_ACCOUNT_KEY="your-account-key"
python -m pytest -v --run-azure
```

If you want to run only the live Parquet tests:
```powershell
python -m pytest -v tests/test_azure_integration.py -k parquet --run-azure
```

To run Delta-specific tests:
```powershell
python -m pytest -v -m delta --run-azure
```

## 4. Linux / macOS
```bash
unzip azure-delta-lake-with-tests.zip
cd azure-delta-lake
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[test]'
python -m pytest -q -m 'not azure'
```

## 5. Build wheel
```bash
python -m build
```
The wheel appears under `dist/`.

## Important limitations
- Offline tests use mocks. They cannot establish Azure service behavior or validate the SDK/Delta backend.
- Live tests require Azure credentials and network access, and may incur small storage transaction costs.
- The tests are **not** a production certification. Still needed: lease-expiry/fault injection, network interruption/ambiguous commit tests, cross-process tests across separate Function Apps, performance/load tests, and verification of Delta commit guarantees on the chosen backend.
- All competing writers must follow the same coordination convention; raw Blob SDK writes can bypass it.
- Do not run destructive integration tests against important data. Tests create a unique prefix and clean it up afterward.
