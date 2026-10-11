from types import SimpleNamespace
from unittest.mock import MagicMock
import pytest
from azure.core.exceptions import HttpResponseError,ResourceExistsError
from azure_data_toolkit.locking import BlobLock,LockUnavailable


def error(code,status=412):
 exc=HttpResponseError(code);exc.error_code=code;exc.status_code=status
 return exc

@pytest.fixture
def clients(monkeypatch):
 blob=MagicMock();blob.blob_name='lock'
 lease=MagicMock()
 monkeypatch.setattr('azure_data_toolkit.locking.BlobLeaseClient',lambda _:lease)
 return blob,lease

def lock(blob,wait=0):
 return BlobLock(SimpleNamespace(get_blob_client=lambda _:blob),'lock',lease_seconds=15,wait_seconds=wait)

def test_existing_leased_blob_goes_to_acquire_not_upload(clients):
 blob,lease=clients;blob.exists.return_value=True
 blob.upload_blob.side_effect=error('LeaseIdMissing')
 lease.acquire.side_effect=error('LeaseAlreadyPresent',409)
 with pytest.raises(LockUnavailable):
  with lock(blob).acquire():pass
 blob.upload_blob.assert_not_called()
 lease.acquire.assert_called_once()

def test_create_and_lease_race_then_acquire(clients):
 blob,lease=clients;blob.exists.side_effect=[False,True]
 blob.upload_blob.side_effect=error('LeaseIdMissing')
 with lock(blob).acquire() as lost:assert not lost.is_set()
 lease.acquire.assert_called_once();lease.release.assert_called_once()

def test_new_blob_created(clients):
 blob,lease=clients;blob.exists.return_value=False
 with lock(blob).acquire():pass
 blob.upload_blob.assert_called_once_with(b'',overwrite=False)

def test_unleased_creation_race(clients):
 blob,lease=clients;blob.exists.return_value=False
 blob.upload_blob.side_effect=ResourceExistsError('exists')
 with lock(blob).acquire():pass
 lease.release.assert_called_once()

@pytest.mark.parametrize('code,status',[('AuthorizationFailure',403),('ConditionNotMet',412),('ServerBusy',503)])
def test_other_errors_propagate(clients,code,status):
 blob,lease=clients;blob.exists.return_value=False
 blob.upload_blob.side_effect=error(code,status)
 with pytest.raises(HttpResponseError):
  with lock(blob).acquire():pass
 lease.acquire.assert_not_called()

def test_lease_error_without_existing_blob_propagates(clients):
 blob,lease=clients;blob.exists.side_effect=[False,False]
 blob.upload_blob.side_effect=error('LeaseIdMissing')
 with pytest.raises(HttpResponseError):
  with lock(blob).acquire():pass
 lease.acquire.assert_not_called()

def test_existing_lock_reusable_after_release(clients):
 blob,lease=clients;blob.exists.return_value=True
 for _ in range(2):
  with lock(blob).acquire():pass
 assert lease.acquire.call_count==2
 assert lease.release.call_count==2
 blob.upload_blob.assert_not_called()
