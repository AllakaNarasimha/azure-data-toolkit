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


def test_renewal_failure_marks_lease_lost_and_blocks_commit(clients, monkeypatch):
 from threading import Event
 from azure_data_toolkit import DataLake, LockLost
 blob, lease = clients
 blob.exists.return_value = True
 lease.renew.side_effect = error('LeaseLost', 412)
 class ImmediateRenewal(Event):
  def __init__(self):
   super().__init__()
   self.first_wait = True
  def wait(self, timeout=None):
   if self.first_wait and timeout is not None:
    self.first_wait = False
    return False
   return super().wait(timeout)
 events = iter([ImmediateRenewal(), Event()])
 monkeypatch.setattr('azure_data_toolkit.locking.threading.Event', lambda: next(events, None) or Event())
 action = MagicMock()
 with lock(blob).acquire() as lost:
  assert lost.wait(timeout=2), 'renewal worker did not report lease loss'
  with pytest.raises(LockLost):
   DataLake._commit(action, lost, 'x')
 action.assert_not_called()
 lease.renew.assert_called_once()
 lease.release.assert_called_once()
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
