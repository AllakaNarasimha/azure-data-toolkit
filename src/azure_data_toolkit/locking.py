"""Renewable, shared Azure Blob lease for cooperative writers."""
from __future__ import annotations
import threading
import time
from contextlib import contextmanager
from azure.core.exceptions import HttpResponseError, ResourceExistsError
from azure.storage.blob import BlobLeaseClient

class LockUnavailable(TimeoutError):
    pass

class LockLost(RuntimeError):
    pass

class BlobLock:
    def __init__(self, container, lock_path: str, *, lease_seconds: int = 60, wait_seconds: float = 30):
        self.blob = container.get_blob_client(lock_path)
        self.lease_seconds = lease_seconds
        self.wait_seconds = wait_seconds
        if not 15 <= lease_seconds <= 60:
            raise ValueError("lease_seconds must be between 15 and 60")

    @contextmanager
    def acquire(self):
        try:
            self.blob.upload_blob(b"", overwrite=False)
        except ResourceExistsError:
            pass
        lease = BlobLeaseClient(self.blob)
        deadline = time.monotonic() + self.wait_seconds
        while True:
            try:
                lease.acquire(lease_duration=self.lease_seconds)
                break
            except HttpResponseError as exc:
                if getattr(exc, "status_code", None) not in (409, 412):
                    raise
                if time.monotonic() >= deadline:
                    raise LockUnavailable(f"Could not acquire {self.blob.blob_name}") from exc
                time.sleep(min(0.5, max(0.01, deadline - time.monotonic())))
        stop = threading.Event()
        lost = threading.Event()
        def renew():
            while not stop.wait(self.lease_seconds / 3):
                try:
                    lease.renew()
                except Exception:
                    lost.set()
                    return
        thread = threading.Thread(target=renew, daemon=True)
        thread.start()
        try:
            yield lost
        finally:
            stop.set()
            thread.join(timeout=5)
            try:
                lease.release()
            except HttpResponseError:
                lost.set()
