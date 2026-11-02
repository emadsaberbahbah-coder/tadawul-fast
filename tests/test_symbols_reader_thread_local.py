"""tests/test_symbols_reader_thread_local.py - core/symbols_reader v8.7.0.

WHY (2026-10-21)
----------------
GoogleSheetsReader built ONE googleapiclient service and every thread of the
10-worker _IO_EXECUTOR shared it. Its httplib2 transport is not thread-safe;
Render logged "Failed to list sheets: 'NoneType' object has no attribute
'close'", "Failed to list sheets: 00000001" and a transient SSLError within
one second on 20 Oct 23:15:18Z, followed by 25 s proxy timeouts. v8.7.0 builds
the service (and its credentials) per calling thread.

Cases (no network: build/Credentials/creds loader are replaced by fakes):
  T1 one thread: repeated _get_service() returns the same object; one build
  T2 eight threads: eight distinct services, never shared between threads
  T3 concurrent read_range / list_sheets on the real _IO_EXECUTOR: every call
     executes on the service owned by the thread that runs it
  T4 TFB_SR_THREAD_LOCAL_SERVICE=0 restores the v8.6.0 single shared service
  T5 a failed build is not cached (the next call on that thread retries) and
     returns None, like the shared path
  T6 version 8.7.0

Run:  python -m pytest -q tests/test_symbols_reader_thread_local.py
"""
from __future__ import annotations

import concurrent.futures
import sys
import threading
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import core.symbols_reader as sr  # noqa: E402


class _Exec:
    def __init__(self, fn):
        self._fn = fn

    def execute(self):
        return self._fn()


class FakeService:
    def __init__(self, owner):
        self.owner = owner          # thread that built it
        self.used_by = []           # threads that executed requests on it

    def spreadsheets(self):
        return self

    def values(self):
        return self

    def get(self, **kw):
        def run():
            self.used_by.append(threading.get_ident())
            if "range" in kw:
                return {"values": [["Symbol"], ["AAA"]]}
            return {"sheets": [{"properties": {"title": "Market_Leaders"}}]}
        return _Exec(run)


@pytest.fixture
def fakes(monkeypatch):
    built = []

    def fake_build(api, version, credentials=None, cache_discovery=True):
        assert (api, version, cache_discovery) == ("sheets", "v4", False)
        svc = FakeService(threading.get_ident())
        built.append(svc)
        return svc

    class FakeCreds:
        @staticmethod
        def from_service_account_info(info, scopes=None):
            assert scopes == ["https://www.googleapis.com/auth/spreadsheets.readonly"]
            return object()

    monkeypatch.setattr(sr, "GOOGLE_API_AVAILABLE", True)
    monkeypatch.setattr(sr, "build", fake_build)
    monkeypatch.setattr(sr, "Credentials", FakeCreds)
    monkeypatch.setattr(sr, "_load_google_creds_dict_from_env", lambda: {"type": "service_account"})
    monkeypatch.delenv("TFB_SR_THREAD_LOCAL_SERVICE", raising=False)
    return built


def test_t1_same_thread_reuses_its_service(fakes):
    reader = sr.GoogleSheetsReader()
    a, b = reader._get_service(), reader._get_service()
    assert a is b and len(fakes) == 1 and a.owner == threading.get_ident()


def test_t2_each_thread_gets_its_own_service(fakes):
    reader = sr.GoogleSheetsReader()
    got = {}
    barrier = threading.Barrier(8)

    def work():
        barrier.wait()
        s1 = reader._get_service()
        s2 = reader._get_service()
        got[threading.get_ident()] = (s1, s2)

    threads = [threading.Thread(target=work) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    services = [pair[0] for pair in got.values()]
    assert all(s1 is s2 for s1, s2 in got.values())
    assert len({id(s) for s in services}) == 8 == len(fakes)
    assert all(svc.owner == tid for tid, (svc, _) in got.items())
    assert reader._thread_services == 8 and reader._service in services


def test_t3_concurrent_reads_never_share_a_transport(fakes):
    reader = sr.GoogleSheetsReader()
    jobs = []
    for i in range(40):
        if i % 4 == 0:
            jobs.append(sr._IO_EXECUTOR.submit(reader.list_sheets, "sid"))
        else:
            jobs.append(sr._IO_EXECUTOR.submit(reader.read_range, "sid", f"Market_Leaders!A{i}:A{i + 1}"))
    results = [j.result(timeout=30) for j in jobs]
    assert all(r for r in results)
    for svc in fakes:
        assert set(svc.used_by) <= {svc.owner}, "a service executed requests for another thread"
    assert sum(len(s.used_by) for s in fakes) == 40


def test_t4_gate_off_restores_shared_service(fakes, monkeypatch):
    monkeypatch.setenv("TFB_SR_THREAD_LOCAL_SERVICE", "0")
    reader = sr.GoogleSheetsReader()
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        services = list(pool.map(lambda _: reader._get_service(), range(12)))
    assert len({id(s) for s in services}) == 1 and len(fakes) == 1


def test_t5_failed_build_is_not_cached(fakes, monkeypatch):
    calls = {"n": 0}

    def flaky_build(*a, **k):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("transient discovery failure")
        return FakeService(threading.get_ident())

    monkeypatch.setattr(sr, "build", flaky_build)
    reader = sr.GoogleSheetsReader()
    assert reader._get_service() is None
    svc = reader._get_service()
    assert isinstance(svc, FakeService) and calls["n"] == 2
    assert reader._get_service() is svc and calls["n"] == 2


def test_t6_version():
    assert sr.SCRIPT_VERSION == "8.7.0"
