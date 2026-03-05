from __future__ import annotations

import sys
import types

from typedb_ops_spine.readiness import connect_with_retries


class _FakeDriver:
    def __init__(self):
        self.closed = False

        class _Databases:
            def all(self):
                raise RuntimeError("not ready")

        self.databases = _Databases()

    def close(self):
        self.closed = True


def test_connect_with_retries_closes_driver_on_failed_roundtrip(monkeypatch):
    created: list[_FakeDriver] = []

    class _Credentials:
        def __init__(self, *_args, **_kwargs):
            pass

    class _DriverOptions:
        def __init__(self, **_kwargs):
            pass

    class _TypeDB:
        @staticmethod
        def driver(*_args, **_kwargs):
            d = _FakeDriver()
            created.append(d)
            return d

    fake_mod = types.SimpleNamespace(
        Credentials=_Credentials,
        DriverOptions=_DriverOptions,
        TypeDB=_TypeDB,
    )
    monkeypatch.setitem(sys.modules, "typedb.driver", fake_mod)
    monkeypatch.setattr("time.sleep", lambda *_args, **_kwargs: None)

    try:
        connect_with_retries("localhost:1729", "admin", "password", retries=2, sleep_s=0)
    except RuntimeError:
        pass

    assert len(created) == 2
    assert all(d.closed for d in created)
