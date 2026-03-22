from __future__ import annotations

import sys
import types

from typedb_ops_spine.readiness import (
    connect_with_retries,
    infer_tls_enabled,
    normalize_typedb_address,
    resolve_connection_address,
)


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


def test_normalize_typedb_address_local_host_defaults_port():
    assert normalize_typedb_address("localhost") == "localhost:1729"


def test_normalize_typedb_address_cloud_endpoint_preserves_scheme():
    assert (
        normalize_typedb_address("https://cloud.typedb.com", default_port=443)
        == "https://cloud.typedb.com:443"
    )


def test_resolve_connection_address_prefers_explicit_address():
    assert (
        resolve_connection_address("https://cloud.typedb.com", "ignored-host", 443)
        == "https://cloud.typedb.com:443"
    )


def test_infer_tls_enabled_from_https_address():
    assert infer_tls_enabled("https://cloud.typedb.com:443", None) is True
    assert infer_tls_enabled("localhost:1729", None) is False


def test_connect_with_retries_infers_tls_from_https_address(monkeypatch):
    captured: dict[str, object] = {}

    class _Credentials:
        def __init__(self, *_args, **_kwargs):
            pass

    class _DriverOptions:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    class _Database:
        def __init__(self, name: str):
            self.name = name

    class _Driver:
        def __init__(self):
            self.databases = types.SimpleNamespace(
                all=lambda: [_Database("ops_db")],
            )

        def close(self):
            captured["closed"] = True

    class _TypeDB:
        @staticmethod
        def driver(address, *_args, **_kwargs):
            captured["address"] = address
            return _Driver()

    fake_mod = types.SimpleNamespace(
        Credentials=_Credentials,
        DriverOptions=_DriverOptions,
        TypeDB=_TypeDB,
    )
    monkeypatch.setitem(sys.modules, "typedb.driver", fake_mod)

    driver = connect_with_retries(
        "https://cloud.typedb.com:443",
        "admin",
        "password",
        tls=None,
        retries=1,
        sleep_s=0,
    )
    driver.close()

    assert captured["address"] == "https://cloud.typedb.com:443"
    assert captured["is_tls_enabled"] is True
