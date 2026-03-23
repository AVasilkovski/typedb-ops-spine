from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest

from typedb_ops_spine.readiness import (
    TypeDBConfigError,
    connect_with_retries,
    infer_tls_enabled,
    normalize_typedb_address,
    resolve_connection_address,
    resolve_connection_config,
    validate_connection_config,
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

    with pytest.raises(RuntimeError, match="TypeDB not ready"):
        connect_with_retries("localhost:1729", "admin", "password", retries=2, sleep_s=0)

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


def test_validate_connection_config_accepts_local_core():
    address, tls, ca_path = validate_connection_config("localhost:1729")

    assert address == "localhost:1729"
    assert tls is False
    assert ca_path is None


def test_validate_connection_config_accepts_https_tls(tmp_path: Path):
    ca_file = tmp_path / "root-ca.pem"
    ca_file.write_text("test-ca", encoding="utf-8")

    address, tls, ca_path = validate_connection_config(
        "https://cloud.typedb.com:443",
        tls=None,
        ca_path=str(ca_file),
    )

    assert address == "https://cloud.typedb.com:443"
    assert tls is True
    assert ca_path == str(ca_file)


def test_resolve_connection_config_prefers_explicit_address():
    address, tls, ca_path = resolve_connection_config(
        "https://cloud.typedb.com",
        "ignored-host",
        443,
        tls=None,
        ca_path=None,
    )

    assert address == "https://cloud.typedb.com:443"
    assert tls is True
    assert ca_path is None


def test_resolve_connection_config_defaults_to_local_non_tls():
    address, tls, ca_path = resolve_connection_config(None, "localhost", 1729)

    assert address == "localhost:1729"
    assert tls is False
    assert ca_path is None


def test_validate_connection_config_rejects_tls_enabled_on_localhost():
    with pytest.raises(TypeDBConfigError, match="TLS is enabled but the resolved TypeDB address is not HTTPS"):
        validate_connection_config("localhost:1729", tls=True)


def test_validate_connection_config_rejects_tls_disabled_for_https():
    with pytest.raises(TypeDBConfigError, match="TLS is disabled but the resolved TypeDB address is HTTPS"):
        validate_connection_config("https://cloud.typedb.com:443", tls=False)


def test_validate_connection_config_rejects_http_scheme():
    with pytest.raises(TypeDBConfigError, match="Unsupported TypeDB address scheme 'http'"):
        validate_connection_config("http://cloud.typedb.com:443")


def test_validate_connection_config_rejects_ca_path_without_tls(tmp_path: Path):
    ca_file = tmp_path / "root-ca.pem"
    ca_file.write_text("test-ca", encoding="utf-8")

    with pytest.raises(TypeDBConfigError, match="TLS root CA path was provided but TLS is disabled"):
        validate_connection_config("localhost:1729", tls=False, ca_path=str(ca_file))


def test_validate_connection_config_rejects_missing_ca_file(tmp_path: Path):
    missing_ca = tmp_path / "missing-ca.pem"

    with pytest.raises(TypeDBConfigError, match="Configured TLS root CA path does not exist"):
        validate_connection_config(
            "https://cloud.typedb.com:443",
            tls=True,
            ca_path=str(missing_ca),
        )


def test_resolve_connection_config_mentions_host_port_fallback():
    with pytest.raises(TypeDBConfigError, match="No explicit --address/TYPEDB_ADDRESS was provided"):
        resolve_connection_config(None, "localhost", 1729, tls=True)


def test_resolve_connection_config_rejects_invalid_port():
    with pytest.raises(TypeDBConfigError, match="Invalid TypeDB port 'not-a-port'"):
        resolve_connection_config(None, "localhost", "not-a-port")


def test_connect_with_retries_does_not_retry_on_config_error(monkeypatch):
    driver_calls: list[str] = []
    sleep_calls: list[float] = []

    class _Credentials:
        def __init__(self, *_args, **_kwargs):
            pass

    class _DriverOptions:
        def __init__(self, **_kwargs):
            pass

    class _TypeDB:
        @staticmethod
        def driver(*_args, **_kwargs):
            driver_calls.append("driver")
            raise AssertionError("driver() should not be called for config errors")

    fake_mod = types.SimpleNamespace(
        Credentials=_Credentials,
        DriverOptions=_DriverOptions,
        TypeDB=_TypeDB,
    )
    monkeypatch.setitem(sys.modules, "typedb.driver", fake_mod)
    monkeypatch.setattr("time.sleep", lambda seconds: sleep_calls.append(seconds))

    with pytest.raises(TypeDBConfigError, match="TLS is enabled but the resolved TypeDB address is not HTTPS"):
        connect_with_retries("localhost:1729", "admin", "password", tls=True, retries=3, sleep_s=5.0)

    assert driver_calls == []
    assert sleep_calls == []


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
