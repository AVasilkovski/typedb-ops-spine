from __future__ import annotations

import sys
import types

from typedb_ops_spine.cli import min_write_probe_cli


def _install_fake_typedb(monkeypatch) -> None:
    typedb_mod = types.ModuleType("typedb")
    typedb_mod.__version__ = "test-driver"

    typedb_driver_mod = types.ModuleType("typedb.driver")
    typedb_driver_mod.Credentials = object
    typedb_driver_mod.DriverOptions = object
    typedb_driver_mod.TransactionType = types.SimpleNamespace(READ="READ", WRITE="WRITE")
    typedb_driver_mod.TypeDB = object

    monkeypatch.setitem(sys.modules, "typedb", typedb_mod)
    monkeypatch.setitem(sys.modules, "typedb.driver", typedb_driver_mod)


def test_min_write_probe_cli_uses_standard_error_prefix_for_config_errors(
    monkeypatch, capsys
):
    _install_fake_typedb(monkeypatch)
    monkeypatch.setenv("TYPEDB_TLS", "true")
    monkeypatch.setattr(
        "sys.argv",
        [
            "ops-min-write-probe",
            "--address",
            "localhost:1729",
        ],
    )

    rc = min_write_probe_cli.main()

    captured = capsys.readouterr()
    assert rc == 1
    assert "[ops-min-write-probe] ERROR:" in captured.err
    assert "TLS is enabled but the resolved TypeDB address is not HTTPS" in captured.err
