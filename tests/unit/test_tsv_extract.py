from __future__ import annotations

import json
from pathlib import Path

from typedb_ops_spine.tsv_extract import extract_tsv


def test_extract_tsv_reads_error_message_and_legacy_field(tmp_path: Path):
    diag_path = tmp_path / "diag.jsonl"
    out_path = tmp_path / "out.tsv"

    records = [
        {
            "stage": "canary_new",
            "db_name": "db1",
            "address": "localhost:1729",
            "action": "execute",
            "answer_kind": "exception",
            "row_count": 0,
            "query_mode": "write",
            "is_ok": False,
            "as_ok_attempted": False,
            "as_ok_succeeded": False,
            "error_code": "E1",
            "error_message": "new field",
            "query_head": "insert ...",
        },
        {
            "stage": "canary_old",
            "db_name": "db1",
            "address": "localhost:1729",
            "action": "execute",
            "answer_kind": "exception",
            "row_count": 0,
            "query_mode": "write",
            "is_ok": False,
            "as_ok_attempted": False,
            "as_ok_succeeded": False,
            "error_code": "E2",
            "error_message_trunc": "legacy field",
            "query_head": "insert ...",
        },
    ]

    with diag_path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")

    with out_path.open("w", encoding="utf-8") as out:
        extract_tsv(diag_path, output=out)

    lines = out_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 3
    assert lines[1].split("\t")[11] == "new field"
    assert lines[2].split("\t")[11] == "legacy field"
