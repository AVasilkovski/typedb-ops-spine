"""
TSV extractor for TypeDB diagnostic JSONL files.

Produces stable, human-readable columns suitable for CI log review.
Resilient to missing fields — every column defaults to empty/zero.
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

# Stable column order — do not reorder without bumping extractor version.
TSV_COLUMNS = [
    "STAGE", "DB", "ADDR", "ACTION", "KIND", "ROWS",
    "MODE", "OK", "B_ATT", "B_OK", "ERR_CODE", "ERR_MSG", "QUERY_HEAD",
]

# Default stage pattern: match canary, probe, init, tenant, raw_seed stages
DEFAULT_STAGE_PATTERN = re.compile(r"^(canary|tenant|init|probe|raw_seed)")


def extract_tsv(
    jsonl_path: str | Path,
    *,
    stage_pattern: re.Pattern[str] | None = None,
    output: object = None,
) -> None:
    """Extract TSV from a JSONL diagnostics file.

    Args:
        jsonl_path: Path to the JSONL file.
        stage_pattern: Regex to filter stages. None = use default pattern.
        output: File-like object to write to. Defaults to sys.stdout.
    """
    path = Path(jsonl_path)
    out = output or sys.stdout
    pat = stage_pattern if stage_pattern is not None else DEFAULT_STAGE_PATTERN

    if not path.exists():
        print(f"No diagnostics file: {path}", file=sys.stderr)
        return

    # Print header
    print("\t".join(TSV_COLUMNS), file=out)

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except (json.JSONDecodeError, ValueError):
                continue

            stage = obj.get("stage", obj.get("component", ""))
            if not pat.search(str(stage)):
                continue

            row = [
                str(stage),
                str(obj.get("db_name", obj.get("db", ""))),
                str(obj.get("address", "")),
                str(obj.get("action", "")),
                str(obj.get("answer_kind", "")),
                str(obj.get("row_count", 0)),
                str(obj.get("query_mode", "-")),
                str(obj.get("is_ok", "-")),
                str(obj.get("as_ok_attempted", obj.get("as_ok_called", "-"))),
                str(obj.get("as_ok_succeeded", "-")),
                str(obj.get("error_code", "")),
                str((obj.get("error_message", "") or "")[:100]),
                str(obj.get("query_head", "")),
            ]
            print("\t".join(row), file=out)


def extract_tsv_from_default() -> None:
    """Extract TSV from the default diagnostics path (for CI integration)."""
    artifacts_dir = os.getenv("CI_ARTIFACTS_DIR", "ci_artifacts")
    default_path = Path(artifacts_dir) / "typedb_diag.jsonl"
    extract_tsv(default_path)
