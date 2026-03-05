"""
CLI entry point: ops-tsv-extract

Extracts stable TSV columns from TypeDB diagnostic JSONL files.
"""

from __future__ import annotations

import argparse
import os
import re
import sys


def main() -> int:
    p = argparse.ArgumentParser(
        prog="ops-tsv-extract",
        description="Extract TSV from TypeDB diagnostic JSONL files.",
    )
    p.add_argument(
        "--input",
        default=None,
        help="Path to JSONL file. Defaults to ci_artifacts/typedb_diag.jsonl.",
    )
    p.add_argument(
        "--pattern",
        default=None,
        help="Regex pattern to filter stages. Default: canary|probe|init|tenant|raw_seed.",
    )
    args = p.parse_args()

    from typedb_ops_spine.tsv_extract import (
        DEFAULT_STAGE_PATTERN,
        extract_tsv,
    )

    input_path = args.input
    if input_path is None:
        artifacts_dir = os.getenv("CI_ARTIFACTS_DIR", "ci_artifacts")
        input_path = os.path.join(artifacts_dir, "typedb_diag.jsonl")

    stage_pattern = DEFAULT_STAGE_PATTERN
    if args.pattern:
        stage_pattern = re.compile(args.pattern)

    extract_tsv(input_path, stage_pattern=stage_pattern)
    return 0


if __name__ == "__main__":
    sys.exit(main())
