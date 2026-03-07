"""
CLI: run cleaner from command line. Supports single config or directory of configs.
"""

import argparse
import sys
from pathlib import Path

from cleaner.engine import run_cleaner, run_cleaner_batch


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run CSV Cleaner with a config file or directory of configs.",
    )
    parser.add_argument(
        "config",
        type=str,
        help="Path to config.json or directory containing .json configs",
    )
    parser.add_argument(
        "-q", "--quiet",
        action="store_true",
        help="Only print errors",
    )
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Error: path not found: {config_path}", file=sys.stderr)
        sys.exit(1)

    try:
        results = run_cleaner_batch(config_path)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    if args.quiet:
        sys.exit(0)

    for path, report in results:
        d = report.to_dict()
        print(f"Config: {path}")
        print(f"  rows_loaded={d['rows_loaded']} rows_output={d['rows_output']} "
              f"time={d['processing_time_seconds']}s modules={d['modules_executed']}")
    sys.exit(0)


if __name__ == "__main__":
    main()
