"""
CLI: run cleaner from command line. Supports single config or directory of configs.
"""

import argparse
import sys
from pathlib import Path

from cleaner.engine import run_cleaner, run_cleaner_batch


def _write_summary_file(report) -> str | None:
    """Write human-readable summary to <output_stem>_summary.txt. Returns path or None."""
    if not report.output_path:
        return None
    out_dir = Path(report.output_path).parent
    stem = Path(report.input_path).stem if report.input_path else Path(report.output_path).stem.replace("_cleaned", "")
    summary_path = out_dir / f"{stem}_summary.txt"
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(report.format_summary())
    return str(summary_path)


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

    for _config_path, report in results:
        rows_removed = report.rows_loaded - report.rows_output
        print("CSV Cleaner")
        print("-----------")
        print()
        print(f"Rows Loaded:  {report.rows_loaded}")
        print(f"Rows Output:  {report.rows_output}")
        print(f"Rows Removed:  {rows_removed}")
        print()
        print("Modules Applied:")
        for mod in report.modules_executed:
            print(f"  • {mod}")
        print()
        if report.output_path:
            print(f"Cleaned file written: {report.output_path}")
        if report.report_path:
            print(f"Report written: {report.report_path}")
        summary_path = _write_summary_file(report)
        if summary_path:
            print(f"Summary written: {summary_path}")
    sys.exit(0)


if __name__ == "__main__":
    main()
