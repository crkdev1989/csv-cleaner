"""
CLI: run cleaner from command line. Supports config file, directory of configs, or preset + input file.
"""

import argparse
import json
import sys
from pathlib import Path

from cleaner.engine import run_cleaner, run_cleaner_batch


def _get_presets_dir() -> Path:
    """Project root / presets (same parent as cleaner package)."""
    return Path(__file__).resolve().parent.parent / "presets"


def _list_presets() -> list[str]:
    """Return sorted list of preset names (stem of each .json in presets/)."""
    presets_dir = _get_presets_dir()
    if not presets_dir.is_dir():
        return []
    return sorted(p.stem for p in presets_dir.glob("*.json"))


def load_preset(name: str) -> dict:
    """
    Load preset by name from presets/<name>.json.
    Raises FileNotFoundError with message 'Preset not found: <name>' if missing.
    """
    presets_dir = _get_presets_dir()
    if not name.endswith(".json"):
        name = f"{name}.json"
    path = presets_dir / name
    if not path.is_file():
        raise FileNotFoundError(f"Preset not found: {name.removesuffix('.json')}")
    return json.loads(path.read_text(encoding="utf-8"))


def _config_from_preset(preset: dict, input_path: str | Path) -> dict:
    """Build a full config dict from a preset and the input file path."""
    input_path = Path(input_path).resolve()
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    out_dir = input_path.parent
    return {
        "input": {"path": str(input_path), "format": None},
        "output": {"path": str(out_dir / "cleaned.csv"), "format": "csv"},
        "report": {"path": str(out_dir / "report.json")},
        "pipeline": preset.get("pipeline", preset.get("modules", [])),
        "chunk_size": preset.get("chunk_size"),
    }


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
        description="Run CSV Cleaner with a config file, directory of configs, or a preset + input file.",
    )
    parser.add_argument(
        "path",
        nargs="?",
        type=str,
        help="Config path (config.json or directory) or input file when using --preset",
    )
    parser.add_argument(
        "-q", "--quiet",
        action="store_true",
        help="Only print errors",
    )
    parser.add_argument(
        "--preset",
        type=str,
        metavar="NAME",
        help="Use a preset pipeline (e.g. leads, crm, scrape). Then path is the input file.",
    )
    parser.add_argument(
        "--list-presets",
        action="store_true",
        help="List available preset names and exit",
    )
    args = parser.parse_args()

    if args.list_presets:
        presets = _list_presets()
        if not presets:
            print("No presets found.", file=sys.stderr)
            sys.exit(0)
        print("Available presets:")
        for name in presets:
            print(f"  {name}")
        sys.exit(0)

    if not args.path:
        parser.error("path is required (config file/directory, or input file with --preset)")

    path = Path(args.path)
    if not path.exists():
        print(f"Error: path not found: {path}", file=sys.stderr)
        sys.exit(1)

    if args.preset:
        try:
            preset = load_preset(args.preset)
        except FileNotFoundError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
        config = _config_from_preset(preset, path)
        try:
            report = run_cleaner(config_dict=config)
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
        results = [(args.path, report)]
    else:
        try:
            results = run_cleaner_batch(path)
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
