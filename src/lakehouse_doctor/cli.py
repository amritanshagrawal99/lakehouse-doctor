"""`lakehouse-doctor scan PATH` and `lakehouse-doctor fix PATH ...`"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

from . import __version__
from .checks import Config, Severity, health_score, run_checks
from .fix import apply, apply_iceberg
from .report import to_json, to_markdown
from .snapshot import load

_UNITS = {"": 1, "b": 1, "kb": 1024, "mb": 1024**2, "gb": 1024**3}


def parse_size(text: str) -> int:
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([kmg]?b?)\s*", text.lower())
    if not m:
        raise argparse.ArgumentTypeError(f"invalid size {text!r} (examples: 128MB, 1GB)")
    return int(float(m.group(1)) * _UNITS[m.group(2)])


def _config(a: argparse.Namespace) -> Config:
    return Config(target_file_size=a.target_file_size, keys=[k for k in (a.key or [])],
                  vacuum_retention_hours=a.retention_hours)


def _storage(a: argparse.Namespace) -> dict[str, str] | None:
    return dict(kv.split("=", 1) for kv in a.storage_option) if a.storage_option else None


def _iceberg_table(a: argparse.Namespace):
    from .iceberg import load_catalog_table
    props = dict(kv.split("=", 1) for kv in a.catalog_prop) if a.catalog_prop else None
    return load_catalog_table(a.table, a.catalog, props)


def _load(a: argparse.Namespace):
    if a.catalog:
        from .iceberg import load_iceberg
        return load_iceberg(_iceberg_table(a), a.table)
    return load(a.table, _storage(a))


def cmd_scan(a: argparse.Namespace) -> int:
    snap = _load(a)
    findings = run_checks(snap, _config(a))
    text = to_json(snap, findings) if a.format == "json" else to_markdown(snap, findings)
    if a.output:
        Path(a.output).write_text(text, encoding="utf-8")
    else:
        sys.stdout.buffer.write(text.encode("utf-8"))
    if a.fail_under is not None and health_score(findings) < a.fail_under:
        return 1
    return 0


def cmd_fix(a: argparse.Namespace) -> int:
    cfg = _config(a)
    actions: list[str] = []
    if a.auto:
        snap = _load(a)
        for f in run_checks(snap, cfg):
            if f.severity is Severity.INFO:
                continue
            for fx in f.fixes:
                if fx.action not in ("none", "vacuum") and fx.action not in actions:
                    actions.append(fx.action)
    if a.catalog:
        if a.expire:
            actions.append("expire")
        if not actions:
            print("Nothing to do.")
            return 0
        for r in apply_iceberg(_iceberg_table(a), actions, cfg, dry_run=a.dry_run):
            print(f"{r.action}: {_summary(r.detail)}")
        return 0
    if a.compact:
        actions.append("compact")
    if a.zorder:
        actions.append("zorder:" + a.zorder)
    if a.checkpoint:
        actions.append("checkpoint")
    if a.vacuum:
        actions.append("vacuum")
    if not actions:
        print("Nothing to do.")
        return 0
    # z-order rewrites every file, so a separate compaction before it is wasted work.
    if any(x.startswith("zorder:") for x in actions) and "compact" in actions:
        actions.remove("compact")
    for r in apply(a.table, actions, cfg, dry_run=a.dry_run, storage_options=_storage(a)):
        print(f"{r.action}: {_summary(r.detail)}")
    return 0


def _summary(detail: dict) -> str:
    keys = ("numFilesAdded", "numFilesRemoved", "files", "snapshots", "version", "planned", "dry_run", "skipped")
    return ", ".join(f"{k}={detail[k]}" for k in keys if k in detail) or str(detail)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="lakehouse-doctor", description="Health checks and fixes for Delta Lake tables.")
    p.add_argument("--version", action="version", version=f"lakehouse-doctor {__version__}")
    sub = p.add_subparsers(dest="cmd", required=True)

    def common(sp):
        sp.add_argument("table", help="Delta table path/URI (local, s3://, abfss://, gs://), or an Iceberg identifier with --catalog")
        sp.add_argument("--catalog", help="pyiceberg catalog name: treat TABLE as an Iceberg identifier such as db.events")
        sp.add_argument("--catalog-prop", action="append", metavar="KEY=VALUE", help="pyiceberg catalog property (uri, warehouse, ...)")
        sp.add_argument("--key", action="append", help="column that queries filter on (repeatable); enables the clustering check")
        sp.add_argument("--target-file-size", type=parse_size, default=parse_size("128MB"))
        sp.add_argument("--retention-hours", type=int, default=168)
        sp.add_argument("--storage-option", action="append", metavar="KEY=VALUE", help="passed to delta-rs (cloud credentials)")

    s = sub.add_parser("scan", help="report table health")
    common(s)
    s.add_argument("--format", choices=["markdown", "json"], default="markdown")
    s.add_argument("-o", "--output")
    s.add_argument("--fail-under", type=int, help="exit 1 when the health score is below this (for CI)")
    s.set_defaults(func=cmd_scan)

    f = sub.add_parser("fix", help="apply fixes")
    common(f)
    f.add_argument("--auto", action="store_true", help="apply every non-destructive fix the scan recommends")
    f.add_argument("--compact", action="store_true")
    f.add_argument("--zorder", metavar="COLS", help="comma-separated columns")
    f.add_argument("--checkpoint", action="store_true")
    f.add_argument("--vacuum", action="store_true", help="delete unreferenced files older than --retention-hours")
    f.add_argument("--expire", action="store_true", help="Iceberg: expire snapshots older than --retention-hours")
    f.add_argument("--dry-run", action="store_true")
    f.set_defaults(func=cmd_fix)

    a = p.parse_args(argv)
    return a.func(a)


if __name__ == "__main__":
    sys.exit(main())
