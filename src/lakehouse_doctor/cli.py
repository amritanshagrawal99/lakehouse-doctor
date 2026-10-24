"""`lakehouse-doctor scan PATH`, `lakehouse-doctor fix PATH ...` and `lakehouse-doctor fleet ROOT`"""
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


def cmd_fleet(a: argparse.Namespace) -> int:
    from . import fleet
    cfg = _config(a)
    if a.catalog:
        from pyiceberg.catalog import load_catalog
        props = dict(kv.split("=", 1) for kv in a.catalog_prop) if a.catalog_prop else {}
        rows = fleet.scan_iceberg_fleet(load_catalog(a.catalog, **props), a.table, cfg)
    else:
        rows = fleet.scan_delta_fleet(a.table, cfg)
    text = fleet.to_json(rows) if a.format == "json" else fleet.to_markdown(rows, a.table)
    if a.output:
        Path(a.output).write_text(text, encoding="utf-8")
    else:
        sys.stdout.buffer.write(text.encode("utf-8"))
    if a.fail_under is not None and any(r.score is None or r.score < a.fail_under for r in rows):
        return 1
    return 0


def cmd_keys(a: argparse.Namespace) -> int:
    from .checks import files_per_lookup
    from .keys import analyse, recommend, split_queries
    rep = analyse(split_queries(Path(a.log).read_text(encoding="utf-8")), a.dialect)
    snap = load(a.table_path, _storage(a)) if a.table_path else None
    parts = snap.partition_columns if snap else []
    print(f"{rep.parsed}/{rep.queries} queries parsed")
    ranked = rep.ranked(a.table)
    if not ranked:
        print(f"no filters on `{a.table}` found in the log")
        return 0
    print()
    print(f"| column | point | range | join | score |{' files/lookup now |' if snap else ''}")
    print(f"|---|---|---|---|---|{'---|' if snap else ''}")
    for col, u in ranked[: a.top_n]:
        note = " (partition)" if col in parts else ""
        now = ""
        if snap:
            fpl = files_per_lookup(snap.files, col)
            now = f" {fpl:.1f} |" if fpl is not None else " - |"
        print(f"| `{col}`{note} | {u.point} | {u.range} | {u.join} | {u.score} |{now}")
    rec = recommend(rep, a.table, parts)
    if rec:
        print()
        print(f"Recommended clustering keys: {', '.join(rec)}")
        print(f"  OPTIMIZE <table> ZORDER BY ({', '.join(rec)})   -- or CLUSTER BY ({', '.join(rec)}) for liquid clustering")
        if a.table_path:
            print(f"  lakehouse-doctor scan {a.table_path} " + " ".join(f"--key {c}" for c in rec))
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
    p = argparse.ArgumentParser(prog="lakehouse-doctor", description="Health checks and fixes for Delta Lake and Iceberg tables.")
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

    fl = sub.add_parser("fleet", help="scan every table under a directory (Delta) or a catalog namespace (Iceberg, with --catalog)")
    common(fl)
    fl.add_argument("--format", choices=["markdown", "json"], default="markdown")
    fl.add_argument("-o", "--output")
    fl.add_argument("--fail-under", type=int, help="exit 1 when any table scores below this")
    fl.set_defaults(func=cmd_fleet)

    k = sub.add_parser("keys", help="recommend clustering keys from a query log")
    k.add_argument("log", help="file of SQL queries separated by semicolons (e.g. a query-history export)")
    k.add_argument("--table", required=True, help="table name as it appears in the queries (last part, e.g. events)")
    k.add_argument("--table-path", help="optional Delta table path: also measure how well each candidate is clustered today")
    k.add_argument("--dialect", help="sqlglot dialect of the log (databricks, snowflake, spark, ...)")
    k.add_argument("--top-n", type=int, default=10)
    k.add_argument("--storage-option", action="append", metavar="KEY=VALUE")
    k.set_defaults(func=cmd_keys)

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
