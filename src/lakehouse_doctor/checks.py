"""Health checks. Each returns zero or more Findings with the evidence and the exact fix."""
from __future__ import annotations

import bisect
import itertools
import math
import statistics
from dataclasses import dataclass, field
from enum import Enum

from deltalake import DeltaTable

from .snapshot import DataFile, Snapshot

MB = 1024 * 1024


class Severity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


PENALTY = {Severity.INFO: 0, Severity.WARNING: 10, Severity.CRITICAL: 25}


@dataclass
class Config:
    target_file_size: int = 128 * MB
    small_file_ratio: float = 0.25    # a file is "small" below this fraction of the target
    min_files_for_compaction: int = 8
    max_partitions_healthy: int = 2000
    skew_ratio: float = 10.0          # largest partition / median partition
    keys: list[str] = field(default_factory=list)  # columns queries filter on (clustering check)
    max_files_per_lookup: float = 2.0
    checkpoint_interval: int = 100
    vacuum_retention_hours: int = 168
    min_reclaimable_bytes: int = 1 * MB


@dataclass
class Fix:
    sql: str        # Databricks / Spark SQL
    python: str     # delta-rs equivalent
    action: str     # key understood by `lakehouse-doctor fix`


@dataclass
class Finding:
    code: str
    severity: Severity
    title: str
    detail: str
    metrics: dict = field(default_factory=dict)
    fixes: list[Fix] = field(default_factory=list)


def _ident(snap: Snapshot) -> str:
    return f"delta.`{snap.uri.rstrip('/')}`"


def _fmt_bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024 or unit == "TB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


# ------------------------------------------------------------------ LH001

def small_files(snap: Snapshot, cfg: Config) -> list[Finding]:
    n = len(snap.files)
    if n < cfg.min_files_for_compaction:
        return []
    threshold = cfg.target_file_size * cfg.small_file_ratio
    small = [f for f in snap.files if f.size < threshold]
    ideal = sum(max(1, math.ceil(sum(f.size for f in g) / cfg.target_file_size)) for g in snap.partitions().values())
    if len(small) < cfg.min_files_for_compaction or n <= 2 * ideal:
        return []
    sev = Severity.CRITICAL if n > 20 * ideal else Severity.WARNING
    return [Finding(
        "LH001", sev, "Too many small files",
        f"{len(small)} of {n} files are under {_fmt_bytes(threshold)} (average {_fmt_bytes(snap.total_bytes / n)}). "
        f"Compaction would leave about {ideal} file(s). Every extra file costs a metadata read, a task and an object-store request.",
        {"files": n, "small_files": len(small), "avg_file_bytes": snap.total_bytes // n, "files_after_compaction": ideal},
        [Fix(f"OPTIMIZE {_ident(snap)}", f"DeltaTable(uri).optimize.compact(target_size={cfg.target_file_size})", "compact")],
    )]


# ------------------------------------------------------------------ LH002 / LH003

def partitioning(snap: Snapshot, cfg: Config) -> list[Finding]:
    if not snap.partition_columns:
        return []
    groups = snap.partitions()
    sizes = sorted(sum(f.size for f in g) for g in groups.values())
    out = []
    avg = statistics.mean(sizes) if sizes else 0
    if len(groups) > cfg.max_partitions_healthy or (len(groups) >= 10 and avg < cfg.target_file_size * cfg.small_file_ratio):
        cols = ", ".join(snap.partition_columns)
        out.append(Finding(
            "LH002", Severity.WARNING if len(groups) <= 10 * cfg.max_partitions_healthy else Severity.CRITICAL,
            "Over-partitioned table",
            f"{len(groups)} partitions on ({cols}) averaging {_fmt_bytes(avg)}, far below one target-size file each. "
            "Partitions this small cannot be compacted into efficient files. Partition by a coarser column, "
            "or drop partitioning and use liquid clustering.",
            {"partitions": len(groups), "avg_partition_bytes": int(avg)},
            [Fix(f"-- recreate with: CREATE TABLE ... CLUSTER BY ({cols})  (liquid clustering)",
                 "# rewrite the table without partition_by, then z_order on the former partition columns", "none")],
        ))
    # Skew is measured in rows: tiny partitions are mostly Parquet overhead, which hides skew in bytes.
    use_rows = all(f.records is not None for f in snap.files)
    weight = {k: sum((f.records if use_rows else f.size) for f in g) for k, g in groups.items()}
    w = sorted(weight.values())
    if len(w) >= 3:
        median = statistics.median(w)
        if median > 0 and w[-1] / median >= cfg.skew_ratio:
            biggest = max(weight, key=weight.get)
            label = ", ".join(f"{c}={v}" for c, v in zip(snap.partition_columns, biggest, strict=True))
            unit = "rows" if use_rows else "bytes"
            share = 100 * w[-1] / sum(w)
            out.append(Finding(
                "LH003", Severity.WARNING, "Skewed partitions",
                f"Partition {label} holds {w[-1]:,} {unit} ({share:.0f}% of the table), {w[-1] / median:.0f}x the median "
                f"partition. Per-partition work (writes, OPTIMIZE, MERGE) will be bottlenecked on it.",
                {"largest_partition": label, "unit": unit, "largest": w[-1], "median": median,
                 "ratio": round(w[-1] / median, 1)},
            ))
    return out


# ------------------------------------------------------------------ LH004

def missing_stats(snap: Snapshot, cfg: Config) -> list[Finding]:
    if not snap.files:
        return []
    data_cols = [c for c in snap.columns if c not in snap.partition_columns]
    no_stats = [f for f in snap.files if f.records is None or not any(f.min.get(c) is not None or f.max.get(c) is not None
                                                                     for c in data_cols)]
    # Files where every value is null legitimately have no min/max; only count files that have rows.
    no_stats = [f for f in no_stats if f.records is None or sum(f.null_count.values()) < (f.records or 0) * max(1, len(data_cols))]
    if not no_stats:
        return []
    pct = 100 * len(no_stats) / len(snap.files)
    return [Finding(
        "LH004", Severity.WARNING if pct >= 20 else Severity.INFO, "Files without column statistics",
        f"{len(no_stats)} of {len(snap.files)} files ({pct:.0f}%) carry no min/max statistics, so queries cannot skip them. "
        "Rewriting them (OPTIMIZE) collects statistics.",
        {"files_without_stats": len(no_stats), "percent": round(pct, 1)},
        [Fix(f"OPTIMIZE {_ident(snap)}", "DeltaTable(uri).optimize.compact()", "compact")],
    )]


# ------------------------------------------------------------------ LH005

def files_per_lookup(files: list[DataFile], column: str) -> float | None:
    """Average number of files a point lookup on `column` must open, judged from per-file min/max.

    The value range is cut at every file boundary; each piece is weighted by its width (for numbers and dates)
    and counts the files whose range covers it. Pieces no file covers hold no data and are ignored.
    1.0 means perfectly clustered; len(files) means every lookup reads every file.
    """
    ranges = [(f.min.get(column), f.max.get(column)) for f in files]
    ranges = [(_num(lo), _num(hi)) for lo, hi in ranges if lo is not None and hi is not None]
    if len(ranges) < 2:
        return None
    try:
        starts = sorted(lo for lo, _ in ranges)
        ends = sorted(hi for _, hi in ranges)
        points = sorted({v for r in ranges for v in r})
    except TypeError:
        return None
    weighted = isinstance(points[0], (int, float))
    total_w = total = 0.0
    for a, b in itertools.pairwise(points):
        covering = bisect.bisect_right(starts, a) - bisect.bisect_right(ends, a)
        if covering <= 0:
            continue
        w = float(b - a) if weighted else 1.0
        total += covering * w
        total_w += w
    if total_w == 0:
        # Every file holds a single value (min == max): count files per distinct value instead.
        counts = [bisect.bisect_right(starts, p) - bisect.bisect_left(ends, p) for p in points]
        return statistics.mean(counts)
    return total / total_w


def _num(v):
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, (int, float)):
        return v
    if hasattr(v, "timestamp"):
        return v.timestamp()
    if hasattr(v, "toordinal"):
        return v.toordinal()
    return v


def clustering(snap: Snapshot, cfg: Config) -> list[Finding]:
    out = []
    keys = cfg.keys or []
    for key in keys:
        if key not in snap.columns:
            out.append(Finding("LH005", Severity.INFO, f"Key column `{key}` not in table", "Check the --key argument."))
            continue
        if key in snap.partition_columns:
            continue
        # Measured inside each partition: a lookup without a partition filter must touch every partition
        # anyway, so only overlap within a partition is fixable by clustering.
        groups = [g for g in snap.partitions().values() if len(g) >= 2]
        scores = [(files_per_lookup(g, key), len(g)) for g in groups]
        scores = [(v, n) for v, n in scores if v is not None]
        if not scores:
            continue
        per_lookup = sum(v * n for v, n in scores) / sum(n for _, n in scores)
        if per_lookup <= cfg.max_files_per_lookup:
            continue
        n = sum(n for _, n in scores) / len(scores)
        where = " within a partition" if snap.partition_columns else ""
        sev = Severity.CRITICAL if per_lookup >= 0.5 * n and n >= 20 else Severity.WARNING
        out.append(Finding(
            "LH005", sev, f"Poor data skipping on `{key}`",
            f"A filter on `{key}` = value has to open {per_lookup:.1f} of {n:.0f} files{where} on average, because the files' "
            f"min/max ranges overlap. Z-ordering or clustering by `{key}` makes each value land in a few files.",
            {"column": key, "avg_files_per_lookup": round(per_lookup, 2), "files": round(n)},
            [Fix(f"OPTIMIZE {_ident(snap)} ZORDER BY ({key})", f"DeltaTable(uri).optimize.z_order([{key!r}])", f"zorder:{key}")],
        ))
    return out


# ------------------------------------------------------------------ LH006

def log_health(snap: Snapshot, cfg: Config) -> list[Finding]:
    if snap.log is None or snap.log.commits_since_checkpoint <= cfg.checkpoint_interval:
        return []
    return [Finding(
        "LH006", Severity.WARNING, "Transaction log needs a checkpoint",
        f"{snap.log.commits_since_checkpoint} commits since the last checkpoint "
        f"({'none yet' if snap.log.last_checkpoint_version is None else 'v' + str(snap.log.last_checkpoint_version)}). "
        "Every reader replays these JSON commits to build the table state, so table load time grows with each write.",
        {"commits_since_checkpoint": snap.log.commits_since_checkpoint, "commit_files": snap.log.commit_files},
        [Fix("-- Spark/Databricks checkpoint automatically every 10 commits (delta.checkpointInterval)",
             "DeltaTable(uri).create_checkpoint()", "checkpoint")],
    )]


# ------------------------------------------------------------------ LH007

def vacuumable(snap: Snapshot, cfg: Config) -> list[Finding]:
    try:
        dt = DeltaTable(snap.uri, storage_options=snap.storage_options)
        paths = dt.vacuum(retention_hours=cfg.vacuum_retention_hours, dry_run=True,
                          enforce_retention_duration=cfg.vacuum_retention_hours >= 168)
    except Exception:  # noqa: BLE001 - vacuum planning is best-effort (permissions, remote stores)
        return []
    size = None
    if snap.local_path is not None:
        # delta-rs keeps listing tombstoned files after they are deleted; only count files still on disk.
        paths = [p for p in paths if (snap.local_path / p).exists()]
        size = sum((snap.local_path / p).stat().st_size for p in paths)
        if paths and size < cfg.min_reclaimable_bytes:
            return []
    if not paths:
        return []
    detail = f"{len(paths)} files are no longer referenced by the table and are older than {cfg.vacuum_retention_hours}h"
    detail += f" ({_fmt_bytes(size)} of storage)." if size is not None else "."
    return [Finding(
        "LH007", Severity.INFO if (size or 0) < 1024 * MB else Severity.WARNING, "Unreferenced files can be vacuumed",
        detail + " VACUUM deletes them; time travel to versions older than the retention window will stop working.",
        {"files": len(paths), "bytes": size},
        [Fix(f"VACUUM {_ident(snap)} RETAIN {cfg.vacuum_retention_hours} HOURS",
             f"DeltaTable(uri).vacuum(retention_hours={cfg.vacuum_retention_hours}, dry_run=False)", "vacuum")],
    )]


ALL_CHECKS = [small_files, partitioning, missing_stats, clustering, log_health, vacuumable]


def run_checks(snap: Snapshot, cfg: Config) -> list[Finding]:
    findings = [f for check in ALL_CHECKS for f in check(snap, cfg)]
    order = {Severity.CRITICAL: 0, Severity.WARNING: 1, Severity.INFO: 2}
    return sorted(findings, key=lambda f: (order[f.severity], f.code))


def health_score(findings: list[Finding]) -> int:
    return max(0, 100 - sum(PENALTY[f.severity] for f in findings))
