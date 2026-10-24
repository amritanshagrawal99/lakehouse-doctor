"""Apply fixes with delta-rs. Destructive actions (vacuum) only run when asked for explicitly."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from deltalake import DeltaTable

from .checks import Config


@dataclass
class FixResult:
    action: str
    detail: dict


def apply(uri: str, actions: list[str], cfg: Config, dry_run: bool = False,
          storage_options: dict[str, str] | None = None) -> list[FixResult]:
    results: list[FixResult] = []
    for action in actions:
        dt = DeltaTable(uri, storage_options=storage_options)  # reload: each action creates a new version
        if action == "compact":
            detail = {"planned": True} if dry_run else dt.optimize.compact(target_size=cfg.target_file_size)
        elif action.startswith("zorder:"):
            cols = action.split(":", 1)[1].split(",")
            detail = {"planned": True, "columns": cols} if dry_run else dt.optimize.z_order(cols, target_size=cfg.target_file_size)
        elif action == "checkpoint":
            if not dry_run:
                dt.create_checkpoint()
            detail = {"version": dt.version()}
        elif action == "vacuum":
            files = dt.vacuum(retention_hours=cfg.vacuum_retention_hours, dry_run=dry_run,
                              enforce_retention_duration=cfg.vacuum_retention_hours >= 168)
            detail = {"files": len(files), "dry_run": dry_run}
        else:
            raise ValueError(f"unknown fix action {action!r}")
        results.append(FixResult(action, detail))
    return results


def apply_iceberg(table, actions: list[str], cfg: Config, dry_run: bool = False) -> list[FixResult]:
    """Iceberg fixes pyiceberg can run itself. Rewrites (compaction, sort, manifests) need Spark and are only reported."""
    results = []
    for action in actions:
        if action != "expire":
            results.append(FixResult(action, {"skipped": "needs Spark (see the SQL in the scan report)"}))
            continue
        cutoff = datetime.now(timezone.utc) - timedelta(hours=cfg.vacuum_retention_hours)
        current = table.current_snapshot()
        expired = [s.snapshot_id for s in table.snapshots()
                   if s.timestamp_ms < cutoff.timestamp() * 1000 and (current is None or s.snapshot_id != current.snapshot_id)]
        if expired and not dry_run:
            table.maintenance.expire_snapshots().by_ids(expired).commit()
        results.append(FixResult("expire", {"snapshots": len(expired), "dry_run": dry_run}))
    return results
