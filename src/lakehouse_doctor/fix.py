"""Apply fixes with delta-rs. Destructive actions (vacuum) only run when asked for explicitly."""
from __future__ import annotations

from dataclasses import dataclass

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
