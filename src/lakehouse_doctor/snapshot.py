"""A read-only snapshot of everything the checks need from one Delta table."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

import pyarrow as pa
from deltalake import DeltaTable


@dataclass
class DataFile:
    path: str
    size: int
    records: int | None
    partition: dict[str, Any]
    min: dict[str, Any] = field(default_factory=dict)
    max: dict[str, Any] = field(default_factory=dict)
    null_count: dict[str, int] = field(default_factory=dict)


@dataclass
class LogInfo:
    commit_files: int
    commits_since_checkpoint: int
    last_checkpoint_version: int | None


@dataclass
class IcebergInfo:
    snapshots: int
    oldest_snapshot_ms: int | None
    current_snapshot_ms: int | None
    manifests: int
    delete_files: int
    delete_records: int


@dataclass
class Snapshot:
    uri: str
    local_path: Path | None
    version: int
    partition_columns: list[str]
    columns: dict[str, str]  # name -> arrow type
    files: list[DataFile]
    history: list[dict]  # newest first
    log: LogInfo | None
    storage_options: dict[str, str] | None = None
    format: str = "delta"           # delta | iceberg
    identifier: str | None = None   # catalog identifier (Iceberg), e.g. db.events
    iceberg: IcebergInfo | None = None

    @property
    def total_bytes(self) -> int:
        return sum(f.size for f in self.files)

    @property
    def total_records(self) -> int:
        return sum(f.records or 0 for f in self.files)

    def partitions(self) -> dict[tuple, list[DataFile]]:
        groups: dict[tuple, list[DataFile]] = {}
        for f in self.files:
            key = tuple(f.partition.get(c) for c in self.partition_columns)
            groups.setdefault(key, []).append(f)
        return groups


def _local_path(uri: str) -> Path | None:
    parsed = urlparse(uri)
    if parsed.scheme in ("", "file"):
        p = unquote(parsed.path) if parsed.scheme == "file" else uri
        if os.name == "nt" and len(p) > 2 and p[0] == "/" and p[2] == ":":
            p = p[1:]
        return Path(p)
    if len(parsed.scheme) == 1:  # Windows drive letter, e.g. C:\tables\t
        return Path(uri)
    return None


def _log_info(table_path: Path) -> LogInfo | None:
    log_dir = table_path / "_delta_log"
    if not log_dir.is_dir():
        return None
    commits = sorted(int(p.name.split(".")[0]) for p in log_dir.glob("*.json") if p.name.split(".")[0].isdigit())
    last_cp = None
    marker = log_dir / "_last_checkpoint"
    if marker.exists():
        try:
            last_cp = int(json.loads(marker.read_text())["version"])
        except (ValueError, KeyError):
            last_cp = None
    latest = commits[-1] if commits else 0
    since = latest - last_cp if last_cp is not None else latest + 1
    return LogInfo(len(commits), since, last_cp)


def load(uri: str, storage_options: dict[str, str] | None = None) -> Snapshot:
    dt = DeltaTable(uri, storage_options=storage_options)
    actions = pa.table(dt.get_add_actions(flatten=True)).to_pylist()
    files = []
    for a in actions:
        f = DataFile(a["path"], int(a["size_bytes"]), a.get("num_records"),
                     {k[len("partition."):]: v for k, v in a.items() if k.startswith("partition.")})
        for k, v in a.items():
            if k.startswith("min."):
                f.min[k[4:]] = v
            elif k.startswith("max."):
                f.max[k[4:]] = v
            elif k.startswith("null_count.") and v is not None:
                f.null_count[k[11:]] = int(v)
        files.append(f)
    schema = pa.schema(dt.schema().to_arrow())
    local = _local_path(dt.table_uri)
    return Snapshot(
        uri=dt.table_uri,
        local_path=local,
        version=dt.version(),
        partition_columns=list(dt.metadata().partition_columns),
        columns={fld.name: str(fld.type) for fld in schema},
        files=files,
        history=dt.history(),
        log=_log_info(local) if local else None,
        storage_options=storage_options,
    )
