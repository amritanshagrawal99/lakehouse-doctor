"""Markdown and JSON reports."""
from __future__ import annotations

import json
from dataclasses import asdict

from .checks import Finding, _fmt_bytes, health_score
from .snapshot import Snapshot

ICON = {"critical": "🔴", "warning": "🟠", "info": "🔵"}


def overview(snap: Snapshot) -> dict:
    return {
        "uri": snap.uri,
        "version": snap.version,
        "files": len(snap.files),
        "bytes": snap.total_bytes,
        "records": snap.total_records,
        "partition_columns": snap.partition_columns,
        "partitions": len(snap.partitions()) if snap.partition_columns else None,
        "commits_since_checkpoint": snap.log.commits_since_checkpoint if snap.log else None,
    }


def to_json(snap: Snapshot, findings: list[Finding]) -> str:
    return json.dumps({"table": overview(snap), "score": health_score(findings), "findings": [asdict(f) for f in findings]},
                      indent=2, default=str)


def to_markdown(snap: Snapshot, findings: list[Finding]) -> str:
    o = overview(snap)
    score = health_score(findings)
    lines = [
        f"# Lakehouse health: {score}/100",
        "",
        f"`{o['uri']}` at version {o['version']}",
        "",
        "| Files | Size | Records | Partitioned by | Partitions |",
        "|---|---|---|---|---|",
        f"| {o['files']} | {_fmt_bytes(o['bytes'])} | {o['records']:,} | {', '.join(o['partition_columns']) or '-'} | "
        f"{o['partitions'] if o['partitions'] is not None else '-'} |",
        "",
    ]
    if not findings:
        lines.append("No issues found.")
        return "\n".join(lines) + "\n"
    for f in findings:
        lines += [f"## {ICON[f.severity.value]} {f.code} {f.title}", "", f.detail, ""]
        for fix in f.fixes:
            lines += ["```sql", fix.sql, "```", "```python", fix.python, "```", ""]
    return "\n".join(lines).rstrip() + "\n"
