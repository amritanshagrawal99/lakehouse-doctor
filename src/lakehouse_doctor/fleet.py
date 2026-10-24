"""Scan every table under a directory (Delta) or a catalog namespace (Iceberg) and rank them by health."""
from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path

from .checks import Config, Finding, _fmt_bytes, health_score, run_checks
from .snapshot import load


@dataclass
class TableHealth:
    table: str
    score: int | None
    files: int = 0
    bytes: int = 0
    findings: list[Finding] = field(default_factory=list)
    reclaimable_bytes: int = 0
    error: str | None = None

    @property
    def worst(self) -> Finding | None:
        return self.findings[0] if self.findings else None


def discover_delta(root: str | Path) -> list[Path]:
    """Directories under `root` that are Delta tables (contain _delta_log). Does not descend into a table."""
    found = []
    for dirpath, dirnames, _ in os.walk(root):
        if "_delta_log" in dirnames:
            found.append(Path(dirpath))
            dirnames.clear()
        else:
            dirnames[:] = sorted(d for d in dirnames if not d.startswith("."))
    return sorted(found)


def _health(name: str, loader, cfg: Config) -> TableHealth:
    try:
        snap = loader()
        findings = run_checks(snap, cfg)
    except Exception as e:  # noqa: BLE001 - one unreadable table must not stop the fleet scan
        return TableHealth(name, None, error=f"{type(e).__name__}: {str(e).splitlines()[0][:200]}")
    reclaim = sum(f.metrics.get("bytes") or 0 for f in findings if f.code == "LH007")
    return TableHealth(name, health_score(findings), len(snap.files), snap.total_bytes, findings, reclaim)


def scan_delta_fleet(root: str | Path, cfg: Config) -> list[TableHealth]:
    root = Path(root)
    out = [_health(p.relative_to(root).as_posix() or ".", lambda p=p: load(str(p)), cfg) for p in discover_delta(root)]
    return _rank(out)


def scan_iceberg_fleet(catalog, namespace: str, cfg: Config) -> list[TableHealth]:
    from .iceberg import load_iceberg
    out = []
    for ident in catalog.list_tables(namespace):
        name = ".".join(ident)
        out.append(_health(name, lambda name=name: load_iceberg(catalog.load_table(name), name), cfg))
    return _rank(out)


def _rank(rows: list[TableHealth]) -> list[TableHealth]:
    # Unreadable tables first (they need a human), then worst score, then biggest.
    return sorted(rows, key=lambda r: (r.score is not None, r.score if r.score is not None else 0, -r.bytes))


def to_markdown(rows: list[TableHealth], root: str) -> str:
    ok = [r for r in rows if r.score is not None]
    avg = sum(r.score for r in ok) / len(ok) if ok else 0
    lines = [
        f"# Fleet health: {len(rows)} tables under `{root}`", "",
        f"Average score **{avg:.0f}/100** · {sum(r.score is not None and r.score < 80 for r in rows)} tables below 80 · "
        f"{_fmt_bytes(sum(r.bytes for r in rows))} scanned · {_fmt_bytes(sum(r.reclaimable_bytes for r in rows))} reclaimable", "",
        "| Table | Score | Files | Size | Worst finding | Findings |", "|---|---|---|---|---|---|",
    ]
    for r in rows:
        if r.error:
            lines.append(f"| `{r.table}` | error | | | {r.error.replace('|', '/')} | |")
            continue
        worst = f"{r.worst.code} {r.worst.title}" if r.worst else "-"
        lines.append(f"| `{r.table}` | {r.score} | {r.files} | {_fmt_bytes(r.bytes)} | {worst} | "
                     f"{', '.join(f.code for f in r.findings) or '-'} |")
    return "\n".join(lines) + "\n"


def to_json(rows: list[TableHealth]) -> str:
    return json.dumps([{**asdict(r), "findings": [asdict(f) for f in r.findings]} for r in rows], indent=2, default=str)
