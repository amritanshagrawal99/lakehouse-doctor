"""Load an Apache Iceberg table (via pyiceberg) into the same Snapshot the checks use for Delta."""
from __future__ import annotations

from .snapshot import DataFile, IcebergInfo, Snapshot

DATA, POSITION_DELETES, EQUALITY_DELETES = 0, 1, 2


def load_catalog_table(identifier: str, catalog: str, props: dict[str, str] | None = None):
    from pyiceberg.catalog import load_catalog
    return load_catalog(catalog, **(props or {})).load_table(identifier)


def load_iceberg(table, identifier: str | None = None) -> Snapshot:
    """`table` is a pyiceberg Table. Reads metadata only: manifests, file stats, snapshots."""
    files: list[DataFile] = []
    delete_files = delete_records = 0
    snap = table.current_snapshot()
    if snap is not None:
        for f in table.inspect.files().to_pylist():
            if f["content"] != DATA:
                delete_files += 1
                delete_records += f["record_count"] or 0
                continue
            metrics = f.get("readable_metrics") or {}
            df = DataFile(f["file_path"], int(f["file_size_in_bytes"]), f["record_count"], dict(f.get("partition") or {}))
            for col, m in metrics.items():
                if not m:
                    continue
                if m.get("lower_bound") is not None:
                    df.min[col] = m["lower_bound"]
                if m.get("upper_bound") is not None:
                    df.max[col] = m["upper_bound"]
                if m.get("null_value_count") is not None:
                    df.null_count[col] = int(m["null_value_count"])
            files.append(df)
    snapshots = table.snapshots()
    times = [s.timestamp_ms for s in snapshots]
    manifests = table.inspect.manifests().num_rows if snap is not None else 0
    return Snapshot(
        uri=table.metadata_location,
        local_path=None,
        version=snap.sequence_number if snap is not None else 0,
        partition_columns=[p.name for p in table.spec().fields],
        columns={f.name: str(f.field_type) for f in table.schema().fields},
        files=files,
        history=[{"timestamp": t} for t in sorted(times, reverse=True)],
        log=None,
        format="iceberg",
        identifier=identifier or ".".join(table.name()[-2:]),
        iceberg=IcebergInfo(len(snapshots), min(times) if times else None, max(times) if times else None,
                            manifests, delete_files, delete_records),
    )
