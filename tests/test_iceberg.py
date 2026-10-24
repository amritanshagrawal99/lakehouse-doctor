import random

import pyarrow as pa
import pytest

pytest.importorskip("pyiceberg")

from pyiceberg.catalog.sql import SqlCatalog  # noqa: E402

from lakehouse_doctor import run_checks  # noqa: E402
from lakehouse_doctor.checks import Config  # noqa: E402
from lakehouse_doctor.cli import main  # noqa: E402
from lakehouse_doctor.fix import apply_iceberg  # noqa: E402
from lakehouse_doctor.iceberg import load_iceberg  # noqa: E402
from lakehouse_doctor.snapshot import DataFile, IcebergInfo, Snapshot  # noqa: E402

SCHEMA = pa.schema([("id", pa.int64()), ("country", pa.string()), ("amount", pa.float64())])


@pytest.fixture
def catalog(tmp_path):
    wh = tmp_path.as_posix()
    cat = SqlCatalog("local", uri=f"sqlite:///{wh}/catalog.db", warehouse=wh)
    cat.create_namespace("db")
    return cat, {"type": "sql", "uri": f"sqlite:///{wh}/catalog.db", "warehouse": wh}


@pytest.fixture
def events(catalog):
    """30 small appends with shuffled ids: small files, overlapping ranges, 30 snapshots."""
    cat, _ = catalog
    t = cat.create_table("db.events", schema=SCHEMA)
    ids = list(range(3000))
    random.Random(1).shuffle(ids)
    for i in range(30):
        chunk = ids[i * 100:(i + 1) * 100]
        t.append(pa.table({"id": pa.array(chunk, pa.int64()), "country": ["IN"] * 100, "amount": [1.0] * 100}, schema=SCHEMA))
    return t


@pytest.fixture
def cfg():
    return Config(target_file_size=256 * 1024, min_files_for_compaction=5, keys=["id"], max_snapshots=10, vacuum_retention_hours=0)


def codes(findings):
    return {f.code for f in findings}


def test_iceberg_snapshot(events):
    snap = load_iceberg(events)
    assert snap.format == "iceberg" and snap.identifier == "db.events"
    assert len(snap.files) == 30 and snap.total_records == 3000
    assert snap.iceberg.snapshots == 30 and snap.iceberg.delete_files == 0
    assert all(f.min["id"] is not None and f.max["id"] is not None for f in snap.files)


def test_iceberg_findings_use_spark_procedures(events, cfg):
    findings = {f.code: f for f in run_checks(load_iceberg(events), cfg)}
    assert {"LH001", "LH005", "IC001"} <= set(findings)
    assert "LH006" not in findings and "LH007" not in findings  # Delta-only checks
    assert findings["LH001"].fixes[0].sql.startswith("CALL <catalog>.system.rewrite_data_files(table => 'db.events'")
    assert "zorder(id)" in findings["LH005"].fixes[0].sql
    assert findings["IC001"].fixes[0].action == "expire"


def test_expire_snapshots(events, cfg):
    dry = apply_iceberg(events, ["expire"], cfg, dry_run=True)[0]
    assert dry.detail == {"snapshots": 29, "dry_run": True}
    apply_iceberg(events, ["expire"], cfg)
    events.refresh()
    snap = load_iceberg(events)
    assert snap.iceberg.snapshots == 1 and snap.total_records == 3000
    assert "IC001" not in codes(run_checks(snap, cfg))


def test_rewrites_are_reported_not_run(events, cfg):
    [r] = apply_iceberg(events, ["compact"], cfg)
    assert "needs Spark" in r.detail["skipped"]


def test_partitioned_iceberg_skew(catalog, cfg):
    cat, _ = catalog
    t = cat.create_table("db.sales", schema=SCHEMA)
    with t.update_spec() as spec:
        spec.add_identity("country")
    rows = [("IN", 5000)] + [(c, 50) for c in ("US", "UK", "SG", "AE")]
    t.append(pa.concat_tables([
        pa.table({"id": pa.array(range(n), pa.int64()), "country": [c] * n, "amount": [1.0] * n}, schema=SCHEMA) for c, n in rows]))
    snap = load_iceberg(t)
    assert snap.partition_columns == ["country"] and len(snap.partitions()) == 5
    skew = next(f for f in run_checks(snap, cfg) if f.code == "LH003")
    assert "country=IN" in skew.detail


def test_merge_on_read_delete_ratio(cfg):
    files = [DataFile(f"f{i}", 10_000_000, 1000, {}) for i in range(4)]
    snap = Snapshot("meta.json", None, 1, [], {"id": "long"}, files, [], None, format="iceberg", identifier="db.t",
                    iceberg=IcebergInfo(2, 0, 1, 2, delete_files=12, delete_records=900))
    f = next(f for f in run_checks(snap, cfg) if f.code == "IC003")
    assert f.metrics["ratio"] == 0.225 and "delete-file-threshold" in f.fixes[0].sql


def test_cli_scan_and_expire(events, catalog, capsys):
    _, props = catalog
    base = ["db.events", "--catalog", "local", *[x for k, v in props.items() for x in ("--catalog-prop", f"{k}={v}")]]
    assert main(["scan", *base, "--key", "id", "--target-file-size", "256KB"]) == 0
    out = capsys.readouterr().out
    assert "rewrite_data_files" in out and "IC001" not in out  # default max_snapshots is 100
    assert main(["fix", *base, "--expire", "--retention-hours", "0"]) == 0
    assert "expire: snapshots=29" in capsys.readouterr().out
