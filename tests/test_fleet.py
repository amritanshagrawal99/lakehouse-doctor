import pyarrow as pa
from deltalake import write_deltalake

from lakehouse_doctor.cli import main
from lakehouse_doctor.fleet import discover_delta, scan_delta_fleet


def test_fleet_ranks_tables(tmp_path, many_small_files, cfg):
    lake = tmp_path / "lake"
    write_deltalake(str(lake / "sales" / "orders"), pa.table({"id": list(range(100))}))
    for i in range(30):
        write_deltalake(str(lake / "sales" / "events"), pa.table({"id": [i]}), mode="append")
    (lake / "broken" / "_delta_log").mkdir(parents=True)  # a directory that looks like a table but is not readable
    (lake / "not_a_table").mkdir()

    assert [p.relative_to(lake).as_posix() for p in discover_delta(lake)] == ["broken", "sales/events", "sales/orders"]
    rows = scan_delta_fleet(lake, cfg)
    assert [r.table for r in rows] == ["broken", "sales/events", "sales/orders"]
    assert rows[0].error and rows[0].score is None
    assert rows[1].score < 100 and "LH001" in [f.code for f in rows[1].findings]
    assert rows[2].score == 100


def test_fleet_cli(tmp_path, capsys):
    lake = tmp_path / "lake"
    for i in range(12):
        write_deltalake(str(lake / "t1"), pa.table({"id": [i]}), mode="append")
    write_deltalake(str(lake / "t2"), pa.table({"id": [1]}))
    assert main(["fleet", str(lake), "--target-file-size", "256KB"]) == 0
    out = capsys.readouterr().out
    assert out.startswith("# Fleet health: 2 tables") and out.index("`t1`") < out.index("`t2`")
    assert main(["fleet", str(lake), "--target-file-size", "256KB", "--fail-under", "100", "-o", str(tmp_path / "f.md")]) == 1


def test_iceberg_fleet(tmp_path, cfg):
    from pyiceberg.catalog.sql import SqlCatalog

    from lakehouse_doctor.fleet import scan_iceberg_fleet
    wh = tmp_path.as_posix()
    cat = SqlCatalog("local", uri=f"sqlite:///{wh}/c.db", warehouse=wh)
    cat.create_namespace("db")
    schema = pa.schema([("id", pa.int64())])
    busy = cat.create_table("db.busy", schema=schema)
    for i in range(12):
        busy.append(pa.table({"id": pa.array([i], pa.int64())}))
    cat.create_table("db.quiet", schema=schema).append(pa.table({"id": pa.array([1], pa.int64())}))
    rows = scan_iceberg_fleet(cat, "db", cfg)
    assert [r.table for r in rows] == ["db.busy", "db.quiet"] and rows[0].score < rows[1].score
