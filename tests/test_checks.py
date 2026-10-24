import json

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from lakehouse_doctor import load, run_checks
from lakehouse_doctor.checks import DataFile, files_per_lookup, health_score
from lakehouse_doctor.cli import main, parse_size
from lakehouse_doctor.fix import apply


def codes(path, cfg):
    return {f.code for f in run_checks(load(path), cfg)}


def test_small_files_found_and_fixed(many_small_files, cfg):
    findings = run_checks(load(many_small_files), cfg)
    lh001 = next(f for f in findings if f.code == "LH001")
    assert lh001.metrics["files"] == 40 and lh001.metrics["files_after_compaction"] == 1
    assert "OPTIMIZE" in lh001.fixes[0].sql

    apply(many_small_files, ["compact"], cfg)
    snap = load(many_small_files)
    assert len(snap.files) == 1 and snap.total_records == 2000
    assert "LH001" not in codes(many_small_files, cfg)


def test_files_per_lookup_metric():
    f = lambda lo, hi: DataFile("p", 1, 1, {}, {"k": lo}, {"k": hi})  # noqa: E731
    assert files_per_lookup([f(0, 9), f(10, 19), f(20, 29)], "k") == 1.0
    assert files_per_lookup([f(0, 100), f(0, 100), f(0, 100)], "k") == 3.0
    assert files_per_lookup([f(0, 9)], "k") is None


def test_clustering_found_and_fixed_by_zorder(unclustered, cfg):
    cfg.keys = ["id"]
    cfg.target_file_size = 4 * 1024  # force z-order to write several files so skipping can be measured
    before = next(f for f in run_checks(load(unclustered), cfg) if f.code == "LH005")
    assert before.metrics["avg_files_per_lookup"] > 15

    apply(unclustered, ["zorder:id"], cfg)
    snap = load(unclustered)
    assert len(snap.files) > 1
    assert files_per_lookup(snap.files, "id") < 2
    assert "LH005" not in codes(unclustered, cfg)


def test_clustering_only_checked_for_declared_keys(unclustered, cfg):
    assert "LH005" not in codes(unclustered, cfg)


def test_skew(skewed, cfg):
    f = next(f for f in run_checks(load(skewed), cfg) if f.code == "LH003")
    assert "region=in" in f.detail and f.metrics["ratio"] >= 10


def test_over_partitioned(over_partitioned, cfg):
    assert "LH002" in codes(over_partitioned, cfg)


def test_log_checkpoint(many_small_files, cfg):
    assert "LH006" in codes(many_small_files, cfg)
    apply(many_small_files, ["checkpoint"], cfg)
    snap = load(many_small_files)
    assert snap.log.commits_since_checkpoint == 0
    assert "LH006" not in codes(many_small_files, cfg)


def test_vacuum_after_compaction(many_small_files, cfg):
    apply(many_small_files, ["compact"], cfg)
    f = next(f for f in run_checks(load(many_small_files), cfg) if f.code == "LH007")
    assert f.metrics["files"] == 40 and f.metrics["bytes"] > 0
    dry = apply(many_small_files, ["vacuum"], cfg, dry_run=True)[0]
    assert dry.detail == {"files": 40, "dry_run": True}
    apply(many_small_files, ["vacuum"], cfg)
    assert "LH007" not in codes(many_small_files, cfg)
    assert DeltaTable(many_small_files).to_pyarrow_table().num_rows == 2000


def test_missing_stats(tmp_path, cfg):
    path = str(tmp_path / "nostats")
    for i in range(3):
        write_deltalake(path, pa.table({"id": [i], "s": ["x"]}), mode="append",
                        configuration={"delta.dataSkippingNumIndexedCols": "0"})
    assert "LH004" in codes(path, cfg)


def test_healthy_table_scores_100(tmp_path, cfg):
    path = str(tmp_path / "ok")
    write_deltalake(path, pa.table({"id": list(range(100))}))
    findings = run_checks(load(path), cfg)
    assert findings == [] and health_score(findings) == 100


def test_cli_scan_json_and_fail_under(many_small_files, tmp_path, capsys):
    out = tmp_path / "r.json"
    args = ["scan", many_small_files, "--target-file-size", "256KB"]
    assert main([*args, "--format", "json", "-o", str(out)]) == 0
    data = json.loads(out.read_text())
    assert data["table"]["files"] == 40 and data["score"] < 100
    assert main([*args, "--fail-under", "100", "-o", str(tmp_path / "r.md")]) == 1


def test_cli_fix_auto_skips_vacuum(many_small_files, capsys):
    assert main(["fix", many_small_files, "--auto", "--target-file-size", "256KB"]) == 0
    out = capsys.readouterr().out
    assert "compact:" in out and "vacuum" not in out
    assert len(load(many_small_files).files) == 1


def test_parse_size():
    assert parse_size("128MB") == 128 * 1024**2 and parse_size("1gb") == 1024**3 and parse_size("512") == 512
