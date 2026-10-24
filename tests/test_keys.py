from pathlib import Path

from lakehouse_doctor.cli import main
from lakehouse_doctor.keys import analyse, recommend, split_queries

LOG = Path(__file__).resolve().parents[1] / "examples" / "query_log.sql"


def report():
    return analyse(split_queries(LOG.read_text()))


def uses(rep, table):
    return {c: (u.point, u.range, u.join) for c, u in rep.ranked(table)}


def test_counts_point_range_and_join_filters():
    rep = report()
    assert rep.queries == 8 and rep.parsed == 7  # "this is not sql" is skipped
    assert uses(rep, "events") == {"user_id": (3, 0, 1), "country": (2, 0, 0), "event_ts": (0, 3, 0), "session_id": (1, 0, 0)}
    assert uses(rep, "users") == {"id": (0, 0, 1)}


def test_aliases_resolved_and_unqualified_single_table():
    rep = analyse(["SELECT * FROM a x JOIN b y ON y.k = x.k WHERE x.c = 1 AND y.d > 5", "SELECT * FROM a WHERE c IN (1, 2)"])
    assert uses(rep, "a") == {"c": (2, 0, 0), "k": (0, 0, 1)}
    assert uses(rep, "b") == {"d": (0, 1, 0), "k": (0, 0, 1)}


def test_non_constant_comparisons_ignored():
    rep = analyse(["SELECT * FROM t WHERE a = UPPER(b)", "SELECT * FROM t WHERE a IN (SELECT x FROM s)"])
    assert "a" not in uses(rep, "t")


def test_recommend_skips_partition_and_join_only_columns():
    rep = report()
    assert recommend(rep, "events", ["country"]) == ["user_id", "event_ts"]
    assert recommend(rep, "users") == []  # only ever joined on


def test_cli(capsys):
    assert main(["keys", str(LOG), "--table", "events"]) == 0
    out = capsys.readouterr().out
    assert "7/8 queries parsed" in out and "Recommended clustering keys: user_id, country" in out


def test_cli_with_table_path(many_small_files, capsys):
    log = Path(many_small_files).parent / "log.sql"
    log.write_text("SELECT * FROM small WHERE id = 5; SELECT * FROM small WHERE id BETWEEN 1 AND 9")
    assert main(["keys", str(log), "--table", "small", "--table-path", many_small_files]) == 0
    out = capsys.readouterr().out
    assert "files/lookup now" in out and "--key id" in out
