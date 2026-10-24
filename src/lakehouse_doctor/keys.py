"""Recommend clustering keys from a query log: which columns do queries actually filter on, per table?

Each query is parsed with sqlglot. Inside every SELECT scope, aliases are resolved to table names and predicates are
classified by the column they constrain:
    point  - col = literal, col IN (...)            (best served by Z-order / clustering)
    range  - col > / < / BETWEEN literal              (also helped, a bit less)
    join   - col = other_table.col                    (useful when the table is joined much more than filtered)
Scores weight point 3, range 2, join 1. Partition columns are reported but not recommended (they already prune).
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp
from sqlglot.optimizer.scope import traverse_scope

WEIGHTS = {"point": 3, "range": 2, "join": 1}


@dataclass
class ColumnUse:
    point: int = 0
    range: int = 0
    join: int = 0

    @property
    def score(self) -> int:
        return self.point * WEIGHTS["point"] + self.range * WEIGHTS["range"] + self.join * WEIGHTS["join"]


@dataclass
class KeyReport:
    queries: int
    parsed: int
    tables: dict[str, dict[str, ColumnUse]] = field(default_factory=dict)

    def ranked(self, table: str) -> list[tuple[str, ColumnUse]]:
        cols = self.tables.get(table.lower(), {})
        return sorted(cols.items(), key=lambda kv: (-kv[1].score, kv[0]))


def split_queries(text: str) -> list[str]:
    return [q.strip() for q in text.split(";") if q.strip()]


def _is_constant(e: exp.Expression) -> bool:
    return not any(True for _ in e.find_all(exp.Column))


def _table_of(col: exp.Column, scope) -> str | None:
    src = scope.sources.get(col.table) if col.table else None
    if src is None and not col.table:
        tables = [s for s in scope.sources.values() if isinstance(s, exp.Table)]
        src = tables[0] if len(tables) == 1 else None
    return src.name.lower() if isinstance(src, exp.Table) else None


def analyse(queries: list[str], dialect: str | None = None) -> KeyReport:
    rep = KeyReport(len(queries), 0)
    uses: dict[str, dict[str, ColumnUse]] = defaultdict(lambda: defaultdict(ColumnUse))
    for q in queries:
        try:
            tree = sqlglot.parse_one(q, read=dialect)
            if not isinstance(tree, (exp.Query, exp.Insert, exp.Update, exp.Delete, exp.Merge)):
                continue
            scopes = list(traverse_scope(tree))
        except Exception:  # noqa: BLE001 - unparseable log lines are counted, not fatal
            continue
        rep.parsed += 1
        for scope in scopes:
            preds = []
            where = scope.expression.args.get("where") if isinstance(scope.expression, exp.Select) else None
            if where is not None:
                preds += [(p, False) for p in where.find_all(exp.EQ, exp.In, exp.Between, exp.GT, exp.GTE, exp.LT, exp.LTE)]
            for join in scope.expression.args.get("joins") or [] if isinstance(scope.expression, exp.Select) else []:
                on = join.args.get("on")
                if on is not None:
                    preds += [(p, True) for p in on.find_all(exp.EQ, exp.In, exp.Between, exp.GT, exp.GTE, exp.LT, exp.LTE)]
            for pred, _ in preds:
                left = pred.this
                if not isinstance(left, exp.Column):
                    continue
                if isinstance(pred, exp.In):
                    kind = "point" if pred.expressions and all(_is_constant(e) for e in pred.expressions) else None
                elif isinstance(pred, exp.Between):
                    kind = "range"
                elif isinstance(pred.expression, exp.Column):
                    # A join key counts for both sides of the equality.
                    kind = "join"
                    other_table = _table_of(pred.expression, scope)
                    if other_table:
                        uses[other_table][pred.expression.name.lower()].join += 1
                elif _is_constant(pred.expression):
                    kind = "point" if isinstance(pred, exp.EQ) else "range"
                else:
                    kind = None
                table = _table_of(left, scope) if kind else None
                if table:
                    use = uses[table][left.name.lower()]
                    setattr(use, kind, getattr(use, kind) + 1)
    rep.tables = {t: dict(cols) for t, cols in uses.items()}
    return rep


def recommend(rep: KeyReport, table: str, partition_columns: list[str] | None = None, top: int = 2) -> list[str]:
    parts = {c.lower() for c in partition_columns or []}
    ranked = [(c, u) for c, u in rep.ranked(table) if c not in parts and (u.point or u.range)]
    return [c for c, _ in ranked[:top]]
