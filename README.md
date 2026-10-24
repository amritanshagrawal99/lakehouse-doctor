# lakehouse-doctor

[![CI](https://github.com/amritanshagrawal99/lakehouse-doctor/actions/workflows/ci.yml/badge.svg)](https://github.com/amritanshagrawal99/lakehouse-doctor/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**Health checks and one-command fixes for Delta Lake and Apache Iceberg tables.** It points at a table (local, S3, ADLS or GCS), reads the
transaction log and file statistics, and tells you why queries on the table are slow or expensive. It also gives the exact
`OPTIMIZE` / `ZORDER` / `VACUUM` commands to fix it, and can run them for you with [delta-rs](https://github.com/delta-io/delta-rs).

No Spark cluster needed: it's pure Python and only reads metadata.

## Example

A streaming job has been appending 500-row micro-batches for a while, with no maintenance
([examples/make_messy_table.py](examples/make_messy_table.py)):

```bash
$ lakehouse-doctor scan demo_events --key user_id --target-file-size 1MB
```

> # Lakehouse health: 50/100
>
> | Files | Size | Records | Partitioned by | Partitions |
> |---|---|---|---|---|
> | 1200 | 4.0 MB | 100,000 | country | 6 |
>
> ## 🔴 LH001 Too many small files
> 1200 of 1200 files are under 256.0 KB (average 3.4 KB). Compaction would leave about 8 file(s). Every extra file costs a
> metadata read, a task and an object-store request.
> ```sql
> OPTIMIZE delta.`s3://lake/demo_events`
> ```
>
> ## 🔴 LH005 Poor data skipping on `user_id`
> A filter on `user_id` = value has to open 166.1 of 200 files within a partition on average, because the files' min/max
> ranges overlap. Z-ordering or clustering by `user_id` makes each value land in a few files.
> ```sql
> OPTIMIZE delta.`s3://lake/demo_events` ZORDER BY (user_id)
> ```
>
> ## 🟠 LH003 Skewed partitions
> Partition country=IN holds 89,958 rows (90% of the table), 45x the median partition. Per-partition work (writes, OPTIMIZE,
> MERGE) will be bottlenecked on it.

```bash
$ lakehouse-doctor fix demo_events --auto --key user_id --target-file-size 1MB
zorder:user_id: numFilesAdded=8, numFilesRemoved=1200

$ lakehouse-doctor scan demo_events --key user_id --target-file-size 1MB
# Lakehouse health: 90/100        <- only the partition skew remains; that needs a data-model change, not maintenance

$ lakehouse-doctor fix demo_events --vacuum --dry-run
vacuum: files=1200, dry_run=True
```

`--auto` never runs `VACUUM`, because vacuum deletes files and breaks time travel past the retention window. It only runs when
you ask for it with `--vacuum`.

## Checks

| Code | Check | How it's measured | Fix |
|---|---|---|---|
| LH001 | Too many small files | Files under ¼ of the target size, compared with the number of files compaction would produce per partition | `OPTIMIZE` |
| LH002 | Over-partitioning | Partition count and average partition size against the target file size | Coarser partitions or liquid clustering |
| LH003 | Partition skew | Rows in the largest partition / median partition | Data-model change (reported, not auto-fixed) |
| LH004 | Missing statistics | Files without min/max stats, so queries can't skip them | `OPTIMIZE` (rewrites with stats) |
| LH005 | Poor data skipping on a key | Average number of files a point lookup must open, from per-file min/max ranges, within each partition | `OPTIMIZE ... ZORDER BY` |
| LH006 | Log needs a checkpoint | Commits since the last checkpoint | `create_checkpoint()` |
| LH007 | Vacuumable storage | Unreferenced files older than the retention window, with bytes reclaimable | `VACUUM ... RETAIN n HOURS` |
| IC001 | Iceberg: snapshots piling up | Retained snapshots against `max_snapshots` | `expire_snapshots` (runs via pyiceberg) |
| IC002 | Iceberg: too many manifests | Manifests in the current snapshot against data files | `rewrite_manifests` |
| IC003 | Iceberg: merge-on-read deletes | Rows marked by delete files / data rows | `rewrite_data_files` with `delete-file-threshold` |

LH001–LH005 work the same on both formats. Per-file bounds come from the Delta log or from Iceberg manifests. LH006/LH007
are specific to Delta, and IC001–IC003 to Iceberg.

The health score starts at 100 and loses 25 points per critical finding and 10 per warning.

### How LH005 works

Each file's `[min, max]` for the key column comes from the Delta log. The key's value range is cut at every file boundary.
For each piece, the tool counts how many files' ranges cover it, then takes the average weighted by the piece's width. The
result is the expected number of files a `WHERE key = x` query opens. It is 1.0 when the table is perfectly clustered and equal
to the file count when every lookup reads every file. It's computed within each partition, because a lookup without a
partition filter has to visit every partition however the data is laid out.

## Usage

```bash
pip install "git+https://github.com/amritanshagrawal99/lakehouse-doctor.git"

lakehouse-doctor scan  PATH [--key COL ...] [--target-file-size 128MB] [--format json] [--fail-under 80]
lakehouse-doctor fix   PATH --auto | --compact | --zorder COL[,COL] | --checkpoint | --vacuum  [--dry-run]

# cloud storage: credentials go to delta-rs
lakehouse-doctor scan s3://lake/events --storage-option AWS_REGION=ap-south-1 --key user_id
```

`--fail-under` makes it usable as a scheduled data-platform check, failing the job when a table's health drops.

### Fleet report

```bash
lakehouse-doctor fleet /lake/bronze --target-file-size 128MB          # every Delta table under a directory
lakehouse-doctor fleet analytics --catalog prod --catalog-prop uri=...  # every Iceberg table in a namespace
lakehouse-doctor fleet /lake --fail-under 70                           # nightly job: fail when any table degrades
```

The report lists unreadable tables first, then every table ranked worst score first, with file count, size, worst finding and
total reclaimable storage.

### Apache Iceberg

```bash
pip install "lakehouse-doctor[iceberg] @ git+https://github.com/amritanshagrawal99/lakehouse-doctor.git"

# any pyiceberg catalog: REST, Glue, Hive, SQL... (properties as in ~/.pyiceberg.yaml)
lakehouse-doctor scan db.events --catalog prod --catalog-prop uri=https://catalog.example.com --key user_id
lakehouse-doctor fix  db.events --catalog prod --expire --retention-hours 168
```

For Iceberg, findings come with the Spark procedure to run, such as `CALL <catalog>.system.rewrite_data_files(...)` or
`... strategy => 'sort', sort_order => 'zorder(user_id)'`. pyiceberg can't rewrite data files yet, so `fix` runs snapshot
expiry only and reports the rewrite commands for you to run.

## Development

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
pytest && ruff check src tests
```

Tests build real Delta tables with known problems: 40 tiny appends, 20 files with shuffled keys, a 90%-skewed partition, 30
three-row partitions, and stats disabled. For each one they assert that the finding appears, apply the fix, and assert that it's
gone and no rows were lost.

## Roadmap


- Deletion-vector and `MERGE` amplification checks
- Fleet scans of cloud prefixes (s3://, abfss://) and Unity Catalog schemas

## License

[MIT](LICENSE). Built on [delta-rs](https://github.com/delta-io/delta-rs) (Apache-2.0).
