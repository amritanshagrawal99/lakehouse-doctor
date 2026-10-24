import random

import pyarrow as pa
import pytest
from deltalake import write_deltalake

from lakehouse_doctor import Config

KB = 1024


@pytest.fixture
def cfg():
    # Tiny thresholds so tests run on kilobyte-sized tables.
    return Config(target_file_size=256 * KB, min_files_for_compaction=5, checkpoint_interval=20,
                  vacuum_retention_hours=0, min_reclaimable_bytes=0)


def batch(ids, region="in"):
    return pa.table({"id": pa.array(ids, pa.int64()), "region": [region] * len(ids),
                     "amount": pa.array([float(i % 97) for i in ids], pa.float64())})


@pytest.fixture
def many_small_files(tmp_path):
    path = str(tmp_path / "small")
    for i in range(40):
        write_deltalake(path, batch(range(i * 50, i * 50 + 50)), mode="append")
    return path


@pytest.fixture
def unclustered(tmp_path):
    """20 files, ids randomly spread so every file's min/max covers almost the whole range."""
    path = str(tmp_path / "unclustered")
    rng = random.Random(7)
    ids = list(range(20_000))
    rng.shuffle(ids)
    for i in range(20):
        write_deltalake(path, batch(ids[i * 1000:(i + 1) * 1000]), mode="append")
    return path


@pytest.fixture
def skewed(tmp_path):
    path = str(tmp_path / "skewed")
    t = pa.concat_tables([batch(range(0, 20_000), "in")] + [batch(range(k * 10, k * 10 + 10), f"r{k}") for k in range(5)])
    write_deltalake(path, t, partition_by=["region"])
    return path


@pytest.fixture
def over_partitioned(tmp_path):
    path = str(tmp_path / "overpart")
    t = pa.concat_tables([batch(range(k, k + 3), f"p{k:03d}") for k in range(30)])
    write_deltalake(path, t, partition_by=["region"])
    return path
