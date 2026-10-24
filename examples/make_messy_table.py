"""Create a deliberately unhealthy Delta table for trying lakehouse-doctor.

Simulates a streaming job that appends tiny micro-batches with no maintenance:
200 small commits, user_ids randomly spread across files, one country holding most of the data.

    python examples/make_messy_table.py demo_events
    lakehouse-doctor scan demo_events --key user_id --target-file-size 1MB
"""
import random
import sys
from datetime import datetime, timedelta

import pyarrow as pa
from deltalake import write_deltalake

path = sys.argv[1] if len(sys.argv) > 1 else "demo_events"
rng = random.Random(42)
countries = ["IN"] * 90 + ["US", "UK", "SG", "AE", "DE"] * 2
start = datetime(2026, 1, 1)
for batch in range(200):
    n = 500
    write_deltalake(path, pa.table({
        "event_id": pa.array(range(batch * n, (batch + 1) * n), pa.int64()),
        "user_id": pa.array([rng.randrange(100_000) for _ in range(n)], pa.int64()),
        "country": [rng.choice(countries) for _ in range(n)],
        "event_ts": pa.array([start + timedelta(seconds=batch * 60 + i) for i in range(n)], pa.timestamp("us")),
        "amount": pa.array([round(rng.random() * 1000, 2) for _ in range(n)], pa.float64()),
    }), mode="append", partition_by=["country"])
print(f"wrote {path}: 200 commits")
