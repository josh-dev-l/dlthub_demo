from typing import Dict, Iterator
import os
import dlt
from dlt.common.typing import TDataItems
import matplotlib.pyplot as plt
import time


def get_rows(limit: int) -> Iterator[Dict[str, int]]:
    yield from map(lambda n: {"row": n}, range(limit))


def measure_extract_time(buffer_size: int) -> float:
    os.environ["DATA_WRITER__BUFFER_MAX_ITEMS"] = str(buffer_size)

    @dlt.resource()
    def buffered_resource() -> TDataItems:
        for row in get_rows(500000):
            yield row

    pipeline = dlt.pipeline(
        pipeline_name=f"extract_pipeline_{buffer_size}",
        destination="duckdb",
        dataset_name="mydata",
        dev_mode=True,
    )

    start_time = time.time()
    pipeline.extract(buffered_resource)
    return time.time() - start_time


# Try different buffer sizes
buffer_sizes = [1, 10, 100, 1000, 5000, 10000, 50000, 100000, 500000]
times = [measure_extract_time(size) for size in buffer_sizes]


plt.figure(figsize=(10, 6))
plt.plot(buffer_sizes, times, marker="o")
plt.xlabel("BUFFER_MAX_ITEMS")
plt.ylabel("Time to Extract (seconds)")
plt.title("Effect of Buffer Size on Extraction Time")
plt.grid(True)
plt.xscale("log")
plt.show()