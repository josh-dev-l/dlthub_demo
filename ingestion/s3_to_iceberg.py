import dlt
from dlt.sources.filesystem import filesystem, read_csv
from dlt.common.pipeline import get_dlt_pipelines_dir
import os
import time
from threading import current_thread
from dlt.common.typing import TDataItems, TDataItem
from typing import Iterable
from dlt.extract import DltResource

BRONZE_LAYER = "cat_rnd_odp_dev_lake_bronze"
dev_pipelines_dir = os.path.join(get_dlt_pipelines_dir(), "dev")


@dlt.resource(name="tpch_region_test", parallelized=False)
def tpch_region() -> TDataItems:
    tpch_region_source_s3 = filesystem(
        file_glob="h/1/region*.tbl"
        )
    region  = tpch_region_source_s3 | read_csv(delimiter="|", header=None, names=["r_regionkey", "r_name", "r_comment"])
    yield from region
    
@dlt.resource(name="tpch_nation_test", parallelized=False)
def tpch_nation() -> TDataItems:
    tpch_nation_source_s3 = filesystem(
        file_glob="h/1/nation*.tbl*"
    )
    nation  = tpch_nation_source_s3 | read_csv(delimiter="|", header=None, names=["n_nationkey", "n_name", "n_regionkey", "n_comment"])
    yield from nation

@dlt.resource(name="tpch_customer_test", parallelized=False)
def tpch_customer() -> TDataItems:
    tpch_customer_source_s3 = filesystem(
        file_glob="h/100/customer*.tbl*"
    )
    customer  = tpch_customer_source_s3 | read_csv(delimiter="|", header=None, names=["c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"])
    yield from customer

@dlt.source
def all_data() -> Iterable[DltResource]:
    return tpch_customer, tpch_nation, tpch_region

pipeline = dlt.pipeline(
    pipeline_name="tpch_athena_pipeline_test", 
    dataset_name=BRONZE_LAYER, 
    destination="athena",
    staging="filesystem",
    progress="log",
    # dev_mode=True,
    pipelines_dir=dev_pipelines_dir
    )

# nation = pipeline.run(nation, table_name='tpch_nation', write_disposition="replace")
# customer = pipeline.run(customer, table_name='tpch_customer', write_disposition="replace")
load_info = pipeline.run(all_data(),write_disposition="replace")
# pipeline.dataset().df_data.df()

print(load_info)