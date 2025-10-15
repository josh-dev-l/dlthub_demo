import dlt
from dlt.sources.filesystem import filesystem, read_csv

BRONZE_LAYER = "cat_rnd_odp_dev_lake_bronze"

tpch_region_source_s3 = filesystem(
    file_glob="h/1/region*.tbl"
)

tpch_nation_source_s3 = filesystem(
    file_glob="h/100/nation*.tbl*"
)

tpch_customer_source_s3 = filesystem(
    file_glob="h/100/customer*.tbl*"
)

region  = (tpch_region_source_s3 | read_csv(delimiter="|", header=None, names=["r_regionkey", "r_name", "r_comment"]))
nation = (tpch_nation_source_s3 | read_csv(delimiter="|", header=None, names=["n_nationkey", "n_name", "n_regionkey", "n_comment"]))
customer = (tpch_customer_source_s3 | read_csv(delimiter="|", header=None, names=["c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"]))

pipeline = dlt.pipeline(
    pipeline_name="tpch_athena_pipeline", 
    dataset_name=BRONZE_LAYER, 
    destination="athena",
    staging="filesystem"
    )

nation = pipeline.run(nation, table_name='tpch_nation')
customer = pipeline.run(customer, table_name='tpch_customer')
region = pipeline.run(region, table_name='tpch_region')
print(nation)
print(customer)
print(region)