import dlt
from dlt.sources.filesystem import filesystem, read_csv

BRONZE_LAYER = "cat_rnd_odp_dev_lake_bronze"

tpch_source_s3 = filesystem(
    file_glob="h/1/region*.tbl"
)

reader  = (tpch_source_s3 | read_csv(delimiter="|", header=None, names=["r_regionkey", "r_name", "r_comment"]))


pipeline = dlt.pipeline(
    pipeline_name="tpch_athena_pipeline", 
    dataset_name=BRONZE_LAYER, 
    destination="athena",
    staging="filesystem"
    )

info = pipeline.run(reader, table_name='tpch_region')
print(info)