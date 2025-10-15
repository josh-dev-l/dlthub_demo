import dlt
from dlt.sources.filesystem import filesystem, read_csv

tpch_nation_source_s3 = filesystem(
    bucket_url="s3://clickhouse-datasets", 
    file_glob="h/1/nation*.tbl"
)

tpch_region_source_s3 = filesystem(
    bucket_url="s3://clickhouse-datasets", 
    file_glob="h/1/region*.tbl"
)   

nation_reader  = (tpch_nation_source_s3 | read_csv(delimiter="|")).with_name("tpch_nations")
region_reader = (tpch_region_source_s3 | read_csv(delimiter="|")).with_name("tpch_regions")

nation_pipeline = dlt.pipeline(pipeline_name="tpch_data_pipeline", dataset_name="tpch_data", destination="duckdb")


info = nation_pipeline.run(nation_reader)
print(info)

region_pipeline = dlt.pipeline(pipeline_name="tpch_data_pipeline", dataset_name="tpch_data", destination="duckdb")

info = region_pipeline.run(region_reader)
print(info)