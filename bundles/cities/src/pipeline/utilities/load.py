import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Define the source path of your raw data files (e.g., cloud storage bucket or Unity Catalog volume path)
source_data_path = "/path/to/your/landing_zone/"
# Define the name of the bronze table
bronze_table_name = "raw_events_bronze"

@dlt.table(
    comment="Raw data ingested from cloud storage, incrementally updated.",
    table_properties={"quality": "bronze"}
)
def raw_events_bronze():
  # Use Auto Loader to incrementally read the data
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json") # Specify the format of your source files (json, csv, parquet, etc.)
      .option("cloudFiles.inferColumnTypes", "true") # Infer schema or provide a predefined one
      .load(source_data_path)
      # Add metadata for data lineage and debugging
      .withColumn("ingest_timestamp", current_timestamp())
      .withColumn("source_file", input_file_name())
  )
