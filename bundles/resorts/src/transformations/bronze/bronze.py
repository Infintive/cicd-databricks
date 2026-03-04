import dlt

catalog = spark.conf.get("catalog_name")
schema = spark.conf.get("schema_name")
volume = spark.conf.get("volume_name")

@dlt.table(name="resorts_bronze")
def resorts_bronze():
  """Load data into the bronze table using Auto Loader."""
  return spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .load(f"/Volumes/{catalog}/{schema}/{volume}/")