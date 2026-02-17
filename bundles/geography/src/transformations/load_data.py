import dlt

catalog = spark.conf.get("catalog_name")
schema = spark.conf.get("schema_name")
volume = spark.conf.get("volume_name")

print(f"{catalog}, {schema}, {volume}")

@dlt.table
def cities():
  return spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .load(f"/Volumes/{catalog}/{schema}/{volume}/")