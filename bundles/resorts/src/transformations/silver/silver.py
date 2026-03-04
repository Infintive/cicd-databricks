import dlt
from pyspark.sql import functions as F


def _elevation_feet(column):
	cleaned = F.regexp_replace(F.col(column), ",", "")
	digits = F.regexp_extract(cleaned, r"(\d+)", 1)
	return F.when(F.length(digits) > 0, digits.cast("int")).otherwise(F.lit(None).cast("int"))


@dlt.table(name="resorts_silver")
def resorts_silver():
	"""Transform bronze resorts data into a clean silver table."""
	bronze_table = dlt.read_stream("resorts_bronze")
	return (
		bronze_table
		.withColumn("name", F.trim(F.col("name")))
		.withColumn("state", F.trim(F.col("state")))
		.withColumn("location", F.trim(F.col("location")))
		.withColumn("summit_elevation", _elevation_feet("summit_elevation"))
		.withColumn("base_elevation", _elevation_feet("base_elevation"))
		.filter(F.col("name").isNotNull())
	)
