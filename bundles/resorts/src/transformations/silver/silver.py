import dlt
from pyspark.sql import functions as F

from utilities.utils import _coordinates_lat_lon, _elevation_feet, _numeric_from_text


@dlt.table(name="resorts_silver")
def resorts_silver():
	"""Transform bronze resorts data into a clean silver table."""
	bronze_table = dlt.read_stream("resorts_bronze")
	lat_col, lon_col = _coordinates_lat_lon("coordinates")
	return (
		bronze_table
		.withColumn("name", F.trim(F.col("name")))
		.withColumn("state", F.trim(F.col("state")))
		.withColumn("location", F.trim(F.col("location")))
		.withColumn("summit_elevation", _elevation_feet("summit_elevation"))
		.withColumn("base_elevation", _elevation_feet("base_elevation"))
		.withColumn("vertical_drop", _numeric_from_text("vertical_drop", "int"))
		.withColumn("skiable_area", _numeric_from_text("skiable_area", "double"))
		.withColumn("annual_snowfall", _numeric_from_text("annual_snowfall", "int"))
		.withColumn("number_of_trails", _numeric_from_text("number_of_trails", "int"))
		.withColumn("_lat", lat_col)
		.withColumn("_lon", lon_col)
		.filter(F.col("name").isNotNull())
	)
