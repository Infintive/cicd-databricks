import dlt
from pyspark.sql import functions as F


def _elevation_feet(column):
	cleaned = F.regexp_replace(F.col(column), ",", "")
	digits = F.regexp_extract(cleaned, r"(\d+)", 1)
	return F.when(F.length(digits) > 0, digits.cast("int")).otherwise(F.lit(None).cast("int"))


def _numeric_from_text(column, data_type="int"):
	cleaned = F.regexp_replace(F.col(column), ",", "")
	number = F.regexp_extract(cleaned, r"(\d+(?:\.\d+)?)", 1)
	casted = number.cast("double") if data_type == "double" else number.cast("int")
	return F.when(F.length(number) > 0, casted).otherwise(F.lit(None).cast(data_type))


def _coordinates_lat_lon(column):
	value = F.col(column)

	pair_lat = F.regexp_extract(value, r"([-+]?\d+\.\d+)\s*;\s*([-+]?\d+\.\d+)", 1)
	pair_lon = F.regexp_extract(value, r"([-+]?\d+\.\d+)\s*;\s*([-+]?\d+\.\d+)", 2)
	lat_pair = F.when(F.length(pair_lat) > 0, pair_lat.cast("double"))
	lon_pair = F.when(F.length(pair_lon) > 0, pair_lon.cast("double"))

	dec_lat = F.regexp_extract(value, r"([-+]?\d+\.\d+)\s*°?\s*([NS])", 1)
	dec_lat_dir = F.regexp_extract(value, r"([-+]?\d+\.\d+)\s*°?\s*([NS])", 2)
	lat_dec = F.when(F.length(dec_lat) > 0, F.when(dec_lat_dir == F.lit("S"), -dec_lat.cast("double")).otherwise(dec_lat.cast("double")))

	dec_lon = F.regexp_extract(value, r"([-+]?\d+\.\d+)\s*°?\s*([EW])", 1)
	dec_lon_dir = F.regexp_extract(value, r"([-+]?\d+\.\d+)\s*°?\s*([EW])", 2)
	lon_dec = F.when(F.length(dec_lon) > 0, F.when(dec_lon_dir == F.lit("W"), -dec_lon.cast("double")).otherwise(dec_lon.cast("double")))

	lat_deg = F.regexp_extract(value, r"(\d{1,3})°\s*(\d{1,2})′\s*(\d{1,2})″\s*([NS])", 1)
	lat_min = F.regexp_extract(value, r"(\d{1,3})°\s*(\d{1,2})′\s*(\d{1,2})″\s*([NS])", 2)
	lat_sec = F.regexp_extract(value, r"(\d{1,3})°\s*(\d{1,2})′\s*(\d{1,2})″\s*([NS])", 3)
	lat_dir = F.regexp_extract(value, r"(\d{1,3})°\s*(\d{1,2})′\s*(\d{1,2})″\s*([NS])", 4)
	lat_dms_value = (
		lat_deg.cast("double")
		+ (lat_min.cast("double") / F.lit(60.0))
		+ (lat_sec.cast("double") / F.lit(3600.0))
	)
	lat_dms = F.when(
		F.length(lat_deg) > 0,
		F.when(lat_dir == F.lit("S"), -lat_dms_value).otherwise(lat_dms_value),
	)

	lon_deg = F.regexp_extract(value, r"(\d{1,3})°\s*(\d{1,2})′\s*(\d{1,2})″\s*([EW])", 1)
	lon_min = F.regexp_extract(value, r"(\d{1,3})°\s*(\d{1,2})′\s*(\d{1,2})″\s*([EW])", 2)
	lon_sec = F.regexp_extract(value, r"(\d{1,3})°\s*(\d{1,2})′\s*(\d{1,2})″\s*([EW])", 3)
	lon_dir = F.regexp_extract(value, r"(\d{1,3})°\s*(\d{1,2})′\s*(\d{1,2})″\s*([EW])", 4)
	lon_dms_value = (
		lon_deg.cast("double")
		+ (lon_min.cast("double") / F.lit(60.0))
		+ (lon_sec.cast("double") / F.lit(3600.0))
	)
	lon_dms = F.when(
		F.length(lon_deg) > 0,
		F.when(lon_dir == F.lit("W"), -lon_dms_value).otherwise(lon_dms_value),
	)

	lat = F.coalesce(lat_pair, lat_dec, lat_dms)
	lon = F.coalesce(lon_pair, lon_dec, lon_dms)

	return lat, lon


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
		.withColumn("_lat", lat_col)
		.withColumn("_lon", lon_col)
		.filter(F.col("name").isNotNull())
	)
