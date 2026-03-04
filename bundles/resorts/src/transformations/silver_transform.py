# import dlt
# from pyspark.sql import SparkSession, functions as F

# spark = SparkSession.builder.getOrCreate()
# catalog = spark.conf.get("catalog_name")
# bronze_schema = spark.conf.get("bronze_schema_name")


# @dlt.table(name="resorts_silver")
# def resorts_silver():
#     """Transform bronze resorts data into a clean silver table."""

#     bronze_table = f"{catalog}.{bronze_schema}.resorts"
#     return (
#         spark.readStream.table(bronze_table)
#         .withColumn("resort", F.trim(F.col("resort")))
#         .withColumn("city", F.trim(F.col("city")))
#         .withColumn("state", F.trim(F.col("state")))
#         .withColumn("peak_elevation", F.col("peak_elevation").cast("int"))
#         .withColumn("base_elevation", F.col("base_elevation").cast("int"))
#         .withColumn("vertical_drop", F.col("vertical_drop").cast("int"))
#         .withColumn("skiable_acreage", F.col("skiable_acreage").cast("int"))
#         .withColumn("total_trails", F.col("total_trails").cast("int"))
#         .withColumn("total_lifts", F.col("total_lifts").cast("int"))
#         .withColumn("avg_annual_snowfall", F.col("avg_annual_snowfall").cast("int"))
#         .withColumn("lift_ticket", F.col("lift_ticket").cast("int"))
#         .filter(F.col("resort").isNotNull())
#     )
