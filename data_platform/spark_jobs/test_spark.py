from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ConnectionTest").getOrCreate()

df = spark.createDataFrame(
    [("ok", 1), ("spark", 2)],
    ["status", "value"]
)

df.show()

spark.stop()