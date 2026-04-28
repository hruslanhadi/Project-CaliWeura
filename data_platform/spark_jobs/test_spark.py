from pyspark.sql import SparkSession

# Initialize a clean session without Delta Lake extensions
spark = SparkSession.builder \
    .appName("SimpleTest") \
    .getOrCreate()

# Create a simple list of tuples
data = [("ok", 1), ("spark", 2), ("docker", 3)]
columns = ["status", "value"]

# Create and show the DataFrame
df = spark.createDataFrame(data, columns)
df.show()

spark.stop()