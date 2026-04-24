from job_config import build_spark_session

spark = build_spark_session("ConnectionTest")
spark.createDataFrame([("ok", 1), ("spark", 2)], ["status", "value"]).show()
spark.stop()
