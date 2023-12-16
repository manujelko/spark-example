from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import random

spark = (
    SparkSession.builder.appName("My PySpark App")  # type: ignore
    .master("spark://localhost:7077")
    .config("spark.executor.memory", "2g")
    .config("spark.executor.cores", "2")
    .getOrCreate()
)

data = [(random.random(),) for _ in range(50_000)]
df = spark.createDataFrame(data, ["value"])

average_value = df.agg(F.avg("value")).first()[0]

result_df = df.withColumn("value_plus_avg", F.col("value") + average_value)

result_df.show()
