"""
Square dataframe columns.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder.appName("My PySpark App")  # type: ignore
    .master("spark://localhost:7077")
    .config("spark.executor.memory", "2g")
    .config("spark.executor.cores", "2")
    .getOrCreate()
)

df = spark.range(1, 100_001).toDF("number")

squared_df = df.withColumn("squared_number", col("number") ** 2)

squared_df.show()

spark.stop()
