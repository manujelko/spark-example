"""
Sum numbers (reduce).
"""
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("My PySpark App")  # type: ignore
    .master("spark://localhost:7077")
    .config("spark.executor.memory", "2g")
    .config("spark.executor.cores", "2")
    .getOrCreate()
)

numbers = list(range(1, 10001))

rdd = spark.sparkContext.parallelize(numbers)

total_sum = rdd.reduce(lambda x, y: x + y)

print("Sum of numbers", total_sum)

spark.stop()
