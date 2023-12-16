"""
Square list numbers.
"""
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("My PySpark App")  # type: ignore
    .master("spark://localhost:7077")
    .config("spark.executor.memory", "2g")
    .config("spark.executor.cores", "2")
    .getOrCreate()
)

numbers_rdd = spark.sparkContext.parallelize(range(1, 100_001))


def square_number(number):
    return number**2


squared_numbers_rdd = numbers_rdd.map(square_number)

squared_numbers = squared_numbers_rdd.collect()

print(squared_numbers[:20])

spark.stop()
