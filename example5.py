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


def multiply_by_random(df, column):
    return df.withColumn(column, F.col(column) + random.random())


def add_random(df, column):
    return df.withColumn(column, F.col(column) + random.random())


def square_values(df, column):
    return df.withColumn(column, F.col(column) ** 2)


df1 = spark.createDataFrame([(random.random(),) for _ in range(10_000)], ["numbers"])
df2 = spark.createDataFrame([(random.random(),) for _ in range(10_000)], ["numbers"])
df3 = spark.createDataFrame([(random.random(),) for _ in range(10_000)], ["numbers"])

tasks = [
    (multiply_by_random, df1, "numbers"),
    (add_random, df2, "numbers"),
    (square_values, df3, "numbers"),
]

results = [function(df, column) for function, df, column in tasks]

for result in results:
    result.show()
