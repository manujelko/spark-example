"""
Multiply dataframe columns by a random number.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
import random
import pandas as pd
from pyspark.sql.types import DoubleType


spark = (
    SparkSession.builder.appName("My PySpark App")  # type: ignore
    .master("spark://localhost:7077")
    .config("spark.executor.memory", "2g")
    .config("spark.executor.cores", "2")
    .getOrCreate()
)


def example(value):
    return value * random.random()


num_rows = 50_000
pdf = pd.DataFrame({"number": random.sample(range(num_rows), num_rows)})
sdf = spark.createDataFrame(pdf)
example_udf = udf(example, DoubleType())
result_sdf = sdf.withColumn("modified_number", example_udf(sdf["number"]))
result_sdf.show()

spark.stop()
