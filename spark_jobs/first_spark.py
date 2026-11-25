from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, round

spark = SparkSession.builder \
    .appName("CityPulse-First-Job") \
    .master("local[*]") \
    .getOrCreate()

data = [
    ("Ludhiana", 82.0, 31.0),
    ("Ludhiana", 95.0, 32.0),
    ("Jalandhar", 65.0, 30.0),
    ("Jalandhar", 72.0, 31.0),
    ("Amritsar", 110.0, 33.0),
    ("Amritsar", 125.0, 34.0)
]

df = spark.createDataFrame(
    data,
    ["city", "pm25", "temperature"]
)

df.show()

print("City-wise pollution summary")

df.groupBy("city").agg(
    round(avg("pm25"), 2).alias("average_pm25"),
    max("pm25").alias("maximum_pm25")
).orderBy("city").show()

print("Total observations:", df.count())

spark.stop()
