from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("Masking Scenario").getOrCreate()

from pyspark.sql.functions import *
from pyspark.sql.types import *
print()
print("Scenario4 - Mask the account number of the users")
print()
print("Input DataFrame")
data = [
    ("Alice", 34, 45634567), 
    ("Bob", 45, 23689754), 
    ("Cathy", 29, 98456424), 
    ("David", 25, 91283753478),
    ("Josh", 28, 102357)
    ]
df = spark.createDataFrame(data, ["Name", "Age", "Acc Number"])
df.persist()
df.show()

print("Output DataFrame")
df.withColumn("mask", expr("repeat('*', length('Acc Number'))")).drop("Acc Number").show()