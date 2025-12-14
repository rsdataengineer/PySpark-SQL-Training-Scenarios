

from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("Aggregations Scenarios").getOrCreate()

from pyspark.sql.functions import *
from pyspark.sql.types import *
print()

print("Scenario3 - Aggregations using groupBy and agg functions")

rdd = spark.sparkContext.parallelize([
    ("sai", "chennai", 40),
    ("zeyo", "hyderabad", 10),
    ("sai", "hyderabad", 20),
    ("zeyo", "chennai", 20),
    ("sai", "chennai", 10),
    ("zeyo", "hyderabad", 10)
])

df1 = rdd.toDF(["name", "city", "amount"])
print("Input DataFrame:")
df1.show()

print("Total Collection")
dfcollection = df1.groupby("name").agg(sum("amount").alias("Total Collection")).orderBy("name")
#Here name column is grouped and amount column is aggregated using sum function
dfcollection.show()
#select name, sum(amount) as 'Total collection' from df1 group by name order by name

print("Total and count of collection")
dfcollection = df1.groupby("name").agg(sum("amount").alias("Total Collection"), count("amount").alias("Collection Count")).orderBy("name")
dfcollection.show()

print("Total, count, and collection of amount")
dfcollection = df1.groupby("name").agg(
    sum("amount").alias("Total Collection"), 
    count("amount").alias("Collection Count"), 
    collect_list("amount").alias("Amount_Collect")).orderBy("name")
dfcollection.show()

print("Total, count, collection and unique collection of amount")
dfcollection = df1.groupby("name").agg(
    sum("amount").alias("Total Collection"), 
    count("amount").alias("Collection Count"), 
    collect_list("amount").alias("Amount_Collect"),                         #collect_list is raw collection and collects all values including duplicates
    collect_set("amount").alias("Unique collection")).orderBy("name")       #collect_set collects only distinct values at collection level
dfcollection.show()
