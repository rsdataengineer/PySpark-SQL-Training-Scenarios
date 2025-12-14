#Scenario - Mon 8 Nov
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("Scenarios").getOrCreate()

from pyspark.sql.functions import *
from pyspark.sql.types import *
print()
print("Scenario2 - Join with handling nulls")

#Input DataFrame1
data1 = [
    (1, "Henry"),
    (2, "Smith"),
    (3, "Hall")
]
columns1 = ["id", "name"]
df1 = spark.createDataFrame(data1, columns1)
print("Input DataFrame1:")
df1.show()

#Input DataFrame2
data2 = [
    (1, 100),
    (2, 500),
    (4, 1000)
]
columns2 = ["id", "salary"]
df2 = spark.createDataFrame(data2, columns2)
print("Input DataFrame2:")
df2.show()


#Performing Left Join on DataFrame1 and DataFrame2
joined_df = df1.join(df2, on="id", how="left")

print("Output DataFrame")
outputdf = joined_df.withColumn("salary", expr("coalesce(salary, 0)")).orderBy("id").show()
print()

#Different approaches to handle nulls in 'salary' column after left join
#Approach 1: Using fillna() - dictionary method
print("Approach 1: Using fillna() - dictionary method")
filled_df1 = joined_df.fillna({"salary": 0}).orderBy("id")
filled_df1.show()

#Approach 2: Using fill.na() - subset method
print("Approach 2: Using fill.na() - subset method")
filled_df2 = joined_df.fillna(0, subset=["salary"]).orderBy("id")
filled_df2.show()

#Approach 3: Coalesce function with expression
print("Approach 3: Coalesce function with expression")
filled_df3 = joined_df.withColumn("salary", coalesce(col("salary"), lit(0))).orderBy("id")
filled_df3.show()

#Approach 4: Coalesce function with expression
print("Approach 4: Coalesce function with expression")
filled_df4 = joined_df.withColumn("salary", expr("coalesce(salary, 0)")).orderBy("id")
filled_df4.show()
print()
