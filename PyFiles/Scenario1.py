from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("Scenario1").getOrCreate()

from pyspark.sql.functions import *
from pyspark.sql.types import *

print("Scenario 1: Find employees with same salary")

data = [
    ("001", "Monika", "Arora", 100000, "2014-02-20 09:00:00", "HR"),
    ("002", "Niharika", "Verma", 300000, "2014-06-11 09:00:00", "Admin"),
    ("003", "Vishal", "Singhal", 300000, "2014-02-20 09:00:00", "HR"),
    ("004", "Amitabh", "Singh", 500000, "2014-02-20 09:00:00", "Admin"),
    ("005", "Vivek", "Bhati", 500000, "2014-06-11 09:00:00", "Admin")
    ]
columns = ["empid","fname","lname","sal","joiningdate","dept"]
dfemp = spark.createDataFrame(data, columns)
print("Input DataFrame:")
dfemp.show()

finaldf = dfemp.alias("emp1").join(dfemp.alias("emp2"), (col("emp1.sal") == col("emp2.sal")) & (col("emp1.empid") != col("emp2.empid")),"inner")
print("Output DataFrame after self join on salary column:")
finaldf.show()
