from pyspark import SparkContext,StorageLevel
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.types import *


# Enabling Spark Configuration and SparkSession
sconf=SparkConf().setAppName("test")
spark=SparkSession.builder.config(conf=sconf).getOrCreate()

# RDD as a list of tuples 
rdd = spark.sparkContext.parallelize([('Alex',21),('Bob',44)])

# creating a schema using StructType
schema = StructType([
   StructField("name", StringType(), True),
   StructField("age", IntegerType(), True)])

# Creating a dataframe from rdd using schema
df=spark.createDataFrame(rdd, schema)

# displaying dataframe 
df.show(truncate=False)

