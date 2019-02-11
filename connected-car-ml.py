TABLE = "/obd/obd_raw_table"
APP_NAME = "Connected Car ML"

from pyspark import SparkContext
from pyspark.sql import SparkSession
import numpy as np
import pandas as pd

spark = SparkSession.builder \
    .appName(APP_NAME) \
    .getOrCreate()

sc = spark.sparkContext
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

from functools import reduce
from pyspark.sql.types import *
from pyspark.sql.functions import date_format
from pyspark.sql.functions import *


## 1. Read from Table:
df1 = sqlContext.read\
     .format("com.mapr.db.spark.sql.DefaultSource")\
     .option("tableName", TABLE).load()

#######################################################

df2 = spark.createDataFrame(df1.groupby('vin', date_format('timestamp', 'yyyy-MM-dd').alias('date')).agg(sqlfunc.corr("engineRPM","engineCoolantTemperature").alias('r')).collect())
df2.orderBy("date").show(75)

#import pyspark.sql.functions as func
#df3 = df1.groupby('vin', date_format('timestamp', 'yyyy-MM-dd').alias('date')).agg({"target": "max"})
#df3 = spark.createDataFrame(df1.groupby('vin', date_format('timestamp', 'yyyy-MM-dd').alias('date')).agg({"target": "max"}).collect())

