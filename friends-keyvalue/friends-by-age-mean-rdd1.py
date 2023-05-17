# -*- coding: utf-8 -*-
"""
Created on Tue May 16 17:05:35 2023

@author: kulpr
"""

from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.appName("SparkSql").getOrCreate()

def mapper(line):
    
    fields = line.split(",")
    return Row(id = int(fields[0]), name = str(fields[1].encode("UTF-8")),\
               age = int(fields[2]), numFriends = int (fields[3]))
        
        
lines = spark.sparkContext.textFile("file:///SparkCourse/friends-keyvalue/fakefriends.csv")
people = lines.map(mapper)

schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

schemaPeople.groupBy("age").mean("numFriends").sort("age").show()

spark.stop()
