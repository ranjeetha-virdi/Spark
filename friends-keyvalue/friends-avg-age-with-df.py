# -*- coding: utf-8 -*-
"""
Created on Tue May 16 17:17:08 2023

@author: kulpr
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("file:///SparkCourse/friends-keyvalue/fakefriends-header.csv")
    


people.groupby("age").agg(func.round(func.avg("friends"),2).alias("avg_friends")).sort("age").show()
spark.stop()