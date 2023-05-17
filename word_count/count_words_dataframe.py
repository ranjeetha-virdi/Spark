# -*- coding: utf-8 -*-
"""
Created on Wed May 17 10:03:43 2023

@author: kulpr
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("WordCount").getOrCreate()
inputDF=spark.read.text("file:///SparkCourse/word_count/Book.txt")
words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
words.filter(words.word!=" ")
lowercaseWords=words.select(func.lower(words.word).alias("word"))
wordCounts = words.groupBy("word").count()
wordCountsSorted = wordCounts.sort("count")

#to see the entire df
wordCountsSorted.show(wordCountsSorted.count())
    