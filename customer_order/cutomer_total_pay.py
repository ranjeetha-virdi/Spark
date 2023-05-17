# -*- coding: utf-8 -*-
"""
Created on Tue May 16 10:49:17 2023

@author: kulpr
"""
from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("Moneyspent")
sc = SparkContext(conf = conf)


def parseLine(line):
    fields = line.split(',')
    custid = int(fields[0])
    price = float(fields[2])
    
    return (custid, price)




lines = sc.textFile("file:///SparkCourse/customer-orders.csv")
rdd = lines.map(parseLine)
totalByPrice =rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
totalByPrices = totalByPrice.mapValues(lambda x: x[0])
#totalByPrice = rdd.reduceByKey(lambda x, y: x + y)

sortedResults = collections.OrderedDict(sorted(totalByPrices.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))