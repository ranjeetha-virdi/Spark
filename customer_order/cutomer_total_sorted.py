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
totalByPrice =rdd.reduceByKey(lambda x, y: x+y)
result_flipped = totalByPrice.map(lambda x : (x[1],x[0])).sortByKey()


result_sorted = result_flipped.collect()



for result in result_sorted:
   
   
        print(str(result[1]) + str(":\t\t{:.2f}".format(result[0])))
