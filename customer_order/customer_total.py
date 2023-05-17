# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
from pyspark import SparkConf, SparkContext

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


results = totalByPrices.collect();
for result in results : 
    print(str(result[0]) + str (":\t{:.2f}".format(result[1])))

   

    

    
 
    
   

